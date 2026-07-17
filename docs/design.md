# revenant — Design Document

**Status:** Initial design, not yet implemented.
**Purpose:** A crash-safe, resumable, single-machine pipeline framework for Python, built as a CLI. Reference this document as the starting point for implementation; it captures all design decisions and their rationale so they don't need to be rediscovered.

---

## 1. Problem statement

Process a stream of input objects through an ordered sequence of stages:

```
input objects -> step A -> step B -> ... -> output objects
```

Requirements:

- A step may emit **zero, one, or many** output objects per input object.
- Stages run **concurrently**: step A can be processing item *N+1* while step B processes step A's output from item *N*.
- Within a single stage, items are processed **one at a time, in order** — not in parallel — because:
  - at least one stage has a very large in-memory footprint and must run as a single process,
  - CPU is expected to saturate quickly, so intra-stage parallelism buys little and risks contention.
- Some stages carry **residual/accumulator state** across items (not just per-item processing).
- Some stages are **non-deterministic** (e.g. AI model calls) and cannot be reliably replayed to produce identical output.
- The whole pipeline must be **resumable after a crash or restart**, reprocessing at most the interrupted item(s) per stage — not the whole run.
- **No additional services** (no Postgres, no Redis, no broker, no orchestrator). Single Python process tree.
- State should be stored as **human-readable JSON** on disk, not SQLite, given no other requirement forces otherwise.
- Runs inside **Docker with a bind-mounted host volume**; no reliable filesystem-event notifications (inotify) — must use polling.
- Input is treated as **fully assembled before the pipeline starts** (a known, fixed-size batch) — not an open-ended stream that grows during a run. This simplifies completion detection considerably (see §7).

---

## 2. Process topology

Two kinds of code:

- **Supervisor** — the top-level `revenant process` entrypoint. Never touches payloads or step logic. Spawns one child OS process per stage, monitors liveness, restarts crashed stages, detects whole-pipeline completion, and handles shutdown signals.
- **Stage runner** — generic driver code, identical for every stage. Given a stage's config (name, `Step` implementation, upstream file, own output file), it loops: read next unread upstream record, call the step's `process()`, durably commit the result, repeat. Exits when it has drained its upstream and upstream is durably done (§7).

Because the stage runner is generic and driven by config, the supervisor doesn't need bespoke per-stage subprocess code — it invokes itself:

```
revenant process                    # supervisor: spawn + monitor all stages
revenant process --stage B          # run only B's stage-runner loop, in the foreground
revenant process --stage B --once   # same, but process exactly one item then exit
revenant status                     # read all on-disk state, print a summary
```

`revenant process --stage B [--once]` *is* the stage-runner entrypoint. The supervisor's "spawn a child" step is just `subprocess.Popen([sys.executable, "-m", "revenant", "process", "--stage", name])` for each configured stage — this is why `--stage` / `--once` require no special-casing: they're the same code path the supervisor already uses, just invoked manually.

Each stage-process holds a **process-wide memory footprint budget of one** — only one instance of a given stage may run at a time, enforced via a PID lock file (§5.4).

---

## 3. Directory / file layout

```
state/
  input.jsonl                 # the fixed, pre-assembled input batch
  A.jsonl                     # output of stage A / input of stage B
  A.checkpoint.json           # cached/derived resume-hint for stage A (not source of truth)
  A.lock                      # PID lock for stage A
  B.jsonl
  B.checkpoint.json
  B.lock
  ...
  pipeline.status.json        # supervisor's cached view of overall progress (fully derivable)
```

Each stage only ever reads the file to its left and writes the file (+ checkpoint) to its right. A stage never needs to know what is upstream of its input file or downstream of its output file — this symmetry is what makes stages addable/removable/reorderable without touching stage-runner code.

---

## 4. Stage config (pipeline wiring)

The pipeline is defined declaratively; nothing about wiring is hardcoded into the supervisor:

```python
PIPELINE = [
    StageConfig(name="A", step_class=StepA, upstream="input"),
    StageConfig(name="B", step_class=StepB, upstream="A"),
    StageConfig(name="C", step_class=StepC, upstream="B"),
]
```

Each stage's output file, checkpoint file, and lock file paths are derived automatically from `name` (`state/{name}.jsonl`, etc.) — there is nothing to keep in sync by hand when stages are added, removed, or reordered.

---

## 5. On-disk file formats

### 5.1 Stage output file (`A.jsonl`, `B.jsonl`, ...) — **the source of truth**

Append-only JSON Lines. Two line types share the file: **record** lines (an emitted output) and **checkpoint** lines (a commit marker). A single completed input item produces one contiguous *commit block*: zero or more record lines, followed by exactly one checkpoint line, all written as **one atomic append** (§6).

**Record line:**

```json
{"type": "record", "seq": 4821, "src_seq": 1523, "parent_seq": 4820, "emitted_at": "2026-07-16T14:22:03.031Z", "payload": {"...": "..."}}
```

- `seq` — monotonically increasing integer, unique within this file, assigned by the writer. Downstream stages track "last consumed `seq`" as their resume position.
- `src_seq` — the `seq` of the upstream item that produced this line. Retained for lineage/debugging; no longer required for crash-dedup (§6 eliminates that need), but cheap to keep.
- `parent_seq` — the ultimate originating item's `seq`, for full cross-stage lineage tracing. Optional but recommended.
- `emitted_at` — for humans/debugging only, not used by any logic.
- `payload` — the step's actual output object. Kept inside an envelope so framework metadata (`seq`, `type`, etc.) never collides with domain field names.

**Checkpoint line:**

```json
{"type": "checkpoint", "src_seq": 1523, "last_emitted_seq": 4822, "state": {"...": "..."}, "committed_at": "2026-07-16T14:22:03.040Z"}
```

- `src_seq` — the upstream item's `seq` that this checkpoint confirms as fully, successfully processed.
- `last_emitted_seq` — the highest `seq` this stage has ever written (i.e., the max `seq` among all record lines up to and including this commit block). Lets the stage resume seq-numbering without rescanning its own file.
- `state` — the JSON-serializable projection of the step's residual/accumulator state as of this commit (see §8, `Step.checkpoint()`).
- `committed_at` — for humans/debugging.

**Why this design (vs. a separate checkpoint-only file):** see §6. In short — a stage only ever emits records that are *already* confirmed by the time a downstream reader can see them, because the commit block (records + checkpoint) is written as a single atomic unit. This removes any window in which a downstream stage could observe outputs from an attempt that later turns out to have crashed, which matters because some steps are non-deterministic and a replayed attempt may not reproduce byte-identical output.

### 5.2 Checkpoint cache file (`A.checkpoint.json`)

**Not the source of truth** — a convenience cache so a stage doesn't need to scan its entire output file backward on every restart just to find its last checkpoint line. Written via write-temp-then-atomic-rename, best-effort, after every commit block:

```json
{
  "stage": "A",
  "last_consumed_seq": 1523,
  "last_emitted_seq": 4822,
  "state": {"...": "..."},
  "items_processed": 1523,
  "updated_at": "2026-07-16T14:22:03.040Z",
  "schema_version": 1
}
```

On startup, a stage reads this file as a *hint* for where to resume. If it's missing, stale, or looks inconsistent with the actual output file (e.g. the file has grown past what this checkpoint claims), the stage falls back to scanning `A.jsonl` backward for the last `type: checkpoint` line and rebuilds this cache from it. This file existing purely as a cache — never load-bearing for correctness — is what allows §6's atomicity story to rely on a single file write rather than two files being kept in sync.

### 5.3 Pipeline status file (`pipeline.status.json`)

Owned and periodically rewritten by the **supervisor** only. Also purely a cache/dashboard — fully reconstructable from every stage's own checkpoint cache if lost:

```json
{
  "state": "running",
  "stages": {
    "A": {"pid": 8834, "last_consumed_seq": 1523, "durably_done": false},
    "B": {"pid": 8835, "last_consumed_seq": 1518, "durably_done": false}
  },
  "updated_at": "2026-07-16T14:22:05.000Z"
}
```

`state` is one of `"running" | "done"`.

### 5.4 Lock file (`A.lock`)

Plain JSON, human-readable:

```json
{"pid": 8834, "hostname": "container-abc123", "started_at": "2026-07-16T14:00:00.000Z"}
```

On startup, a stage checks for an existing lock file; if present, it checks whether `pid` is still alive (e.g. `os.kill(pid, 0)` / checking `/proc/<pid>`). If alive, refuse to start. If dead, the lock is stale — overwrite it and proceed. (Liveness checks are PID-based and assume a single host; this framework does not need to detect a lock held by a process on a *different* machine — confirmed out of scope.)

---

## 6. Crash-safety and atomicity model

**Core invariant:** for any given input item, everything that item produces — all of its emitted output records *and* the checkpoint line confirming it — is written to the stage's output file as **one atomic append** (a single `write()` call to an `O_APPEND` file descriptor; atomic in practice for reasonably small payloads on local filesystems). There is no on-disk state where "some but not all" of an item's outputs are visible, and no state where an output is visible but not yet confirmed.

**Why this matters (the case it was designed to close):** an earlier design considered appending each output the instant a step's `process()` generator yielded it, then writing the checkpoint separately once the whole item finished. That's a nicer story for early downstream visibility into a long-running item, but it opens a race: a downstream stage polling the output file could see and start acting on output records from an item that **later crashes before its checkpoint is written** — meaning the pipeline's own bookkeeping says that attempt never happened, while a downstream stage already built on it. For a deterministic step this is harmless (the replayed attempt produces identical output). For a **non-deterministic** step (e.g. one calling an AI model), the replayed attempt can produce materially different output, and now downstream has consumed content that the source-of-truth record disavows. The atomic-commit-block design closes this entirely: a downstream reader, by construction, can never observe a record before the checkpoint line that confirms it, so there is no such thing as "acted on an attempt that turned out to be discarded" — replayed attempts after a crash are simply never observed pre-commit, regardless of determinism.

**Consequence knowingly accepted:** downstream can no longer see *any* of an item's outputs until that item is fully finished and committed, even if some outputs were ready early (e.g. output #1 of 3 was ready long before #3). This was evaluated and accepted — the actual concurrency requirement was always *inter-item* (stage A on item N+1 while stage B works on stage A's output from item N), never *intra-item* streaming visibility. Any step that would have benefited from intra-item streaming should instead be split into two stages: a stateless "planner" stage that emits many small sub-items in one commit block, followed by a stage that processes each sub-item independently — this recovers the pipelining benefit through ordinary inter-item concurrency rather than needing partial visibility into one item.

**Step function shape implied by this:** `process()` is a generator (see §8) that the framework fully drains (buffering all yields in memory) before writing anything to disk. The write only happens after the generator completes and returns its updated state.

**Docker/bind-mount caveat:** the atomicity of `write()`-to-append and of temp-file-then-`rename()` (used for the checkpoint cache and lock files) is a property of the underlying filesystem, not of Python. This holds reliably on native Linux filesystems and on Docker bind mounts backed by a genuine Linux host filesystem. It is **not guaranteed** on:
- bind mounts through Docker Desktop on macOS/Windows (the shared-folder layer may not be a true POSIX filesystem underneath),
- network filesystems (NFS/SMB), if the volume is ever backed by one.

Confirm the actual host filesystem before relying on this. If there's doubt, an `fsync` on the containing directory after `rename()` (not just the file) is the extra-paranoid belt-and-suspenders step.

---

## 7. Completion / drain detection

Because input is treated as fully assembled before the pipeline starts (§1), the parent process knows the input's final `seq` (the count of lines it wrote) without needing any marker file. This is the base case for a purely computed, recursive "durably done" check — no marker files need to cascade through the pipeline:

```
stage_0 (reads input.jsonl) durably_done
    ⟺ stage_0.last_consumed_seq == input's final seq   (known a priori by the parent)

stage_i durably_done
    ⟺ stage_{i-1} durably_done
      AND stage_i.last_consumed_seq == stage_{i-1}.last_emitted_seq
```

Once `stage_{i-1}` is durably done, its `last_emitted_seq` is frozen forever (it will never run again, never emit more) — so the comparison for `stage_i` is stable no matter when it's evaluated afterward. The recursion is well-founded because each stage's "done" status, once true, freezes exactly the fact the next stage's check depends on.

**Whole pipeline done** = the last stage in the chain is durably done.

This same check is used in two places:
- by each stage's own loop, to decide whether "no new upstream item right now" means "poll again later" or "exit, I'm finished,"
- by the supervisor, to decide whether the whole pipeline run is complete.

No `*.closed.json` marker files are needed anywhere in this design, because input is known-final up front. *(If input were allowed to be appended to during a run, a single closed-marker at the input boundary would be needed to give stage 0 a definite "no more will ever arrive" signal — this was considered and explicitly deferred; see §12.)*

---

## 8. Step function interface

```python
class RetryableError(Exception):
    """Raise to indicate a transient failure; the framework will retry
    this same item (with backoff), without advancing the checkpoint."""

class SkipItem(Exception):
    """Raise to deliberately drop this item: emit nothing, advance the
    checkpoint past it, and record it in the stage's dead-letter file."""

class Step:
    def load(self, saved_state: Any | None) -> Any:
        """
        Called once when the stage process starts.
        - Do one-time expensive setup here (load a model, open resources).
        - `saved_state` is the "state" field from the last commit's
          checkpoint line (or checkpoint.json cache), or None on a fresh
          start with no prior history.
        - Returns the initial in-memory `state` value to thread through
          process(). This value need NOT be JSON-serializable — it may
          hold live objects (e.g. a loaded model handle bundled with a
          small accumulator dict).
        """

    def process(self, payload: dict, state: Any):
        """
        Called once per input item. A generator:
        - yield each output payload (dict) as it's produced — 0, 1, or
          many times. The framework buffers these in memory; nothing is
          written to disk until the generator fully completes (see §6).
        - When done, `return new_state` (captured via the generator's
          StopIteration value) — the updated in-memory state to carry
          into the next item.
        - Raise RetryableError or SkipItem for the corresponding
          handling (see below). Any other exception is treated as a
          process-fatal crash: the framework does not catch it, the
          checkpoint is left untouched, and the stage process exits
          non-zero. On restart, this same item is retried from
          scratch — no special-casing needed, this is just the ordinary
          crash-recovery path.
        """

    def checkpoint(self, state: Any) -> Any:
        """
        Given the current in-memory state (as returned by process()),
        return the JSON-serializable subset that should be persisted
        in the commit's checkpoint line / cache file. Lets a step keep
        large/unserializable objects (e.g. a loaded model) bundled with
        state in memory without those ever being written to disk.
        For stateless steps or steps with no heavy setup, this can just
        be `lambda state: state`, and `load()` can just return an empty
        initial state — no extra burden.
        """
```

### Error-handling semantics

| Exception raised | Framework behavior |
|---|---|
| `RetryableError` | Log it; do **not** advance checkpoint; retry the same item (track attempt count / backoff, e.g. alongside `state` in the checkpoint cache); give up and escalate to a fatal crash after a configurable max-attempts. |
| `SkipItem` | Advance checkpoint past this item; emit nothing for it; append a record to `A.deadletter.jsonl` for visibility; continue. |
| Anything else (unhandled) | Do not catch. Process exits non-zero. Checkpoint untouched. This *is* the crash-recovery path — no special handling needed; the supervisor will restart this stage and it resumes from the last successful commit. |

### Determinism note

Because of the atomic-commit-block design (§6), **steps do not need to be deterministic**. A crash mid-`process()` simply means the entire attempt (all buffered yields, not-yet-returned state) is discarded and redone from scratch on restart; since nothing from an in-progress attempt is ever written to disk (only fully-completed, checkpoint-confirmed attempts are), there is no possibility of a downstream stage having consumed output from a discarded, non-reproduced attempt. This was a real gap in an earlier iteration of the design (relying on per-record `src_seq` deduplication, which assumed replayed attempts produce identical content) and is now closed structurally rather than by convention.

**What is *not* covered:** side effects a step performs outside the pipeline's own files (e.g. sending an email, calling a non-idempotent external API as a side effect rather than as an emitted output) will still happen again on a replay after a crash. If any step does this, it needs its own idempotency key (e.g., keyed on the input item's `seq`) at the point it calls out. None of the currently planned steps are known to do this, but worth checking before implementation.

---

## 9. CLI shape

```
revenant process                    # supervisor: spawn + monitor every configured stage,
                                     #   run until the whole pipeline is durably done, then exit
revenant process --stage <name>            # run only that stage's loop, in the foreground
revenant process --stage <name> --once     # process exactly one available item, then exit
revenant status                     # print each stage's offset / running-or-idle / done state,
                                     #   derived by reading every checkpoint cache + lock file
```

`--stage` and `--once` require no special-casing in the supervisor's own code — they exist because the supervisor never has processing logic itself; it only spawns the exact same stage-runner entrypoint that `--stage` invokes directly. This also makes `--stage <name>` the natural tool for manually testing a single stage in isolation.

---

## 10. Supervisor behavior

1. For each configured stage, spawn `revenant process --stage <name>` as a subprocess; the child acquires its own lock file on startup.
2. Poll (same interval used everywhere else, given no filesystem-event notifications are reliable in this Docker/bind-mount environment) each stage subprocess:
   - Still alive? Leave it running.
   - Exited cleanly (i.e. it decided it was durably done, per §7)? Do **not** respawn — that stage has legitimately finished forever.
   - Exited unexpectedly (crash, non-zero, killed)? Respawn it. It resumes exactly from its last commit per the checkpoint mechanism — the supervisor does not need to know *why* it died.
3. Whole pipeline is done once the **last** stage in the chain has exited cleanly (which, by the recursive definition in §7, can only happen after every stage upstream of it has already done so).
4. On SIGTERM/SIGINT to the supervisor: propagate the signal to all still-running children, so each finishes its current item (reaching the atomic commit-block boundary, never stopping mid-item — mid-item isn't an observable state on disk anyway per §6) before exiting, rather than the supervisor dying and orphaning children.
5. Once done: rewrite `pipeline.status.json` one last time, then exit.

---

## 11. Open items / deferred decisions

- **Adding new input mid-run.** Explicitly deferred — input is currently treated as a fixed, fully-assembled batch known before the pipeline starts (§1, §7). If this changes later, the fix is a single `input.closed.json` marker (`{"final_seq": N, "closed_at": "..."}`) written explicitly whenever input is done being appended to, which becomes the new base case for §7's recursion; every other stage boundary is unaffected. Two operating modes would fall out of this: batch mode (close input immediately, run to drain) vs. long-running/streaming mode (leave input open, keep enqueuing, only close it to deliberately shut the pipeline down for good).
- **Snapshot frequency.** Confirmed: checkpoint after *every* completed item (not batched every N items), since tasks may be long-running and the cost of reprocessing more than one item on crash was judged not worth the savings from less-frequent snapshotting.
- **Log rotation / compaction.** Not yet designed. `A.jsonl` grows unboundedly for the life of a run; if a stage's output file needs periodic compaction (e.g. dropping already-fully-consumed lines), note that `seq` numbers — not line-position-in-file — are the resumption unit specifically so this remains possible without breaking downstream consumers.
- **Retry backoff policy for `RetryableError`.** Max attempts / backoff schedule not yet specified.
- **Docker bind-mount atomicity verification.** Confirm the actual host filesystem backing the bind mount before relying on `write()`-append and `rename()` atomicity (§6's caveat) — this is load-bearing for the entire crash-safety story.

---

## 12. Naming

Package/CLI name: **`revenant`**. Checked clear on PyPI at time of writing (no colliding package found); chosen over `ratchet`, `waypoint`, `silo`, `conveyor`, `ledger`, `stepstone`, `golem`, `cairn`, `wardstone`, `sigil`, and `wyrm`, all of which were either taken or had meaningful brand/collision concerns (see design discussion history for details). "Revenant" — a being that returns after being struck down — was chosen as a direct metaphor for the crash-resume guarantee at the core of this framework.
