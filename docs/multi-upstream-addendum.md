# revenant — Design Addendum (Proposal): Multiple Upstreams (Fan-In)

**Status:** Proposal — not yet finalized or approved. The decisions below
represent one worked-through option, written up in implementation-ready
detail so it can be reviewed concretely rather than in the abstract. Schema
changes, the union-only scope decision (§0, §8), and the specific
`Step.process()` dispatch mechanism (§5) are the parts most likely to need
revisiting before sign-off — flag disagreement with any of them before
building against this document.
**Relationship to `docs/design.md`:** If adopted, this would be an addendum,
not a replacement. It proposes extending the existing design (sections 4,
5.1, 7, 8 in particular) to allow a stage to declare more than one upstream.
Nothing here would change existing behavior for pipelines that only ever
use a single upstream per stage — the intent is for this to be purely
additive, in the same spirit as `docs/blob-storage-addendum.md` — but that
should be verified against the final version rather than assumed from this
draft.

---

## 0. Problem

Today, `StageConfig.upstream` is a single name, and every piece of the
runtime (`upstream_output_path`, checkpoint schema, drain detection) assumes
exactly one upstream file per stage. This makes it impossible to express a
"fan-in" shape such as:

```
input -> A -> B1 -> C1 -> D
              B2 -> C2 -> D
```

where `D` needs to read from both `C1` and `C2`.

**Explicitly out of scope for this addendum:** any notion of *joining*,
*pairing*, *grouping*, or *aggregating* records from different upstreams by
shared lineage (e.g. matching two records that share a `parent_seq`),
including the fan-out-mismatch problem (one branch producing a different
number of records per original item than another). Those were evaluated and
deliberately rejected as framework features — see §8. This addendum's only
job is to let a stage **read from N upstream files as one interleaved
stream of individually-committed items**, exactly as it already reads from
one upstream today. Anything fancier is application logic implemented
inside a `Step` subclass using its own persisted `state`, per
`docs/design.md` §8 — the framework does not need to know or care that such
logic exists.

---

## 1. Proposed design decisions

These are the resolutions this proposal argues for, worked out in enough
detail to implement against if accepted. They're presented as a coherent
set rather than a menu, but each is independently reviewable — none is load
bearing for the others except where noted (e.g. §2.1's `src_stage` field
follows directly from the union-only scope in §0):

| Decision | Resolution |
|---|---|
| What does "multiple upstreams" mean semantically? | **Union/interleave only.** Every committed record from every declared upstream is delivered to `process()` as its own independent call, exactly like a single-upstream stage today. There is no pairing, matching, or waiting for records from different upstreams to "belong together." |
| Joining / aggregation / grouping | **Explicitly deferred, not implemented.** If an application needs to correlate records across upstreams (e.g. by `parent_seq`), it does so itself inside its `Step` subclass's `process()`, using the existing `state` mechanism as a buffer/waiting-room. The framework provides no group-closure signal, no per-parent counts, and no join primitive. See §8 for the full rationale already worked out for this decision. |
| Ordering across upstreams | **Round-robin by declared upstream order**, checked once per poll cycle. Not: interleave by `emitted_at` timestamp (avoids relying on clock comparisons across independently-written files), not: always drain one upstream before touching the next (would starve the others). |
| Does interleaving order need to be crash-persisted? | **No.** Rotation position only affects fairness/ordering of interleaving, never correctness — a restart is free to resume the rotation from the first declared upstream. Only the per-upstream *consumed position* is load-bearing and must be checkpointed (see §2). |
| Backward compatibility of `StageConfig.upstream` | **Accepts a single string (existing behavior, unchanged) or a list of strings (new).** Internally normalized to a tuple of upstream names. No pipeline using the old single-string form needs to change. |
| Record line schema | **New field required: `src_stage`.** See §2.1 — without it, `src_seq` alone becomes ambiguous once a stage has multiple upstreams, since each upstream file has its own independent `seq` numbering. |
| Checkpoint line / cache schema | **`last_consumed_seq` becomes a mapping** of upstream name → seq, for every stage (single- or multi-upstream). This is a breaking schema change to the checkpoint format; see §2.2 and §9 (migration note). |
| `Step.process()` signature | **New optional third parameter, `source`** — the name of the immediate upstream (or `"input"`) that produced the item being processed. Existing single-upstream `Step` subclasses that don't accept it continue to work unmodified; the stage runner introspects the method signature and only passes `source` if the step's `process()` accepts it. See §5. |
| Cycles / self-reference | **Same validation as today, generalized per-name.** Every name in a stage's upstream list must already be a known stage name (or `"input"`) appearing earlier in the pipeline list — no new cycle-detection logic needed, the existing forward-reference check just needs to run once per listed name instead of once per stage. |
| Fan-out mismatch across branches | **Not this framework's problem.** If two branches feeding a common downstream stage fan out by different amounts, the downstream `Step` deals with that entirely in its own `process()`/`state` logic (or doesn't handle it, and processes records independently under the union model). No new exception types, no group-closure markers. |

---

## 2. Schema changes

### 2.1 Record line — add `src_stage`

Current schema (`docs/design.md` §5.1):

```json
{"type": "record", "seq": 4821, "src_seq": 1523, "parent_seq": 4820, "emitted_at": "...", "payload": {"...": "..."}}
```

Problem: `src_seq` is the `seq` of the upstream item that produced this
record. When a stage has exactly one upstream, `src_seq` is unambiguous —
there's only one file it could have come from. Once a stage can have
*multiple* upstreams, each upstream file has its own independent, restarting
`seq` counter (starting at 1). `src_seq: 5` could mean "item 5 of `C1`" or
"item 5 of `C2`" — these are different items. Without recording which
upstream produced a given `seq`, lineage tracing becomes unreliable the
moment any stage upstream of the current one had multiple upstreams itself.

**New field, required on every record line going forward:**

```json
{"type": "record", "seq": 4821, "src_seq": 1523, "src_stage": "C1", "parent_seq": 4820, "emitted_at": "...", "payload": {"...": "..."}}
```

- `src_stage` — the name of the upstream stage (or `"input"`) that this
  record's `src_seq` refers to. Always populated, even for stages with only
  one upstream (uniform schema, no special-casing based on stage arity).
- `parent_seq` is unaffected and stays unambiguous as-is: it always traces
  back to the single, global `input.jsonl`'s `seq` space, since there is
  only ever one input file for the whole pipeline. No `parent_stage` field
  is needed.

`make_record_line()` in `src/revenant/io_utils.py` gains a required
`src_stage: str` parameter.

### 2.2 Checkpoint line / checkpoint cache — `last_consumed_seq` becomes a mapping

Current schema:

```json
{"type": "checkpoint", "last_consumed_seq": 1523, "last_emitted_seq": 4822, "state": {"...": "..."}, "committed_at": "..."}
```

New schema — `last_consumed_seq` is now an object keyed by upstream name:

```json
{
  "type": "checkpoint",
  "last_consumed_seq": {"C1": 42, "C2": 17},
  "last_emitted_seq": 4822,
  "state": {"...": "..."},
  "committed_at": "..."
}
```

- Every key in `last_consumed_seq` corresponds to one of the stage's
  declared upstream names. A single-upstream stage simply has one key
  (e.g. `{"A": 1523}`) — the schema is uniform across arities, there is no
  "int for one upstream, dict for many" special case.
- `last_emitted_seq` is unaffected — a stage still has exactly one output
  file and one emitted-seq counter, regardless of how many upstreams feed
  it.
- The checkpoint cache file (`{stage}.checkpoint.json`, §5.2 of the base
  design) mirrors this same shape for `last_consumed_seq`.

This is a **breaking format change** to an already-specified schema. See §9
for what this means for any state directories that predate this addendum.

---

## 3. `StageConfig` changes (`src/revenant/config.py`)

```python
@dataclass(frozen=True)
class StageConfig:
    name: str
    step_class: Type[Step]
    upstream: str | list[str]   # accepts either form; see normalization below
    ...

    @property
    def upstreams(self) -> tuple[str, ...]:
        """Normalized list of upstream names, in declared order.
        Always a tuple, even for a single upstream, so all downstream
        code can treat every stage uniformly regardless of arity."""
        if isinstance(self.upstream, str):
            return (self.upstream,)
        return tuple(self.upstream)

    def upstream_output_paths(self, state_dir: Path) -> dict[str, Path]:
        """Replaces the old single-path `upstream_output_path`. Maps each
        declared upstream name to its output file path. 'input' resolves
        to state_dir / 'input.jsonl' exactly as before; every other name
        resolves to state_dir / f'{name}.jsonl'."""
        return {
            name: (state_dir / "input.jsonl" if name == "input" else state_dir / f"{name}.jsonl")
            for name in self.upstreams
        }
```

- `upstream_output_path()` (singular) is removed in favor of
  `upstream_output_paths()` (plural). Any call site relying on the old
  singular method needs updating — there are two in the base implementation
  (`stage_runner.run_stage`, and indirectly `supervisor.py` via
  `stage_runner` helpers); see §5 and §6.
- `blobs_dir()`, `output_path()`, `checkpoint_path()`, `lock_path()`,
  `deadletter_path()` are all unaffected — a stage still has exactly one of
  each of these, regardless of upstream count.

### `validate_pipeline()` changes

```python
def validate_pipeline(pipeline: list[StageConfig]) -> None:
    names = [stage.name for stage in pipeline]
    if len(names) != len(set(names)):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise ValueError(f"Duplicate stage names: {', '.join(duplicates)}")

    known_names = set()
    for index, stage in enumerate(pipeline):
        upstream_names = stage.upstreams

        if len(upstream_names) != len(set(upstream_names)):
            dupes = sorted({u for u in upstream_names if upstream_names.count(u) > 1})
            raise ValueError(f"Stage {stage.name!r} lists the same upstream more than once: {', '.join(dupes)}")

        for upstream_name in upstream_names:
            if upstream_name == "input":
                continue
            if upstream_name in known_names:
                continue
            raise ValueError(f"Stage {stage.name!r} references unknown or forward-referenced upstream {upstream_name!r}")

        known_names.add(stage.name)
```

This is the same forward-reference check as today, just applied once per
name in a stage's upstream list instead of once per stage. A stage
referencing its own name (direct self-loop) is still rejected, because
`stage.name` is only added to `known_names` *after* its own upstream list is
validated — same ordering guarantee as the existing implementation.

---

## 4. Stage runner changes (`src/revenant/stage_runner.py`)

### 4.1 Resume point loading

`load_resume_point()` now returns a per-upstream mapping instead of a single
int:

```python
def load_resume_point(stage: StageConfig, state_dir: Path) -> tuple[dict[str, int], int, object]:
    """Return (last_consumed_seq_by_upstream, last_emitted_seq, state)."""
    checkpoint_path = stage.checkpoint_path(state_dir)
    if checkpoint_path.exists():
        with open(checkpoint_path) as f:
            cached = json.load(f)
        consumed = dict(cached["last_consumed_seq"])
    else:
        last = read_last_checkpoint_line(stage.output_path(state_dir))
        if last is None:
            consumed = {name: 0 for name in stage.upstreams}
            return consumed, 0, None
        consumed = dict(last["last_consumed_seq"])

    # Backfill any upstream name that's missing from a loaded checkpoint
    # (e.g. a pipeline edit added a new upstream to an existing stage
    # after some checkpoints already existed under the old shape).
    for name in stage.upstreams:
        consumed.setdefault(name, 0)

    last_emitted_seq = cached["last_emitted_seq"] if checkpoint_path.exists() else last["last_emitted_seq"]
    state = cached["state"] if checkpoint_path.exists() else last["state"]
    return consumed, last_emitted_seq, state
```

### 4.2 Reading the next item — round-robin across upstreams

Replace the single `iter_records_after(...)` call with a per-poll-cycle
round-robin across all declared upstreams:

```python
def _next_item(stage: StageConfig, state_dir: Path, consumed: dict[str, int]) -> tuple[str, dict] | None:
    """Return (source_upstream_name, item) for the next available item,
    round-robining across upstreams in declared order. Returns None if
    no upstream currently has anything new."""
    paths = stage.upstream_output_paths(state_dir)
    for name in stage.upstreams:  # fixed declared order each call; no
                                  # persisted rotation cursor needed, see §1
        item = next(iter_records_after(paths[name], consumed[name]), None)
        if item is not None:
            return name, item
    return None
```

Note: this always starts scanning from the first declared upstream on every
poll. That's a deliberate simplification — it means a very "chatty"
first-listed upstream can, in principle, be served first indefinitely if it
always has something ready before the loop reaches later upstreams. Given
the interleave-only semantics (§1), no correctness property depends on
fairness here, only latency for the less-chatty upstream. If strict
round-robin fairness across polls turns out to matter in practice, this is
the one place to add a persisted-or-in-memory rotation cursor later — it's
an isolated, backward-compatible change confined to this function.

### 4.3 Main loop changes

```python
def run_stage(stage, state_dir, once=False, pipeline=None) -> None:
    lock_path = stage.lock_path(state_dir)
    acquire_lock(lock_path)
    try:
        consumed, last_emitted_seq, saved_state = load_resume_point(stage, state_dir)

        step = stage.step_class()
        state = step.load(saved_state)
        process_accepts_source = _process_accepts_source(step)  # see §5

        input_final_seq = read_input_final_seq(state_dir)

        while True:
            found = _next_item(stage, state_dir, consumed)

            if found is None:
                stage_done = is_stage_durably_done(stage, state_dir, input_final_seq, pipeline)
                if stage_done or once:
                    return
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            source_name, next_item = found

            outputs = []
            try:
                if process_accepts_source:
                    gen = step.process(next_item["payload"], state, source_name)
                else:
                    gen = step.process(next_item["payload"], state)
                while True:
                    outputs.append(next(gen))
            except StopIteration as stop:
                new_state = stop.value
            except RetryableError:
                time.sleep(POLL_INTERVAL_SECONDS)
                if once:
                    return
                continue
            except SkipItem:
                new_state = state
                outputs = []
                # deadletter handling below, source_name included

            lines = []
            seq = last_emitted_seq
            for payload in outputs:
                seq += 1
                lines.append(
                    make_record_line(
                        seq=seq,
                        src_seq=next_item["seq"],
                        src_stage=source_name,       # new field, see §2.1
                        parent_seq=next_item.get("parent_seq", next_item["seq"]),
                        payload=payload,
                        emitted_at=_now_iso(),
                    )
                )
            checkpoint_state = step.checkpoint(new_state)
            new_consumed = dict(consumed)
            new_consumed[source_name] = next_item["seq"]
            lines.append(
                make_checkpoint_line(
                    last_consumed_seq=new_consumed,   # now a dict, see §2.2
                    last_emitted_seq=seq,
                    state=checkpoint_state,
                    committed_at=_now_iso(),
                )
            )
            atomic_append_lines(stage.output_path(state_dir), lines)

            # ... deadletter write (if SkipItem raised) should also record
            # src_stage=source_name for consistency ...

            consumed = new_consumed
            last_emitted_seq = seq
            state = new_state

            atomic_write_json(stage.checkpoint_path(state_dir), {
                "stage": stage.name,
                "last_consumed_seq": consumed,
                "last_emitted_seq": last_emitted_seq,
                "state": checkpoint_state,
                "updated_at": _now_iso(),
                "schema_version": 2,   # bump, see §9
            })

            if once:
                return
    finally:
        release_lock(lock_path)
```

Key points:

- Exactly **one** upstream's consumed position advances per commit block —
  the one the processed item came from. This is unchanged in spirit from
  today's single-upstream loop; it's still "read one item, process it,
  commit one block," just with an extra dimension for *which* upstream's
  counter to bump.
- The commit block itself (buffer outputs in memory, write records +
  checkpoint as one atomic append) is **completely unchanged** — everything
  in `docs/design.md` §6 continues to hold. A multi-upstream stage still
  processes exactly one input item per iteration and still can't observe
  a partial commit from any upstream, for the same reasons as before.

### 4.4 `is_stage_durably_done()` changes

```python
def is_stage_durably_done(stage, state_dir, input_final_seq, pipeline=None) -> bool:
    my_consumed, _, _ = load_resume_point(stage, state_dir)

    for upstream_name in stage.upstreams:
        if upstream_name == "input":
            final_seq = input_final_seq if input_final_seq is not None else 0
            if my_consumed.get(upstream_name, 0) < final_seq:
                return False
            continue

        if pipeline is None:
            raise ValueError("pipeline is required to resolve upstream drain state")

        upstream_stage = next((c for c in pipeline if c.name == upstream_name), None)
        if upstream_stage is None:
            raise ValueError(f"Unknown upstream stage {upstream_name!r} for {stage.name!r}")

        upstream_done = is_stage_durably_done(upstream_stage, state_dir, input_final_seq, pipeline)
        _, upstream_last_emitted, _ = load_resume_point(upstream_stage, state_dir)
        if not (upstream_done and my_consumed.get(upstream_name, 0) >= upstream_last_emitted):
            return False

    return True
```

This is a direct generalization of the existing recursive definition in
`docs/design.md` §7: a stage is durably done once it is durably done
*with respect to every one of its upstreams individually* — every upstream
must itself be durably done, and this stage's consumed position for that
upstream must have caught up to that upstream's frozen `last_emitted_seq`.
The recursion is still well-founded for the same reason as before: once an
upstream is durably done, its `last_emitted_seq` is frozen forever, so each
individual per-upstream check is stable no matter when it's evaluated.

A DAG with fan-out and fan-in (as in the motivating example in §0) is still
guaranteed acyclic by `validate_pipeline`'s forward-reference rule, so this
recursion always terminates.

---

## 5. `Step` interface changes (`src/revenant/step.py`)

```python
def process(self, payload: dict, state: Any, source: str | None = None) -> Iterator[dict]:
    """Called once per input item.

    `source` is the name of the immediate upstream (or "input") that
    produced this item, present only for stages with more than one
    declared upstream. Single-upstream Step subclasses may omit this
    parameter entirely -- the stage runner detects whether process()
    accepts it and calls accordingly (see stage_runner._process_accepts_source).
    ...
    """
```

No change to the base class's default behavior or to `load()` /
`checkpoint()`. This is additive: existing subclasses with the two-argument
`process(self, payload, state)` signature are entirely unaffected.

**Backward-compatible dispatch** (new helper in `stage_runner.py`):

```python
import inspect

def _process_accepts_source(step: Step) -> bool:
    """Return True if step.process() accepts a third positional
    argument (or **kwargs), so the stage runner knows whether to pass
    `source`. Computed once per stage-process startup, not per item."""
    sig = inspect.signature(step.process)
    params = [p for p in sig.parameters.values() if p.name != "self"]
    if any(p.kind == inspect.Parameter.VAR_POSITIONAL for p in params):
        return True
    return len(params) >= 3
```

This means a `Step` subclass only needs to add the `source` parameter if it
actually cares which upstream an item came from (e.g. a multi-upstream
stage doing its own application-level correlation, per §8). A step attached
to a single-upstream stage never needs to change.

---

## 6. Supervisor / CLI changes (`src/revenant/supervisor.py`, `src/revenant/cli.py`)

- `supervisor.run_supervisor()` calls `load_resume_point()` to populate
  `pipeline.status.json`'s `last_consumed_seq` field per stage — this now
  becomes the per-upstream dict rather than a single int. Update the status
  JSON shape accordingly, e.g.:

  ```json
  "D": {"pid": 9012, "last_consumed_seq": {"C1": 42, "C2": 17}, "last_emitted_seq": 59, "durably_done": false}
  ```

- `cli.py`'s `cmd_status()` prints `cp['last_consumed_seq']` directly today;
  since this is now a dict, either print it as-is (acceptable, still
  human-readable JSON-ish output) or format it more nicely, e.g.:

  ```python
  consumed_str = ", ".join(f"{k}={v}" for k, v in cp["last_consumed_seq"].items())
  print(f"{stage.name}: consumed=[{consumed_str}] emitted={cp['last_emitted_seq']} ...")
  ```

- No changes needed to `spawn_stage()`, signal handling, or the
  process-monitoring loop itself — those are agnostic to a stage's upstream
  count.

---

## 7. Example usage

**Declaring a fan-in stage:**

```python
PIPELINE = [
    StageConfig(name="A", step_class=StepA, upstream="input"),
    StageConfig(name="B1", step_class=StepB1, upstream="A"),
    StageConfig(name="B2", step_class=StepB2, upstream="A"),
    StageConfig(name="C1", step_class=StepC1, upstream="B1"),
    StageConfig(name="C2", step_class=StepC2, upstream="B2"),
    StageConfig(name="D", step_class=StepD, upstream=["C1", "C2"]),
]
```

**A `Step` that cares which upstream produced each item** (still just
interleaving, no joining):

```python
class StepD(Step):
    def load(self, saved_state):
        return saved_state if saved_state is not None else {"c1_count": 0, "c2_count": 0}

    def process(self, payload, state, source):
        if source == "C1":
            state["c1_count"] += 1
        elif source == "C2":
            state["c2_count"] += 1
        yield {"source": source, "payload": payload}
        return state
```

**A `Step` that does its own application-level joining on top of the
interleaved stream** (illustrates that this is entirely application code,
not framework code — included here only to show the pattern is possible,
not to specify any framework behavior):

```python
class JoinByParent(Step):
    """Application-level join: buffers whichever side arrives first,
    keyed by parent_seq, and only yields once both sides for a given
    parent_seq have been seen. Entirely built on ordinary `state` --
    the framework has no idea this pairing logic exists."""

    def load(self, saved_state):
        return saved_state if saved_state is not None else {"pending": {}}

    def process(self, payload, state, source):
        parent_seq = payload["parent_seq"]
        pending = state["pending"]
        bucket = pending.setdefault(str(parent_seq), {})
        bucket[source] = payload

        if "C1" in bucket and "C2" in bucket:
            yield {"parent_seq": parent_seq, "c1": bucket["C1"], "c2": bucket["C2"]}
            del pending[str(parent_seq)]

        return {"pending": pending}
```

This example does **not** handle the fan-out-mismatch case (§0) — if either
branch produces more than one record per `parent_seq`, this simple
implementation would overwrite `bucket[source]` with the last one seen
rather than collecting all of them. That's expected: this addendum doesn't
attempt to solve that problem, and any `Step` that needs to is responsible
for its own, application-specific bucketing/collection logic.

---

## 8. Why joining/aggregation is deliberately not a framework feature

(Restating the rationale explicitly here since it's the central decision
this addendum rests on, so a future reader doesn't need to rediscover it.)

- **No single correct semantics.** "Consumes C1 and C2" could reasonably
  mean order-independent union, pairing by shared lineage (`parent_seq`),
  waiting for a full group when either side fanned out, or a cross-product
  — which one is right depends entirely on what the downstream step is
  trying to accomplish, not on anything the framework can infer from file
  contents.
- **Fan-out breaks simple pairing.** The moment either upstream branch can
  emit more than one record per original input item, "wait for the
  matching record from each side" stops being well-defined — you'd need an
  explicit signal that a branch is *done* producing for a given parent
  (a concept the current schema has no representation for), plus a policy
  for one-to-many or cross-product pairing. Building this generically,
  speculatively, without a concrete required semantics in hand, risks
  encoding the wrong policy into the framework.
- **`state` already solves this without new framework surface.** Every
  `Step` already gets a JSON-serializable `state` value threaded through
  `load()` / `process()` / `checkpoint()` and durably checkpointed after
  every commit (`docs/design.md` §8). A join/aggregation buffer is just
  another shape of accumulator state, no different in kind from
  `CountWords`' running `{word: count}` table in `examples/wordcount/`.
  It gets crash-safety for free from the existing atomic-commit-block
  mechanism (§6 of the base design) with zero new framework code.
- **Consistent with the framework's existing philosophy.** The
  planner/worker pattern (`docs/design.md` §6, `examples/planner_worker/`)
  already establishes the precedent: rather than teaching the framework
  new intra-item or cross-item semantics, keep the framework's primitive
  dumb (one input item in, generator out, one atomic commit) and let
  pipeline *shape* plus ordinary `Step` logic carry the complexity.

---

## 9. Migration note (schema version bump)

The checkpoint line/cache schema change (§2.2) and record line schema
change (§2.1) are breaking changes to an already-specified on-disk format.
Concretely:

- Bump the checkpoint cache's `schema_version` field from `1` to `2`
  (see `stage.checkpoint_path()` writes in `stage_runner.py`).
- Any `state/` directory containing checkpoints written under the old
  (`last_consumed_seq: int`) schema will not be loadable by
  `load_resume_point()` as written in §4.1 — it expects a dict and will
  raise if it gets an int. Decide one of:
  - **Reset-only migration** (simplest, recommended if no long-running
    production pipelines predate this addendum): document that upgrading
    requires starting from an empty `state/` directory.
  - **Best-effort migration shim**: if `cached["last_consumed_seq"]` is
    found to be an `int` rather than a `dict` on load, treat it as
    `{stage.upstreams[0]: cached["last_consumed_seq"]}` — valid only
    because a pre-addendum stage always had exactly one upstream. Old
    record lines missing `src_stage` would similarly need a fallback
    (assume `stage.upstreams[0]` for any record lacking the field). This
    is more forgiving but adds permanent complexity for a one-time
    transition; recommend against unless there's a concrete need to
    upgrade in place.

---

## 10. Implementation checklist

- [ ] `StageConfig.upstream` accepts `str | list[str]`; add `upstreams`
      property (normalized tuple) and `upstream_output_paths()` (replaces
      `upstream_output_path()`).
- [ ] `validate_pipeline()` updated to iterate each stage's `upstreams`,
      reject duplicate names within one stage's list, and reject unknown /
      forward-referenced upstream names per-name (§3).
- [ ] `make_record_line()` gains required `src_stage: str` parameter;
      every call site updated to pass the source upstream name (§2.1).
- [ ] `make_checkpoint_line()` / checkpoint cache writer updated so
      `last_consumed_seq` is always a `{upstream_name: seq}` dict, for
      every stage regardless of upstream count (§2.2).
- [ ] `load_resume_point()` returns `(dict[str, int], int, state)`; handles
      backfilling missing upstream keys (§4.1).
- [ ] New `_next_item()` helper implementing round-robin polling across a
      stage's declared upstreams (§4.2).
- [ ] `run_stage()` updated: tracks `consumed: dict[str, int]`, advances
      only the source upstream's entry per commit, passes `source_name`
      into `process()` when accepted (§4.3).
- [ ] New `_process_accepts_source()` helper using `inspect.signature` for
      backward-compatible dispatch to `Step.process()` (§5).
- [ ] `is_stage_durably_done()` updated to require every declared upstream
      to be individually durably done and fully consumed (§4.4).
- [ ] `supervisor.py` status-building code and `cli.py`'s `cmd_status()`
      updated to handle `last_consumed_seq` as a dict (§6).
- [ ] `Step.process()` docstring updated to document the optional `source`
      parameter (§5); no change to `load()` / `checkpoint()`.
- [ ] Checkpoint cache `schema_version` bumped to `2`; migration approach
      chosen and documented (§9).
- [ ] Unit tests (suggested, mirroring `tests/test_smoke.py` style):
  - `validate_pipeline()` accepts a stage with `upstream=["A", "B"]`
    where both are declared earlier.
  - `validate_pipeline()` rejects a stage listing the same upstream name
    twice, and rejects an unknown/forward-referenced name inside a list.
  - A `run_stage()` test with two upstream files, verifying items from
    both are delivered to `process()` (order not asserted beyond "both
    appear"), and that each upstream's consumed position advances
    independently and correctly.
  - `is_stage_durably_done()` returns `False` if only one of two
    upstreams has drained, and `True` once both have.
  - A `Step` subclass with a 2-arg `process()` (no `source`) still runs
    correctly on a multi-upstream stage (backward-compat dispatch test).
  - A `Step` subclass with a 3-arg `process()` correctly receives the
    right `source` name for items from each upstream.
- [ ] `docs/design.md` §11 (Open items) gets a one-line pointer to this
      addendum, same treatment as the existing blob-storage pointer.
- [ ] No changes expected to `io_utils.atomic_append_lines`,
      `atomic_write_json`, or the crash-safety/atomicity model in
      `docs/design.md` §6 — a multi-upstream stage still processes and
      commits exactly one item at a time. If the implementer finds a need
      to touch the atomicity primitives themselves, stop and flag it —
      that would indicate this design needs revisiting.
