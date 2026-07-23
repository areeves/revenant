# revenant — Design Addendum: Blob Storage

**Status:** Approved for implementation.
**Relationship to `docs/design.md`:** This is an addendum, not a replacement.
It extends the existing design (sections 3–8 in particular) to cover binary
artifacts that shouldn't be embedded directly in `.jsonl` payloads. Nothing
here changes existing behavior for pipelines that don't use blobs — this is
purely additive.

---

## 0. Problem

Some steps produce or consume binary data (images, audio/video, model
weights, etc.) that's impractical to embed as JSON in a stage's `.jsonl`
output — either because it's too large to buffer/serialize sanely, or
because it needs to be handed to an external tool (e.g. `ffmpeg`) as a real
file rather than piped through Python as bytes.

The pattern: a step writes binary data to its own dedicated directory on
disk, under a unique, content-derived name, and yields only the **path** to
that file as a normal string field in its payload. Downstream steps resolve
that path back to a real file.

This addendum specifies exactly how that directory is laid out, what API a
`Step` uses to write into it, how the path gets resolved downstream, and
what invariants must hold for this to stay consistent with the crash-safety
model in `docs/design.md` §6.

---

## 1. Design decisions (final)

These were discussed and settled; implement against these, don't
re-litigate them:

| Decision | Resolution |
|---|---|
| One blob store per stage, or shared across stages? | **One per stage.** Each stage owns exactly one blob directory, symmetric with how it owns exactly one `.jsonl` output file. No stage reads another stage's blob directory. |
| How far downstream can a blob path be forwarded? | **One hop only.** A blob written by stage A is referenced by `A.jsonl` and may be read by stage B (the stage with `upstream="A"`). B must **not** forward that same path string into its own yield. If B needs the content to survive past its own processing, it re-blobs it (copies or derives a new artifact) into `B.blobs/`. This is enforced by convention/documentation only, not by the framework (see §6 below). |
| Content-addressing? | **Yes, always.** Every blob is named by the SHA-256 hex digest of its bytes. No opt-out mode. This gives free dedup and makes retried (deterministic) attempts idempotent — an identical retry writes to the same final path and is a no-op. |
| Path format in payloads | **Relative to `state_dir`**, as a plain string. No new field-marking/declaration mechanism — a blob path is just a normal payload value, same as any other string. |
| How `Step` accesses the store | **Plain attributes**, set by the stage runner before `step.load()` is called: `step.blob_store` and `step.state_dir`. No change to the `Step.load()` / `process()` / `checkpoint()` signatures. |
| Two-phase writes (external tools) vs. in-memory bytes | **Both supported**, via one underlying `commit()`. `write(bytes)` is a convenience wrapper for the in-memory case; `reserve_path()` / `reserve_dir()` + `commit()` is for external tools (e.g. `ffmpeg`) that need to write to a real path themselves. |
| Garbage collection | **Deferred entirely.** Not implemented, not stubbed, not scheduled. Blob directories grow unboundedly, same caveat as `docs/design.md` §11 already accepts for `.jsonl` growth. The one-hop rule above is what keeps future GC tractable when it's eventually built (liveness of a blob becomes computable from the downstream stage's `last_consumed_seq` checkpoint — no grace-window heuristics needed) but no GC code should be written now. |
| Blob-field declarations, `revenant inspect`, size reporting in `revenant status` | **Deferred entirely.** Do not build. |
| Subprocess failure handling (e.g. `ffmpeg` exits non-zero) | **No new error-handling path.** An uncaught subprocess failure is process-fatal, per the existing table in `docs/design.md` §8 ("Anything else (unhandled)"). Do not add blob- or subprocess-specific exception types. |
| Schema versioning | **Not applicable.** No existing on-disk schema changes; this addendum introduces a new directory and a new `Step` API surface only. |

---

## 2. Directory layout

One new directory per stage, parallel to its existing `.jsonl` / checkpoint
/ lock files:

```
state/
  A.jsonl
  A.checkpoint.json
  A.lock
  A.blobs/
    .scratch/                      # transient, used during writes only
    3f/
      3f9a1c...-chunk_0001.ts      # final, content-addressed
  B.jsonl
  B.blobs/
  ...
```

- Two-level sharding: the first two hex characters of the digest become a
  subdirectory (`3f/`), to avoid one flat directory accumulating thousands
  of entries.
- `.scratch/` lives *inside* `A.blobs/` specifically so that the final
  `os.replace()` move in `commit()` is guaranteed to be a same-filesystem
  rename (and therefore atomic), not a cross-filesystem copy. This must not
  be reconfigurable to point elsewhere.
- `.gitignore` already excludes `/state/` wholesale, so no change needed
  there.

---

## 3. `StageConfig` changes

Add one method, following the existing pattern in `src/revenant/config.py`:

```python
def blobs_dir(self, state_dir: Path) -> Path:
    return state_dir / f"{self.name}.blobs"
```

No other changes to `StageConfig` or `validate_pipeline()`.

---

## 4. `BlobStore` (new module: `src/revenant/blob_store.py`)

```python
"""
Content-addressed blob storage for stage outputs that shouldn't be
embedded directly in .jsonl payloads (e.g. images, media files). See
docs/design-blob-storage.md for the full rationale.

One BlobStore per stage process, constructed once at stage-runner
startup and attached to the Step instance as `step.blob_store`.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path


class BlobStore:
    def __init__(self, blobs_dir: Path, state_dir: Path):
        self.blobs_dir = blobs_dir
        self.state_dir = state_dir
        self.scratch_dir = blobs_dir / ".scratch"
        self.blobs_dir.mkdir(parents=True, exist_ok=True)
        self.scratch_dir.mkdir(parents=True, exist_ok=True)

    def reserve_path(self, suffix: str = "") -> Path:
        """Return a scratch path an external tool can write to directly."""
        return self.scratch_dir / f"{uuid.uuid4().hex}{suffix}"

    def reserve_dir(self) -> Path:
        """Same idea, for tools that produce many output files at once
        (e.g. ffmpeg segmenting one input into N chunk files)."""
        d = self.scratch_dir / uuid.uuid4().hex
        d.mkdir(parents=True, exist_ok=True)
        return d

    def write(self, data: bytes, suffix: str = "") -> str:
        """Convenience wrapper: write in-memory bytes and commit in one call."""
        scratch_path = self.reserve_path(suffix=suffix)
        fd = os.open(scratch_path, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        return self.commit(scratch_path, suffix=suffix)

    def commit(self, scratch_path: Path, suffix: str | None = None) -> str:
        """Hash a completed scratch file and move it into content-addressed
        storage. Returns a path relative to state_dir, suitable for storing
        directly in a payload dict.

        `scratch_path` must already be fully written and fsync'd before
        calling this -- commit() does not fsync the source file itself,
        only the containing-directory fsync implied by os.replace on most
        Linux filesystems. Callers writing via write() get this for free;
        callers using reserve_path()/reserve_dir() directly (e.g. an
        external subprocess) are responsible for the tool having finished
        and flushed before commit() is called.
        """
        digest = _sha256_file(scratch_path)
        actual_suffix = suffix if suffix is not None else scratch_path.suffix
        shard = digest[:2]
        name = f"{digest}{actual_suffix}"
        target_dir = self.blobs_dir / shard
        target_dir.mkdir(parents=True, exist_ok=True)
        final_path = target_dir / name

        if final_path.exists():
            # Identical content already stored (e.g. a deterministic retry
            # after a crash) -- discard the duplicate scratch file.
            scratch_path.unlink(missing_ok=True)
        else:
            os.replace(scratch_path, final_path)  # atomic: same filesystem

        return str(final_path.relative_to(self.state_dir))

    def resolve(self, relative_path: str) -> Path:
        """Turn a payload's stored relative path back into a real path."""
        return self.state_dir / relative_path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
```

Notes for the implementer:

- `reserve_dir()` returning a directory (not just a path) is needed for the
  `ffmpeg -f segment` case, where the tool decides its own output filenames
  inside that directory; the step then walks the directory afterward and
  calls `commit()` once per resulting file.
- `commit()`'s dedup-on-exists check means a deterministic step that's
  retried after a crash (per `docs/design.md` §8's determinism note) simply
  no-ops on the second write instead of producing a duplicate blob under a
  different name.
- No cleanup of `.scratch/` is implemented here. Interrupted attempts leave
  orphaned files there; this is accepted, same bucket as the deferred GC
  item in §1.

---

## 5. Stage runner wiring (`src/revenant/stage_runner.py`)

In `run_stage()`, immediately after the existing step construction:

```python
step = stage.step_class()
state = step.load(saved_state)
```

add:

```python
step = stage.step_class()
step.blob_store = BlobStore(stage.blobs_dir(state_dir), state_dir)
step.state_dir = state_dir
state = step.load(saved_state)
```

(Attribute assignment happens *before* `load()` so a step's `load()` can
also use `self.blob_store` for setup if it ever needs to, e.g. pre-warming
something — not a required use case today, just don't foreclose it by
ordering it after `load()`.)

Import `BlobStore` from the new `revenant.blob_store` module at the top of
`stage_runner.py`.

No other changes to `run_stage()` — blob writes happen inside a step's own
`process()` body using `self.blob_store`, and the resulting path strings
flow through the existing `yield {...}` / commit-block mechanism completely
unchanged. The atomic-commit-block logic in `stage_runner.py` and
`io_utils.py` requires **no modification**: a blob path is just a string
value inside a payload dict, indistinguishable from any other payload
field as far as `make_record_line` / `atomic_append_lines` are concerned.

---

## 6. `Step` base class (`src/revenant/step.py`)

No changes to the class itself. Add a docstring note near the class or in
`docs/design.md` documenting the two new attributes that get set
externally by the runner:

```python
class Step:
    """
    ...

    In addition to the methods below, a running Step instance has two
    attributes set by the stage runner before load() is called:

      self.blob_store: BlobStore   -- for writing/reading binary artifacts
                                       too large or unsuitable to embed in
                                       .jsonl payloads. See
                                       docs/design-blob-storage.md.
      self.state_dir: Path         -- the pipeline's state directory, for
                                       resolving blob paths read from an
                                       upstream payload.

    IMPORTANT: a payload field containing a blob path (as returned by
    self.blob_store.write()/commit()) must not be forwarded unchanged into
    this step's own yield. If downstream needs the referenced content to
    persist past this stage, re-write it via self.blob_store into this
    stage's own blob directory. Blob paths are one-hop only -- see
    docs/design-blob-storage.md section 1. This is not enforced by the
    framework; violating it will not fail loudly today, but will make a
    blob unsafe to reclaim whenever garbage collection is eventually
    implemented.
    """
```

---

## 7. Example usage (for reference / a test fixture)

**In-memory bytes (e.g. a rendered image):**

```python
class RenderPage(Step):
    def process(self, payload, state):
        rendered_bytes = render_to_png(payload)
        path = self.blob_store.write(rendered_bytes, suffix=".png")
        yield {"document_id": payload["document_id"], "page": payload["page"], "image_path": path}
        return state
```

**External tool writing directly to disk (ffmpeg segmenting):**

```python
class SegmentMedia(Step):
    def process(self, payload, state):
        input_path = self.blob_store.resolve(payload["media_path"])
        out_dir = self.blob_store.reserve_dir()

        subprocess.run(
            [
                "ffmpeg", "-i", str(input_path),
                "-f", "segment", "-segment_time", "30",
                "-c", "copy", str(out_dir / "chunk_%04d.ts"),
            ],
            check=True,  # non-zero exit -> uncaught -> process-fatal, per design.md §8
        )

        for chunk_file in sorted(out_dir.glob("chunk_*.ts")):
            relative_path = self.blob_store.commit(chunk_file)
            yield {"chunk_path": relative_path}

        shutil.rmtree(out_dir, ignore_errors=True)
        return state
```

**Downstream consumer:**

```python
class ProcessChunk(Step):
    def process(self, payload, state):
        chunk_path = self.blob_store.resolve(payload["chunk_path"])
        # ... do something with the real file at chunk_path ...
        # NOTE: do not yield payload["chunk_path"] onward unchanged.
        yield {"result": summarize(chunk_path)}
        return state
```

---

## 8. Crash-safety argument (for review, not for the developer to re-derive)

This section exists so a reviewer can check the implementation against the
intended invariant, per `docs/design.md` §6's standard of closing races
structurally rather than by convention wherever possible.

- A blob's *existence on disk* before its referencing record is committed
  is not itself observable by anything — nothing reads `A.blobs/` directory
  listings to discover work. The only way to reach a blob is via a path
  string inside an already-committed `.jsonl` record.
- Therefore a crash between `commit()` and the stage's subsequent
  `atomic_append_lines()` call (which writes the record + checkpoint) is
  safe: the blob is an orphan, structurally identical to a discarded
  non-deterministic attempt already accepted by `docs/design.md` §8's
  determinism note. Nothing downstream can have observed it.
- `commit()`'s `os.replace()` is same-filesystem (scratch is nested under
  the same `{stage}.blobs/`), so it carries the same atomicity guarantee
  already relied on elsewhere (`atomic_write_json`, `atomic_append_lines`),
  subject to the same Docker bind-mount filesystem caveat already
  documented in `docs/design.md` §6.
- The one-hop rule (§1) is what keeps a *future* GC pass correct without
  needing time-based grace windows: liveness of any blob in `A.blobs/`
  reduces to a single comparison against stage B's `last_consumed_seq`
  checkpoint. This addendum does not implement that GC pass, but the
  layout and the one-hop rule are chosen so that implementing it later
  doesn't require revisiting this design.

---

## 9. Implementation checklist

- [ ] New file `src/revenant/blob_store.py` — `BlobStore` class as in §4.
- [ ] `StageConfig.blobs_dir()` added to `src/revenant/config.py`.
- [ ] `stage_runner.run_stage()` constructs `BlobStore` and attaches
      `step.blob_store` / `step.state_dir` before calling `step.load()`.
- [ ] Docstring note added to `Step` in `src/revenant/step.py` per §6.
- [ ] This file committed as `docs/design-blob-storage.md`, and a
      one-line pointer added to `docs/design.md` §11 ("Open items") noting
      that blob storage exists, is documented separately, and that GC for
      it is deferred alongside the existing `.jsonl` compaction item.
- [ ] Unit tests (suggested, mirroring `tests/test_smoke.py` style):
  - `BlobStore.write()` round-trips bytes and returns a path resolvable
    via `resolve()`.
  - Writing identical bytes twice (simulating a deterministic retry)
    produces the same final path and does not error.
  - `reserve_dir()` + manual file creation + `commit()` per file works for
    the multi-output case (no subprocess/ffmpeg dependency needed in the
    test — just write files directly into the reserved dir).
  - A full `run_stage()` test where a `Step` subclass calls
    `self.blob_store.write(...)` inside `process()` and the resulting path
    shows up correctly in the committed `.jsonl` record.
- [ ] No changes to `io_utils.py`, `supervisor.py`, or `cli.py` are
      expected. If the implementer finds a need to touch them, stop and
      flag it — that would indicate a decision from §1 needs revisiting.
