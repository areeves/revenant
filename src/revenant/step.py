"""
Step interface. See docs/design.md, section 8, for the full rationale.

Implement one subclass of `Step` per pipeline stage.
"""

from __future__ import annotations

from typing import Any, Iterator


class RetryableError(Exception):
    """Raise to indicate a transient failure.

    The framework retries this same item without advancing the checkpoint.
    Backoff and max-attempts escalation are not yet implemented; the
    current behavior is an unconditional retry with a fixed poll-interval
    delay.
    """


class SkipItem(Exception):
    """Raise to deliberately drop this item.

    The framework will emit nothing for it, advance the checkpoint past
    it, and record it in the stage's dead-letter file.
    """


class Step:
    """Base class for a single pipeline stage's processing logic.

    Any other exception raised from `process()` is treated as a
    process-fatal crash: it is not caught, the checkpoint is left
    untouched, and the stage process exits non-zero. On restart, the
    same item is retried from scratch. This is intentional — see
    docs/design.md section 8.

    Attributes set by stage runner:
      blob_store: BlobStore   -- for writing/reading binary artifacts
                                  too large or unsuitable to embed in
                                  .jsonl payloads. See
                                  docs/design-blob-storage.md.
      state_dir: Path         -- the pipeline's state directory, for
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

    def load(self, saved_state: Any | None) -> Any:
        """Called once when the stage process starts.

        Do one-time expensive setup here (load a model, open resources).
        `saved_state` is the last commit's persisted state, or None on a
        fresh start. Return the initial in-memory state to thread
        through `process()`. The returned value need not be
        JSON-serializable.
        """
        return saved_state

    def process(self, payload: dict, state: Any) -> Iterator[dict]:
        """Called once per input item.

        A generator: `yield` each output payload (dict) as it's
        produced -- zero, one, or many times. The framework buffers all
        yields in memory and only commits them to disk once this
        generator fully completes (see docs/design.md section 6).

        Must `return new_state` (captured via the generator's
        StopIteration value) with the updated state to carry into the
        next item.
        """
        raise NotImplementedError
        yield  # pragma: no cover - makes this a generator function

    def checkpoint(self, state: Any) -> Any:
        """Return the JSON-serializable projection of `state` to persist.

        Lets a step keep large/unserializable objects (e.g. a loaded
        model) bundled with state in memory without those ever being
        written to disk. Default: persist state as-is.
        """
        return state
