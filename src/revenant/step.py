"""
Step interface. See docs/design.md, section 8, for the full rationale.

Implement one subclass of `Step` per pipeline stage.
"""

from __future__ import annotations

from typing import Any, Iterator


class RetryableError(Exception):
    """Raise to indicate a transient failure.

    The framework will retry this same item (with backoff), without
    advancing the checkpoint.
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
