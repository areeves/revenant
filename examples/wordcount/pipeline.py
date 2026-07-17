"""
Example pipeline: word counting.

Demonstrates:
  - a stateless stage that fans out one input into many outputs
    (SplitWords: one line of text -> many word records)
  - a stateful stage that carries an accumulator across items
    (CountWords: a running word -> count table, checkpointed each item)

This pipeline is a reference for the Step API shape (docs/design.md,
section 8), not a performance example.
"""

from __future__ import annotations

from revenant.config import StageConfig
from revenant.step import Step


class SplitWords(Step):
    """Stateless: splits {"text": "..."} into one output per word.

    A clean example of the "zero, one, or many outputs per input"
    requirement -- an empty line yields zero outputs, a normal line
    yields many.
    """

    def process(self, payload, state):
        text = payload.get("text", "")
        for word in text.split():
            yield {"word": word.lower().strip(".,!?;:")}
        return state


class CountWords(Step):
    """Stateful: maintains a running {word: count} table.

    `state` here is small and trivially JSON-serializable, so
    checkpoint() is the default identity -- load()/checkpoint() only
    need to do real work when state holds something that *isn't*
    directly serializable (e.g. a loaded model handle).
    """

    def load(self, saved_state):
        return saved_state if saved_state is not None else {"counts": {}}

    def process(self, payload, state):
        word = payload["word"]
        counts = state["counts"]
        counts[word] = counts.get(word, 0) + 1
        # Emit a running snapshot for this word every time it updates.
        yield {"word": word, "count": counts[word]}
        return {"counts": counts}


PIPELINE = [
    StageConfig(name="split", step_class=SplitWords, upstream="input"),
    StageConfig(name="count", step_class=CountWords, upstream="split"),
]
