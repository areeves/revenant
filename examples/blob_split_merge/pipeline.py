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


class SplitFile(Step):
    def process(self, payload, state):
        input = payload.get("input", "")
        with open(input) as f:
            lines = [l for l in f]
        total = len(lines)
        for l in lines:
            part = self.blob_store.write(bytes(l, 'utf8'))
            yield {'input': input, 'part': part, 'total': total}
        return state

class MergeFile(Step):
    def process(self, payload, state):
        state = state or {"parts": {}}
        input = payload.get("input")
        part = payload.get("part")
        total = payload.get("total")
        parts = state.get("parts", {}).get(input, [])
        parts.append(part)
        if len(parts) < total:
            state["parts"][input] = parts
            return state
        texts = [self.blob_store.resolve(p).read_text() for p in parts]
        text = "".join(texts)
        result = self.blob_store.write(bytes(text, 'utf8'))
        yield {"input": input, "total":total, "result":result}
        del(state["parts"][input])
        return state


PIPELINE = [
    StageConfig(name="split", step_class=SplitFile, upstream="input"),
    StageConfig(name="merge", step_class=MergeFile, upstream="split"),
]
