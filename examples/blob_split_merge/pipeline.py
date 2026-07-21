"""
Example pipeline: blob split/merge.

Demonstrates:
  - a stateless stage that fans out one input file into many blob-backed
    parts (SplitFile: one file -> many part records)
  - a stateful stage that reassembles those parts once all fragments
    are present (MergeFile: collect parts until the full file is ready)

This pipeline is a reference for the Step API shape (docs/design.md,
section 8) and the blob-store integration pattern, not a performance
example.
"""

from __future__ import annotations

from revenant.config import StageConfig
from revenant.step import Step


class SplitFile(Step):
    """Stateless: split one input file into blob-backed parts."""

    def process(self, payload, state):
        input = payload.get("input", "")
        with open(input) as f:
            lines = [l for l in f]
        total = len(lines)
        for l in lines:
            # Each line becomes a separate blob-backed part that can be
            # merged later once the full input has arrived.
            part = self.blob_store.write(bytes(l, 'utf8'))
            yield {'input': input, 'part': part, 'total': total}
        return state

class MergeFile(Step):
    """Stateful: collect blob parts until the file can be reassembled."""

    def process(self, payload, state):
        state = state or {"parts": {}}
        input = payload.get("input")
        part = payload.get("part")
        total = payload.get("total")
        parts = state.get("parts", {}).get(input, [])
        parts.append(part)
        if len(parts) < total:
            # Keep gathering fragments for this input until all expected
            # parts have arrived.
            state["parts"][input] = parts
            return state
        # Once every fragment is present, read them back from blob storage
        # and emit the merged blob for the original input.
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
