"""
Example pipeline: planner/worker split.

Demonstrates the pattern from docs/design.md, section 6, for a step
that would otherwise want to stream partial results downstream before
it's fully finished with one input item.

Because a stage's outputs only become visible downstream once the
*entire* item's commit block (all yields + checkpoint) is written
atomically, a single stage cannot give downstream early access to
some of its outputs while still working on more. If a step's natural
shape is "one input produces many independent, expensive units of
work" (e.g. render each page of a document, call a model once per
chunk of a long input), split it into two stages instead:

  1. A stateless "planner" stage that quickly expands one input into
     many small, independent sub-items (cheap, fast, one commit).
  2. A "worker" stage that processes each sub-item on its own -- this
     recovers pipelining across sub-items via ordinary inter-item
     concurrency, without needing intra-item streaming at all.
"""

from __future__ import annotations

from revenant.config import StageConfig
from revenant.step import Step


class PlanDocument(Step):
    """Stateless: expands {"document_id": ..., "pages": N} into N
    single-page sub-items in one commit block."""

    def process(self, payload, state):
        doc_id = payload["document_id"]
        for page in range(1, payload["pages"] + 1):
            yield {"document_id": doc_id, "page": page}
        return state


class RenderPage(Step):
    """Processes exactly one page per item.

    This is where an expensive or non-deterministic per-unit step
    would live (e.g. an AI model call) -- it's intentionally isolated
    to one sub-item at a time so that stage-level concurrency (this
    stage working on page N while PlanDocument works on the next
    document) gives you the overlap that intra-item streaming would
    otherwise have been used for.
    """

    def process(self, payload, state):
        # Placeholder for real (possibly non-deterministic, possibly
        # expensive) per-page work.
        rendered = f"<page {payload['page']} of {payload['document_id']}>"
        yield {"document_id": payload["document_id"], "page": payload["page"], "rendered": rendered}
        return state


PIPELINE = [
    StageConfig(name="plan", step_class=PlanDocument, upstream="input"),
    StageConfig(name="render", step_class=RenderPage, upstream="plan"),
]
