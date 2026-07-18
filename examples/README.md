# Examples

Two reference pipelines, written against the `Step` / `StageConfig` API
described in `docs/design.md`.

- **`wordcount/`** — a stateless fan-out stage (`SplitWords`, one input
  line -> many word records) feeding a stateful accumulator stage
  (`CountWords`, a running `{word: count}` table, checkpointed after
  every item). Good first reference for the basic shapes: `process()`
  as a generator, and `load()`/`checkpoint()` for carrying state
  across items.

- **`planner_worker/`** — demonstrates the planner/worker split
  pattern from `docs/design.md` section 6: a stateless "planner" stage
  expands one input into many independent sub-items in a single
  commit, and a "worker" stage processes each sub-item on its own.
  Use this shape whenever a step's natural form is "one input produces
  many expensive, independent units of work" and you want those units
  pipelined against each other, since a single stage can't stream
  partial results downstream mid-item.

## Running an example

```bash
cd revenant
pip install -e .
python examples/wordcount/make_input.py     # seeds state/input.jsonl
revenant --pipeline examples.wordcount.pipeline:PIPELINE process
revenant --pipeline examples.wordcount.pipeline:PIPELINE status
```

The stage runner is fully implemented and the examples are runnable
end-to-end through the CLI. The `Step` classes above can also be
exercised directly without the runner, e.g.:

```python
from examples.wordcount.pipeline import SplitWords

step = SplitWords()
state = step.load(None)
print(list(step.process({"text": "hello world"}, state)))
# [{'word': 'hello'}, {'word': 'world'}]
```
