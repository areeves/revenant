# revenant

A crash-safe, resumable, file-based pipeline framework for Python.

Runs a chain of stages over a batch of input items, where each stage can
emit zero, one, or many outputs per input, stages run concurrently as
separate processes, and all progress is durably checkpointed to plain
JSON files on disk -- so a crash or restart only reprocesses the item
that was interrupted, not the whole run.

See [`docs/design.md`](docs/design.md) for the full design rationale.
The stage runner is fully implemented and runnable end-to-end through
the CLI, so the examples can be exercised directly from the repository.

## Install

Directly from GitHub, no PyPI publish required:

```bash
pip install git+https://github.com/TODO-replace-with-your-username/revenant.git
```

Or for local development (editable install, so code changes take effect
immediately):

```bash
git clone https://github.com/TODO-replace-with-your-username/revenant.git
cd revenant
pip install -e ".[dev]"
```

## Examples

See [`examples/`](examples/) for two reference pipelines (a stateful
word-count pipeline, and a planner/worker split pipeline) with
runnable `make_input.py` seed scripts.

## Quick start

Define your pipeline as a list of `StageConfig` objects:

```python
# my_pipeline.py
from revenant.config import StageConfig
from revenant.step import Step

class StepA(Step):
    def process(self, payload, state):
        yield {"doubled": payload["n"] * 2}
        return state

PIPELINE = [
    StageConfig(name="A", step_class=StepA, upstream="input"),
]
```

Seed `state/input.jsonl` with your input items (one JSON object per
line, wrapped as `{"type": "record", "seq": 1, "payload": {...}}`,
`seq` starting at 1 and increasing), then run:

```bash
revenant --pipeline my_pipeline:PIPELINE process
```

Or run/test a single stage directly:

```bash
revenant --pipeline my_pipeline:PIPELINE process --stage A --once
```

Check progress at any time:

```bash
revenant --pipeline my_pipeline:PIPELINE status
```

## Development

```bash
pip install -e ".[dev]"
pytest
```
