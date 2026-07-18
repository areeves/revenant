"""
Declarative pipeline wiring. See docs/design.md, section 4.

Example:

    from revenant.config import StageConfig
    from my_steps import StepA, StepB, StepC

    PIPELINE = [
        StageConfig(name="A", step_class=StepA, upstream="input"),
        StageConfig(name="B", step_class=StepB, upstream="A"),
        StageConfig(name="C", step_class=StepC, upstream="B"),
    ]
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Type

from revenant.step import Step


def validate_pipeline(pipeline: list[StageConfig]) -> None:
    """Validate pipeline wiring before the stage runner uses it."""
    names = [stage.name for stage in pipeline]
    if len(names) != len(set(names)):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        raise ValueError(f"Duplicate stage names: {', '.join(duplicates)}")

    known_names = set()
    for index, stage in enumerate(pipeline):
        if stage.upstream == "input":
            known_names.add(stage.name)
            continue

        if stage.upstream in known_names:
            known_names.add(stage.name)
            continue

        if stage.upstream in names[:index]:
            raise ValueError(
                f"Stage {stage.name!r} references upstream {stage.upstream!r} that appears later in the pipeline"
            )

        raise ValueError(f"Stage {stage.name!r} references unknown upstream {stage.upstream!r}")


@dataclass(frozen=True)
class StageConfig:
    name: str
    step_class: Type[Step]
    upstream: str  # another stage's `name`, or "input"

    def output_path(self, state_dir: Path) -> Path:
        return state_dir / f"{self.name}.jsonl"

    def checkpoint_path(self, state_dir: Path) -> Path:
        return state_dir / f"{self.name}.checkpoint.json"

    def lock_path(self, state_dir: Path) -> Path:
        return state_dir / f"{self.name}.lock"

    def deadletter_path(self, state_dir: Path) -> Path:
        return state_dir / f"{self.name}.deadletter.jsonl"

    def upstream_output_path(self, state_dir: Path) -> Path:
        if self.upstream == "input":
            return state_dir / "input.jsonl"
        return state_dir / f"{self.upstream}.jsonl"
