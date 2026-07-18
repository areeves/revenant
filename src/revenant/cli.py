"""
Command-line entrypoint. See docs/design.md, section 9.

A user's pipeline is a Python module exposing a `PIPELINE` list of
StageConfig objects (see revenant.config). Point revenant at it with
--pipeline "mypackage.mymodule:PIPELINE", or set the REVENANT_PIPELINE
environment variable to the same dotted path.
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from pathlib import Path
from typing import Sequence

from revenant.config import StageConfig, validate_pipeline


def _load_pipeline(dotted_path: str) -> list[StageConfig]:
    module_path, _, attr = dotted_path.partition(":")
    if not attr:
        raise ValueError(
            f"--pipeline must be of the form 'module.path:ATTR_NAME', got {dotted_path!r}"
        )
    # Console-script entry points (unlike `python -m` or `python script.py`)
    # do not add the current directory to sys.path, so a pipeline module
    # living in the user's own project directory would otherwise never be
    # importable when running the installed `revenant` command from there.
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    module = importlib.import_module(module_path)
    pipeline = list(getattr(module, attr))
    validate_pipeline(pipeline)
    return pipeline


def _resolve_pipeline_arg(args: argparse.Namespace) -> str:
    dotted_path = args.pipeline or os.environ.get("REVENANT_PIPELINE")
    if not dotted_path:
        raise SystemExit(
            "No pipeline specified. Pass --pipeline 'module:ATTR' or set "
            "the REVENANT_PIPELINE environment variable."
        )
    return dotted_path


def cmd_process(args: argparse.Namespace) -> None:
    pipeline_spec = _resolve_pipeline_arg(args)
    pipeline = _load_pipeline(pipeline_spec)
    state_dir = Path(args.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)

    if args.stage:
        from revenant.stage_runner import run_stage

        stage = next((s for s in pipeline if s.name == args.stage), None)
        if stage is None:
            raise SystemExit(f"Unknown stage {args.stage!r}")
        run_stage(stage, state_dir, once=args.once, pipeline=pipeline)
    else:
        from revenant.supervisor import run_supervisor

        run_supervisor(pipeline, state_dir, pipeline_spec)


def cmd_status(args: argparse.Namespace) -> None:
    import json

    state_dir = Path(args.state_dir)
    pipeline = _load_pipeline(_resolve_pipeline_arg(args))

    for stage in pipeline:
        checkpoint_path = stage.checkpoint_path(state_dir)
        if checkpoint_path.exists():
            with open(checkpoint_path) as f:
                cp = json.load(f)
            print(
                f"{stage.name}: consumed={cp['last_consumed_seq']} "
                f"emitted={cp['last_emitted_seq']} items={cp.get('items_processed', '?')}"
            )
        else:
            print(f"{stage.name}: not started")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="revenant")
    parser.add_argument(
        "--pipeline",
        help="Dotted path to your pipeline: 'module.path:ATTR_NAME'. "
        "Falls back to the REVENANT_PIPELINE env var.",
    )
    parser.add_argument(
        "--state-dir",
        default="state",
        help="Directory for all on-disk pipeline state (default: ./state)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    process_parser = subparsers.add_parser("process", help="Run the pipeline (or one stage).")
    process_parser.add_argument(
        "--stage", help="Run only this stage's loop in the foreground, instead of the supervisor."
    )
    process_parser.add_argument(
        "--once", action="store_true", help="With --stage, process exactly one item then exit."
    )
    process_parser.set_defaults(func=cmd_process)

    status_parser = subparsers.add_parser("status", help="Print each stage's progress.")
    status_parser.set_defaults(func=cmd_status)

    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
