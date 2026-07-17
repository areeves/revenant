"""
Generic stage-runner loop. See docs/design.md, sections 5-8.

This is a scaffold: locking, checkpoint-loading, and the commit-write
path are wired up; the polling loop's TODOs are where item-by-item
logic (calling into the Step, handling RetryableError/SkipItem, seq
bookkeeping) still needs to be filled in.
"""

from __future__ import annotations

import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from revenant.config import StageConfig
from revenant.io_utils import (
    atomic_append_lines,
    atomic_write_json,
    iter_records_after,
    read_input_final_seq,
    read_last_checkpoint_line,
)
from revenant.step import RetryableError, SkipItem

POLL_INTERVAL_SECONDS = 1.0


class LockHeldError(RuntimeError):
    """Raised when a stage's lock file is held by another live process."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def acquire_lock(lock_path: Path) -> None:
    if lock_path.exists():
        try:
            existing = lock_path.read_text()
        except OSError:
            existing = ""
        if existing:
            import json

            try:
                parsed = json.loads(existing)
            except json.JSONDecodeError:
                parsed = {}
            pid = parsed.get("pid")
            if pid is not None:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    raise LockHeldError(f"Lock held by pid {pid}") from None
                else:
                    raise LockHeldError(f"Lock held by pid {pid}") from None
    atomic_write_json(
        lock_path,
        {"pid": os.getpid(), "hostname": socket.gethostname(), "started_at": _now_iso()},
    )


def release_lock(lock_path: Path) -> None:
    lock_path.unlink(missing_ok=True)


def load_resume_point(stage: StageConfig, state_dir: Path) -> tuple[int, int, object]:
    """Return (last_consumed_seq, last_emitted_seq, state) to resume from.

    Prefers the checkpoint cache file; falls back to scanning the
    stage's own output file for the last checkpoint line if the cache
    is missing (docs/design.md, section 5.2).
    """
    checkpoint_path = stage.checkpoint_path(state_dir)
    if checkpoint_path.exists():
        import json

        with open(checkpoint_path) as f:
            cached = json.load(f)
        return cached["last_consumed_seq"], cached["last_emitted_seq"], cached["state"]

    last = read_last_checkpoint_line(stage.output_path(state_dir))
    if last is None:
        return 0, 0, None
    return last["src_seq"], last["last_emitted_seq"], last["state"]


def is_upstream_durably_done(
    stage: StageConfig,
    state_dir: Path,
    input_final_seq: int | None,
    pipeline: Sequence[StageConfig] | None = None,
) -> bool:
    """Recursive drain check. See docs/design.md, section 7."""
    my_consumed, _, _ = load_resume_point(stage, state_dir)
    if stage.upstream == "input":
        final_seq = input_final_seq if input_final_seq is not None else 0
        return my_consumed >= final_seq

    if pipeline is None:
        raise ValueError("pipeline is required to resolve upstream drain state")

    upstream_stage = next((candidate for candidate in pipeline if candidate.name == stage.upstream), None)
    if upstream_stage is None:
        raise ValueError(f"Unknown upstream stage {stage.upstream!r} for {stage.name!r}")

    upstream_done = is_upstream_durably_done(upstream_stage, state_dir, input_final_seq, pipeline)
    _, upstream_last_emitted, _ = load_resume_point(upstream_stage, state_dir)
    return upstream_done and my_consumed >= upstream_last_emitted


def run_stage(
    stage: StageConfig,
    state_dir: Path,
    once: bool = False,
    pipeline: Sequence[StageConfig] | None = None,
) -> None:
    """Run one stage's processing loop until its upstream is durably done.

    If `once` is True, process at most a single available item and
    return instead of looping.
    """
    lock_path = stage.lock_path(state_dir)
    acquire_lock(lock_path)
    try:
        last_consumed_seq, last_emitted_seq, saved_state = load_resume_point(stage, state_dir)

        step = stage.step_class()
        state = step.load(saved_state)

        input_final_seq = read_input_final_seq(state_dir)

        while True:
            next_item = None
            for record in iter_records_after(stage.upstream_output_path(state_dir), last_consumed_seq):
                next_item = record
                break

            if next_item is None:
                upstream_done = is_upstream_durably_done(
                    stage,
                    state_dir,
                    input_final_seq,
                    pipeline,
                )
                if upstream_done or once:
                    return
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            outputs = []
            gen = step.process(next_item["payload"], state)
            try:
                while True:
                    outputs.append(next(gen))
            except StopIteration as stop:
                new_state = stop.value
            except RetryableError:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            except SkipItem:
                new_state = state
                outputs = []

            # Build and write the atomic commit block: buffered output
            # records + one checkpoint line (docs/design.md, section 6).
            lines = []
            seq = last_emitted_seq
            for payload in outputs:
                seq += 1
                lines.append(
                    {
                        "type": "record",
                        "seq": seq,
                        "src_seq": next_item["seq"],
                        "parent_seq": next_item.get("parent_seq", next_item["seq"]),
                        "emitted_at": _now_iso(),
                        "payload": payload,
                    }
                )
            checkpoint_state = step.checkpoint(new_state)
            lines.append(
                {
                    "type": "checkpoint",
                    "src_seq": next_item["seq"],
                    "last_emitted_seq": seq,
                    "state": checkpoint_state,
                    "committed_at": _now_iso(),
                }
            )
            atomic_append_lines(stage.output_path(state_dir), lines)

            if isinstance(outputs, list) and not outputs and new_state is state and next_item is not None:
                atomic_append_lines(
                    stage.deadletter_path(state_dir),
                    [{"type": "deadletter", "src_seq": next_item["seq"], "payload": next_item["payload"]}],
                )

            last_consumed_seq = next_item["seq"]
            last_emitted_seq = seq
            state = new_state

            atomic_write_json(
                stage.checkpoint_path(state_dir),
                {
                    "stage": stage.name,
                    "last_consumed_seq": last_consumed_seq,
                    "last_emitted_seq": last_emitted_seq,
                    "state": checkpoint_state,
                    "updated_at": _now_iso(),
                    "schema_version": 1,
                },
            )

            if once:
                return
    finally:
        release_lock(lock_path)
