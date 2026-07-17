"""
Supervisor: spawns one child process per stage, monitors and restarts
them, and detects whole-pipeline completion. See docs/design.md,
section 10.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import json
from pathlib import Path
from typing import Sequence

from revenant.config import StageConfig
from revenant.io_utils import atomic_write_json, read_last_checkpoint_line
from revenant.stage_runner import is_upstream_durably_done, load_resume_point

POLL_INTERVAL_SECONDS = 1.0


def spawn_stage(stage_name: str, pipeline_spec: str, state_dir: Path) -> subprocess.Popen:
    env = os.environ.copy()
    env["REVENANT_PIPELINE"] = pipeline_spec
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "revenant.cli",
            "--state-dir",
            str(state_dir),
            "process",
            "--stage",
            stage_name,
        ],
        env=env,
    )


def run_supervisor(pipeline: Sequence[StageConfig], state_dir: Path, pipeline_spec: str) -> None:
    procs: dict[str, subprocess.Popen] = {
        stage.name: spawn_stage(stage.name, pipeline_spec, state_dir) for stage in pipeline
    }
    done: set[str] = set()
    input_final_seq = None
    input_path = state_dir / "input.jsonl"
    if input_path.exists():
        last_checkpoint = read_last_checkpoint_line(input_path)
        if last_checkpoint is not None:
            input_final_seq = last_checkpoint.get("last_emitted_seq")

    try:
        while len(done) < len(pipeline):
            for stage in pipeline:
                if stage.name in done:
                    continue
                proc = procs[stage.name]
                ret = proc.poll()
                if ret is None:
                    continue
                if ret == 0:
                    stage_done = is_upstream_durably_done(stage, state_dir, input_final_seq, pipeline)
                    if stage_done:
                        done.add(stage.name)
                    else:
                        procs[stage.name] = spawn_stage(stage.name, pipeline_spec, state_dir)
                else:
                    procs[stage.name] = spawn_stage(stage.name, pipeline_spec, state_dir)

            status = {
                "state": "running",
                "stages": {},
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            for stage in pipeline:
                last_consumed_seq, last_emitted_seq, _ = load_resume_point(stage, state_dir)
                status["stages"][stage.name] = {
                    "pid": procs[stage.name].pid,
                    "last_consumed_seq": last_consumed_seq,
                    "last_emitted_seq": last_emitted_seq,
                    "durably_done": stage.name in done,
                }
            atomic_write_json(state_dir / "pipeline.status.json", status)
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            if proc.poll() is None:
                proc.wait(timeout=30)

    atomic_write_json(
        state_dir / "pipeline.status.json",
        {
            "state": "done",
            "stages": {
                stage.name: {
                    "pid": None,
                    "last_consumed_seq": load_resume_point(stage, state_dir)[0],
                    "last_emitted_seq": load_resume_point(stage, state_dir)[1],
                    "durably_done": True,
                }
                for stage in pipeline
            },
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    )
