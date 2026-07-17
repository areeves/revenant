"""
Supervisor: spawns one child process per stage, monitors and restarts
them, and detects whole-pipeline completion. See docs/design.md,
section 10.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence

from revenant.config import StageConfig
from revenant.io_utils import atomic_write_json

POLL_INTERVAL_SECONDS = 1.0


def spawn_stage(stage_name: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "revenant.cli", "process", "--stage", stage_name]
    )


def run_supervisor(pipeline: Sequence[StageConfig], state_dir: Path) -> None:
    procs: dict[str, subprocess.Popen] = {
        stage.name: spawn_stage(stage.name) for stage in pipeline
    }
    done: set[str] = set()

    try:
        while len(done) < len(pipeline):
            for stage in pipeline:
                if stage.name in done:
                    continue
                proc = procs[stage.name]
                ret = proc.poll()
                if ret is None:
                    continue  # still running
                if ret == 0:
                    # TODO: confirm this was a *clean* drain (upstream
                    # durably done) rather than just an early exit, per
                    # docs/design.md section 7, before marking done.
                    done.add(stage.name)
                else:
                    # Unexpected exit -- respawn. The stage resumes from
                    # its last commit; the supervisor doesn't need to
                    # know why it died.
                    procs[stage.name] = spawn_stage(stage.name)

            # TODO: write pipeline.status.json here from each stage's
            # checkpoint cache (docs/design.md, section 5.3).
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        for proc in procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in procs.values():
            if proc.poll() is None:
                proc.wait(timeout=30)

    atomic_write_json(state_dir / "pipeline.status.json", {"state": "done"})
