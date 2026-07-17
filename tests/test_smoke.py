import json
import subprocess
import sys

import pytest

from revenant.config import StageConfig
from revenant.stage_runner import LockHeldError, acquire_lock, run_stage
from revenant.step import Step


def test_package_imports():
    import revenant

    assert revenant.__version__


def test_stage_config_paths(tmp_path):
    class DummyStep(Step):
        def process(self, payload, state):
            yield payload
            return state

    stage = StageConfig(name="A", step_class=DummyStep, upstream="input")
    assert stage.output_path(tmp_path) == tmp_path / "A.jsonl"
    assert stage.upstream_output_path(tmp_path) == tmp_path / "input.jsonl"


def test_acquire_lock_rejects_live_pid(tmp_path):
    lock_path = tmp_path / "A.lock"
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        lock_path.write_text(json.dumps({"pid": proc.pid}))
        with pytest.raises(LockHeldError):
            acquire_lock(lock_path)
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_run_stage_drains_input_and_writes_checkpoint(tmp_path):
    class EchoStep(Step):
        def process(self, payload, state):
            yield payload
            return state

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    stage = StageConfig(name="A", step_class=EchoStep, upstream="input")
    input_path = stage.upstream_output_path(state_dir)
    input_path.write_text(
        "\n".join(
            [
                json.dumps({"type": "record", "seq": 1, "src_seq": 1, "payload": {"value": 1}}),
                json.dumps({"type": "record", "seq": 2, "src_seq": 2, "payload": {"value": 2}}),
                json.dumps({"type": "checkpoint", "src_seq": 2, "last_emitted_seq": 2, "state": None}),
            ]
        )
        + "\n"
    )

    run_stage(stage, state_dir, input_final_seq=2)

    checkpoint = json.loads((stage.checkpoint_path(state_dir)).read_text())
    assert checkpoint["last_consumed_seq"] == 2
    assert checkpoint["last_emitted_seq"] == 2
