import json
import subprocess
import sys

import pytest

from revenant.config import StageConfig, validate_pipeline
from revenant.io_utils import make_checkpoint_line, make_record_line, read_input_final_seq
from revenant.stage_runner import LockHeldError, acquire_lock, run_stage
from revenant.step import SkipItem, Step


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
                json.dumps(make_record_line(seq=1, src_seq=1, parent_seq=1, payload={"value": 1})),
                json.dumps(make_record_line(seq=2, src_seq=2, parent_seq=2, payload={"value": 2})),
                json.dumps(make_checkpoint_line(last_consumed_seq=2, last_emitted_seq=2, state=None)),
            ]
        )
        + "\n"
    )

    run_stage(stage, state_dir)

    checkpoint = json.loads((stage.checkpoint_path(state_dir)).read_text())
    assert checkpoint["last_consumed_seq"] == 2
    assert checkpoint["last_emitted_seq"] == 2


def test_run_stage_deadletters_skipitem_and_writes_no_output_record(tmp_path):
    class SkipStep(Step):
        def process(self, payload, state):
            raise SkipItem()

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    stage = StageConfig(name="A", step_class=SkipStep, upstream="input")
    input_path = stage.upstream_output_path(state_dir)
    input_path.write_text(
        "\n".join(
            [
                json.dumps(make_record_line(seq=1, src_seq=1, parent_seq=1, payload={"value": 1})),
                json.dumps(make_checkpoint_line(last_consumed_seq=1, last_emitted_seq=1, state=None)),
            ]
        )
        + "\n"
    )

    run_stage(stage, state_dir)

    deadletter_lines = [
        json.loads(line) for line in (stage.deadletter_path(state_dir)).read_text().splitlines() if line.strip()
    ]
    output_lines = [json.loads(line) for line in (stage.output_path(state_dir)).read_text().splitlines() if line.strip()]

    assert deadletter_lines[0]["src_seq"] == 1
    assert not any(line.get("type") == "record" and line.get("src_seq") == 1 for line in output_lines)


def test_run_stage_does_not_deadletter_empty_output_without_skipitem(tmp_path):
    class EmptyStep(Step):
        def process(self, payload, state):
            if False:
                yield payload
            return state

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    stage = StageConfig(name="A", step_class=EmptyStep, upstream="input")
    input_path = stage.upstream_output_path(state_dir)
    input_path.write_text(
        "\n".join(
            [
                json.dumps(make_record_line(seq=1, src_seq=1, parent_seq=1, payload={"value": 1})),
                json.dumps(make_checkpoint_line(last_consumed_seq=1, last_emitted_seq=1, state=None)),
            ]
        )
        + "\n"
    )

    run_stage(stage, state_dir)

    assert not stage.deadletter_path(state_dir).exists()


def test_read_input_final_seq_returns_checkpoint_value(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    input_path = state_dir / "input.jsonl"
    input_path.write_text(
        json.dumps(
            {
                "type": "checkpoint",
                "last_consumed_seq": 5,
                "last_emitted_seq": 5,
                "state": None,
            }
        )
        + "\n"
    )

    assert read_input_final_seq(state_dir) == 5


def test_read_input_final_seq_returns_none_when_missing(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    assert read_input_final_seq(state_dir) is None


def test_validate_pipeline_rejects_unknown_upstream_and_duplicate_names():
    class DummyStep(Step):
        def process(self, payload, state):
            yield payload
            return state

    with pytest.raises(ValueError, match="unknown upstream"):
        validate_pipeline(
            [
                StageConfig(name="A", step_class=DummyStep, upstream="input"),
                StageConfig(name="B", step_class=DummyStep, upstream="missing"),
            ]
        )

    with pytest.raises(ValueError, match="Duplicate stage names"):
        validate_pipeline(
            [
                StageConfig(name="A", step_class=DummyStep, upstream="input"),
                StageConfig(name="A", step_class=DummyStep, upstream="A"),
            ]
        )
