from revenant.config import StageConfig
from revenant.step import SkipItem, RetryableError, Step


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
