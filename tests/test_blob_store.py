import json

from revenant.blob_store import BlobStore
from revenant.config import StageConfig
from revenant.io_utils import make_checkpoint_line, make_record_line
from revenant.stage_runner import run_stage
from revenant.step import Step


def test_write_round_trips_bytes(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    store = BlobStore(state_dir / "A.blobs", state_dir)

    relative_path = store.write(b"hello world", suffix=".txt")

    resolved = store.resolve(relative_path)
    assert resolved.exists()
    assert resolved.read_bytes() == b"hello world"
    # Path returned must be relative to state_dir, as specified.
    assert resolved == (state_dir / relative_path)
    assert not relative_path.startswith("/")
    assert resolved.is_absolute()


def test_identical_writes_dedup_to_same_path(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    store = BlobStore(state_dir / "A.blobs", state_dir)

    first_path = store.write(b"deterministic payload", suffix=".bin")
    second_path = store.write(b"deterministic payload", suffix=".bin")

    assert first_path == second_path
    resolved = store.resolve(first_path)
    assert resolved.exists()
    assert resolved.read_bytes() == b"deterministic payload"

    # Only one file should exist under the content-addressed shard --
    # the second write must have discarded its scratch copy, not created
    # a duplicate blob.
    shard_dir = resolved.parent
    assert len(list(shard_dir.iterdir())) == 1


def test_reserve_dir_and_commit_multi_output(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    store = BlobStore(state_dir / "A.blobs", state_dir)

    out_dir = store.reserve_dir()
    assert out_dir.exists()
    assert out_dir.is_dir()

    file_a = out_dir / "chunk_0001.ts"
    file_b = out_dir / "chunk_0002.ts"
    file_a.write_bytes(b"chunk one")
    file_b.write_bytes(b"chunk two")

    path_a = store.commit(file_a)
    path_b = store.commit(file_b)

    assert path_a != path_b
    assert store.resolve(path_a).read_bytes() == b"chunk one"
    assert store.resolve(path_b).read_bytes() == b"chunk two"
    # Scratch files should have been moved out, not copied.
    assert not file_a.exists()
    assert not file_b.exists()


def test_run_stage_persists_blob_path_in_committed_record(tmp_path):
    class WriteBlobStep(Step):
        def process(self, payload, state):
            path = self.blob_store.write(bytes(payload["text"], "utf8"), suffix=".txt")
            yield {"blob_path": path}
            return state

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    stage = StageConfig(name="A", step_class=WriteBlobStep, upstream="input")

    input_path = stage.upstream_output_path(state_dir)
    input_path.write_text(
        "\n".join(
            [
                json.dumps(make_record_line(seq=1, src_seq=1, parent_seq=1, payload={"text": "hello"})),
                json.dumps(make_checkpoint_line(last_consumed_seq=1, last_emitted_seq=1, state=None)),
            ]
        )
        + "\n"
    )

    run_stage(stage, state_dir)

    output_lines = [
        json.loads(line) for line in stage.output_path(state_dir).read_text().splitlines() if line.strip()
    ]
    record_lines = [line for line in output_lines if line.get("type") == "record"]
    assert len(record_lines) == 1

    blob_path = record_lines[0]["payload"]["blob_path"]
    assert blob_path.startswith("A.blobs/")

    resolved = state_dir / blob_path
    assert resolved.exists()
    assert resolved.read_text() == "hello"
