import json

from revenant.config import StageConfig
from revenant.io_utils import make_checkpoint_line, make_record_line
from revenant.stage_runner import run_stage

from examples.blob_split_merge.pipeline import SplitFile, MergeFile


def _write_input_jsonl(path, payloads):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(make_record_line(seq=i, src_seq=i, parent_seq=i, payload=p))
        for i, p in enumerate(payloads, start=1)
    ]
    lines.append(json.dumps(make_checkpoint_line(last_consumed_seq=len(payloads), last_emitted_seq=len(payloads))))
    path.write_text("\n".join(lines) + "\n")


def _run_split_then_merge(tmp_path, source_file):
    state_dir = tmp_path / "state"
    state_dir.mkdir()

    split_stage = StageConfig(name="split", step_class=SplitFile, upstream="input")
    merge_stage = StageConfig(name="merge", step_class=MergeFile, upstream="split")
    pipeline = [split_stage, merge_stage]

    _write_input_jsonl(split_stage.upstream_output_path(state_dir), [{"input": str(source_file)}])

    run_stage(split_stage, state_dir, pipeline=pipeline)
    run_stage(merge_stage, state_dir, pipeline=pipeline)

    output_path = merge_stage.output_path(state_dir)
    if not output_path.exists():
        return state_dir, []

    output_lines = [
        json.loads(line)
        for line in output_path.read_text().splitlines()
        if line.strip()
    ]
    return state_dir, [line for line in output_lines if line.get("type") == "record"]


def test_empty_file_produces_no_merge_output(tmp_path):
    source_file = tmp_path / "empty.txt"
    source_file.write_text("")

    state_dir, records = _run_split_then_merge(tmp_path, source_file)

    # SplitFile has no lines to yield for an empty file, so nothing ever
    # reaches MergeFile for this input -- zero parts is not the same as
    # "one part with total=0", and no output record should be produced.
    assert records == []


def test_single_line_file_merges_to_one_result(tmp_path):
    source_file = tmp_path / "single_line.txt"
    source_file.write_text("only one line\n")

    state_dir, records = _run_split_then_merge(tmp_path, source_file)

    assert len(records) == 1
    resolved = state_dir / records[0]["payload"]["result"]
    assert resolved.read_text() == "only one line\n"


def test_multi_line_file_merges_all_parts_in_order(tmp_path):
    source_file = tmp_path / "multi_line.txt"
    source_file.write_text("line one\nline two\nline three\n")

    state_dir, records = _run_split_then_merge(tmp_path, source_file)

    assert len(records) == 1
    resolved = state_dir / records[0]["payload"]["result"]
    assert resolved.read_text() == "line one\nline two\nline three\n"
