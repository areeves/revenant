"""
Seed state/input.jsonl for the blob_split_merge example.

This script creates a tiny input stream for the blob split/merge
pipeline by writing one logical record that points to a sample file.
Run from the repo root:
    python examples/blob_split_merge/make_input.py
"""

from __future__ import annotations

from pathlib import Path

from revenant.io_utils import atomic_append_lines, make_checkpoint_line, make_record_line

LINES = [
    "README.md",
]

def main() -> None:
    state_dir = Path("state")
    state_dir.mkdir(exist_ok=True)
    input_path = state_dir / "input.jsonl"
    if input_path.exists():
        input_path.unlink()

    # Create one logical input record for the sample file.
    records = [
        make_record_line(
            seq=i,
            src_seq=i,
            parent_seq=i,
            payload={"input": text},
        )
        for i, text in enumerate(LINES, start=1)
    ]
    # Treat the generated input as already committed so downstream
    # stages can consume it immediately. The checkpoint line marks the
    # latest visible state for tooling that reads the last committed
    # record (see docs/design.md, section 5.1).
    records.append(make_checkpoint_line(last_consumed_seq=len(LINES), last_emitted_seq=len(LINES)))
    atomic_append_lines(input_path, records)
    print(f"Wrote {len(LINES)} input records to {input_path}")


if __name__ == "__main__":
    main()
