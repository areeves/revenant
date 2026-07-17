"""
Seed state/input.jsonl for the wordcount example.

Run from the repo root:
    python examples/wordcount/make_input.py
"""

from __future__ import annotations

from pathlib import Path

from revenant.io_utils import atomic_append_lines

LINES = [
    "the quick brown fox jumps over the lazy dog",
    "the dog barks at the fox",
    "quick quick quick",
]


def main() -> None:
    state_dir = Path("state")
    state_dir.mkdir(exist_ok=True)
    input_path = state_dir / "input.jsonl"
    if input_path.exists():
        input_path.unlink()

    records = [
        {
            "type": "record",
            "seq": i,
            "src_seq": None,
            "parent_seq": i,
            "emitted_at": None,
            "payload": {"text": text},
        }
        for i, text in enumerate(LINES, start=1)
    ]
    # input.jsonl is treated as already-committed; append a checkpoint
    # line so tooling that reads "last committed record" sees it all
    # as available immediately (see docs/design.md, section 5.1).
    records.append(
        {
            "type": "checkpoint",
            "src_seq": len(LINES),
            "last_emitted_seq": len(LINES),
            "state": None,
            "committed_at": None,
        }
    )
    atomic_append_lines(input_path, records)
    print(f"Wrote {len(LINES)} input records to {input_path}")


if __name__ == "__main__":
    main()
