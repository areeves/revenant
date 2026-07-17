"""
Seed state/input.jsonl for the planner_worker example.

Run from the repo root:
    python examples/planner_worker/make_input.py
"""

from __future__ import annotations

from pathlib import Path

from revenant.io_utils import atomic_append_lines

DOCUMENTS = [
    {"document_id": "doc-a", "pages": 3},
    {"document_id": "doc-b", "pages": 2},
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
            "payload": doc,
        }
        for i, doc in enumerate(DOCUMENTS, start=1)
    ]
    records.append(
        {
            "type": "checkpoint",
            "src_seq": len(DOCUMENTS),
            "last_emitted_seq": len(DOCUMENTS),
            "state": None,
            "committed_at": None,
        }
    )
    atomic_append_lines(input_path, records)
    print(f"Wrote {len(DOCUMENTS)} input records to {input_path}")


if __name__ == "__main__":
    main()
