"""
Low-level durable file I/O primitives. See docs/design.md, section 6,
for why these atomicity properties matter.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable


def atomic_append_lines(path: Path, lines: Iterable[dict]) -> None:
    """Append JSON lines to `path` as a single atomic write.

    Used to write a full commit block (records + checkpoint line) in
    one write() call, so a reader never observes a partial commit.
    """
    text = "".join(json.dumps(line, separators=(",", ":")) + "\n" for line in lines)
    if not text:
        return
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, text.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_json(path: Path, data: Any) -> None:
    """Write `data` as JSON to `path` via write-temp-then-rename.

    Used for the checkpoint cache and lock files (docs/design.md,
    sections 5.2 and 5.4) -- these are convenience caches, never the
    source of truth, but should still never be observed half-written.
    """
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def read_last_checkpoint_line(path: Path) -> dict | None:
    """Scan `path` backward for the last `{"type": "checkpoint", ...}` line.

    Fallback used when the checkpoint cache file is missing, stale, or
    looks inconsistent with the actual output file -- the output file
    is always the source of truth (docs/design.md, section 5.1).
    """
    if not path.exists():
        return None
    last_checkpoint = None
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("type") == "checkpoint":
                last_checkpoint = record
    return last_checkpoint


def read_input_final_seq(state_dir: Path) -> int | None:
    """Return the total number of items ever written to input.jsonl.

    Input is treated as a fixed, fully-assembled batch before the
    pipeline starts (docs/design.md, section 1), so this value is
    stable for the life of a run and safe to read independently by any
    stage process without coordination.
    """
    last_checkpoint = read_last_checkpoint_line(state_dir / "input.jsonl")
    if last_checkpoint is None:
        return None
    return last_checkpoint.get("last_emitted_seq")


def iter_records_after(path: Path, after_seq: int) -> Iterable[dict]:
    """Yield committed `type: record` lines with seq > after_seq.

    "Committed" means: only records that appear before (or as part of)
    a checkpoint line are ever considered -- an uncommitted trailing
    record (from an in-progress or crashed attempt) is never yielded.
    """
    if not path.exists():
        return
    pending: list[dict] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record["type"] == "record":
                pending.append(record)
            elif record["type"] == "checkpoint":
                for rec in pending:
                    if rec["seq"] > after_seq:
                        yield rec
                pending = []
