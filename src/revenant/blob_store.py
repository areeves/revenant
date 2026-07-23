"""
Content-addressed blob storage for stage outputs that shouldn't be
embedded directly in .jsonl payloads (e.g. images, media files). See
docs/design-blob-storage.md for the full rationale.

One BlobStore per stage process, constructed once at stage-runner
startup and attached to the Step instance as `step.blob_store`.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path


class BlobStore:
    def __init__(self, blobs_dir: Path, state_dir: Path):
        self.blobs_dir = blobs_dir
        self.state_dir = state_dir
        self.scratch_dir = blobs_dir / ".scratch"
        self.blobs_dir.mkdir(parents=True, exist_ok=True)
        self.scratch_dir.mkdir(parents=True, exist_ok=True)

    def reserve_path(self, suffix: str = "") -> Path:
        """Return a scratch path an external tool can write to directly."""
        return self.scratch_dir / f"{uuid.uuid4().hex}{suffix}"

    def reserve_dir(self) -> Path:
        """Same idea, for tools that produce many output files at once
        (e.g. ffmpeg segmenting one input into N chunk files)."""
        d = self.scratch_dir / uuid.uuid4().hex
        d.mkdir(parents=True, exist_ok=True)
        return d

    def write(self, data: bytes, suffix: str = "") -> str:
        """Convenience wrapper: write in-memory bytes and commit in one call."""
        scratch_path = self.reserve_path(suffix=suffix)
        fd = os.open(scratch_path, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        return self.commit(scratch_path, suffix=suffix)

    def commit(self, scratch_path: Path, suffix: str | None = None) -> str:
        """Hash a completed scratch file and move it into content-addressed
        storage. Returns a path relative to state_dir, suitable for storing
        directly in a payload dict.

        `scratch_path` must already be fully written and fsync'd before
        calling this -- commit() does not fsync the source file itself,
        only the containing-directory fsync implied by os.replace on most
        Linux filesystems. Callers writing via write() get this for free;
        callers using reserve_path()/reserve_dir() directly (e.g. an
        external subprocess) are responsible for the tool having finished
        and flushed before commit() is called.
        """
        digest = _sha256_file(scratch_path)
        actual_suffix = suffix if suffix is not None else scratch_path.suffix
        shard = digest[:2]
        name = f"{digest}{actual_suffix}"
        target_dir = self.blobs_dir / shard
        target_dir.mkdir(parents=True, exist_ok=True)
        final_path = target_dir / name

        if final_path.exists():
            # Identical content already stored (e.g. a deterministic retry
            # after a crash) -- discard the duplicate scratch file.
            scratch_path.unlink(missing_ok=True)
        else:
            os.replace(scratch_path, final_path)  # atomic: same filesystem

        return str(final_path.relative_to(self.state_dir))

    def resolve(self, relative_path: str) -> Path:
        """Turn a payload's stored relative path back into a real path."""
        return self.state_dir / relative_path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
