#!/usr/bin/env python3
"""Verify checked-in fixture and baseline artifact SHA-256 manifests."""

from __future__ import annotations

import hashlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFESTS = (ROOT / "artifacts" / "SHA256SUMS", ROOT / "tests" / "fixtures" / "SHA256SUMS")


def main() -> int:
    failures: list[str] = []
    checked = 0
    for manifest in MANIFESTS:
        for raw_line in manifest.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            expected, relative = line.split(None, 1)
            path = ROOT / relative
            if not path.is_file():
                failures.append(f"missing: {relative}")
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            checked += 1
            if digest != expected:
                failures.append(f"hash mismatch: {relative}")
    if failures:
        raise SystemExit("provenance verification failed:\n" + "\n".join(failures))
    print(f"provenance ok: verified {checked} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
