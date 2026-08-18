#!/usr/bin/env python3
"""Redacted secret/public-data scan for HEAD or all reachable git history.

The scanner prints only file/revision, line, and finding kind. It never emits
the matched value. It is intentionally a conservative gate for accidental
commits, not a replacement for a provider-specific secret scanner.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PATTERNS = (
    ("private_key", re.compile(r"-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----")),
    ("provider_token", re.compile(r"\b(?:gh[pousr]_\w{12,}|github_pat_\w{12,}|glpat[-_]\w{12,}|sk-[A-Za-z0-9_-]{16,})\b")),
    ("cloud_key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("private_path", re.compile(r"/(?:Users|home)/[A-Za-z0-9_.-]+")),
    # Deliberately uppercase-only: lowercase Python parameter names such as
    # ``secret=`` are test fixtures, not configuration leaks.
    ("secret_assignment", re.compile(r"\b(?:SECRET|PASSWORD|ACCESS_TOKEN|AUTH_TOKEN|API_KEY)\s*[:=]\s*['\"]?([^'\"\s]{8,})")),
    ("email", re.compile(r"\b[\w.+-]{2,}@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
)
PLACEHOLDERS = {
    "replace-with-a-long-random-secret",
    "replace-me",
    "changeme",
    "placeholder",
    "example",
}


def _findings(text: str) -> set[str]:
    found: set[str] = set()
    for kind, pattern in PATTERNS:
        for match in pattern.finditer(text):
            value = match.group(1) if kind == "secret_assignment" and match.lastindex else match.group(0)
            if kind == "secret_assignment" and value.lower().strip("'\"") in PLACEHOLDERS:
                continue
            if kind == "email" and value.lower() in {"operator@example.com", "noreply@example.com"}:
                continue
            found.add(kind)
    return found


def _scan_file(label: str, content: bytes) -> list[str]:
    if b"\x00" in content[:4096]:
        return []
    findings: list[str] = []
    text = content.decode("utf-8", errors="ignore")
    for number, line in enumerate(text.splitlines(), 1):
        for kind in sorted(_findings(line)):
            findings.append(f"{label}:{number}:{kind}")
    return findings


def _tracked_head() -> list[tuple[str, bytes]]:
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).split(b"\0")
    return [(str(Path(path.decode())), (ROOT / path.decode()).read_bytes()) for path in paths if path]


def _reachable_history() -> list[tuple[str, bytes]]:
    revisions = subprocess.check_output(["git", "rev-list", "--all"], cwd=ROOT, text=True).splitlines()
    findings: list[tuple[str, bytes]] = []
    for revision in revisions:
        paths = subprocess.check_output(
            ["git", "ls-tree", "-r", "--name-only", revision], cwd=ROOT, text=True
        ).splitlines()
        for path in paths:
            try:
                content = subprocess.check_output(["git", "show", f"{revision}:{path}"], cwd=ROOT)
            except subprocess.CalledProcessError:
                continue
            findings.append((f"{revision[:12]}:{path}", content))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", action="store_true", help="scan all reachable revisions")
    args = parser.parse_args()
    entries = _reachable_history() if args.history else _tracked_head()
    findings = [finding for label, content in entries for finding in _scan_file(label, content)]
    if findings:
        raise SystemExit("secret/public-data scan failed (redacted):\n" + "\n".join(sorted(set(findings))))
    print(f"secret/public-data scan ok (redacted): scanned {len(entries)} file snapshots")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
