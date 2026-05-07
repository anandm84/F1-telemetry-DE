"""
Generate or verify a SHA-256 manifest for the curated sample bundle.

Usage:
    python scripts/checksums.py                # write checksums.sha256
    python scripts/checksums.py --verify       # verify against existing manifest

The manifest covers every file under docs/curation/sample_output/ and the
documentation files in docs/curation/. Paths are relative to the bundle root
so the manifest is portable between machines.
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path

MANIFEST_NAME = "checksums.sha256"


def _iter_target_files(root: Path):
    """Yield (relative_path, absolute_path) for every file in the bundle."""
    sample = root / "docs" / "curation" / "sample_output"
    docs = root / "docs" / "curation"

    if sample.exists():
        for p in sorted(sample.rglob("*")):
            if p.is_file() and p.name != MANIFEST_NAME:
                yield p.relative_to(root), p

    for p in sorted(docs.glob("*")):
        if p.is_file() and p.name != MANIFEST_NAME and p.suffix.lower() in {
            ".md", ".csv", ".xml", ".json", ".mmd", ".png", ".jsonld", ".pdf"
        }:
            yield p.relative_to(root), p


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_manifest(root: Path) -> int:
    manifest_path = root / "docs" / "curation" / MANIFEST_NAME
    lines = []
    for rel, abs_path in _iter_target_files(root):
        lines.append(f"{_sha256(abs_path)}  {rel.as_posix()}")

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[checksums] Wrote {len(lines)} entries to {manifest_path}")
    return 0


def _verify_manifest(root: Path) -> int:
    manifest_path = root / "docs" / "curation" / MANIFEST_NAME
    if not manifest_path.exists():
        print(f"[checksums] manifest not found: {manifest_path}", file=sys.stderr)
        return 2

    failures = 0
    checked = 0
    with manifest_path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line:
                continue
            try:
                expected, rel = line.split(None, 1)
            except ValueError:
                print(f"[checksums] malformed line: {line!r}", file=sys.stderr)
                failures += 1
                continue

            target = root / rel
            if not target.exists():
                print(f"[checksums] MISSING {rel}", file=sys.stderr)
                failures += 1
                continue

            actual = _sha256(target)
            checked += 1
            if actual != expected:
                print(f"[checksums] MISMATCH {rel}\n  expected {expected}\n  actual   {actual}", file=sys.stderr)
                failures += 1

    if failures:
        print(f"[checksums] {failures} failures across {checked} files", file=sys.stderr)
        return 1
    print(f"[checksums] OK — {checked} files verified")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="Repository root (default: cwd).")
    parser.add_argument("--verify", action="store_true", help="Verify existing manifest instead of writing one.")
    args = parser.parse_args()
    root = Path(args.root).resolve()

    if args.verify:
        return _verify_manifest(root)
    return _write_manifest(root)


if __name__ == "__main__":
    raise SystemExit(main())
