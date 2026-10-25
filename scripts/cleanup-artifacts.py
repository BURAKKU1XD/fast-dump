#!/usr/bin/env python3
"""Prepare verified evidence copies and an explicit local artifact deletion plan."""

import argparse
import collections
import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import zipfile


ROOT = Path(__file__).resolve().parents[1]
BUILD_DIRS = {
    "debug", "release", "linux-tui", "linux-tui-msrv", "msrv-tui",
    "perf-tui", "pipeline-status-validation", "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-musl", "zig-cache", "zig-global-cache",
    "zig-local-cache", "__pycache__",
}
MIXED_ZIPS = {"full-dump.zip", "measurements.zip", "core-readers-change.zip"}
EVIDENCE_SUFFIXES = {
    ".csv", ".json", ".md", ".log", ".txt", ".out", ".err", ".stdout",
    ".stderr", ".html", ".svg", ".py", ".ps1", ".sh", ".toml", ".lock",
    ".patch", ".sha256", ".ansi", ".rs", ".yml", ".yaml", ".c",
}


def checked(path):
    path = Path(path).absolute()
    if not path.is_relative_to(ROOT):
        raise ValueError(f"Path outside repository: {path}")
    # Проверяем каждый компонент, чтобы junction не вывел копирование за корень.
    current = ROOT
    for part in path.relative_to(ROOT).parts:
        current /= part
        if current.exists() or current.is_symlink():
            info = current.lstat()
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 1024:
                raise ValueError(f"Reparse point is not allowed: {current}")
    resolved = path.resolve()
    if not resolved.is_relative_to(ROOT):
        raise ValueError(f"Resolved path outside repository: {path}")
    return resolved


def digest(path):
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def files(directory):
    if not directory.exists():
        return
    checked(directory)
    for parent, directories, names in os.walk(directory, followlinks=False):
        for name in directories:
            checked(Path(parent) / name)
        for name in sorted(names):
            yield checked(Path(parent) / name)


def metadata(path):
    before = path.stat()
    sha = digest(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"File changed during hashing: {path}")
    return {
        "path": str(path), "relative_path": path.relative_to(ROOT).as_posix(),
        "bytes": before.st_size, "mtime_ns": before.st_mtime_ns, "sha256": sha,
    }


def write_json(path, value):
    checked(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def copy_verified(source, destination):
    source, destination = checked(source), checked(destination)
    record = metadata(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if digest(destination) != record["sha256"]:
            raise RuntimeError(f"Refusing to overwrite different evidence: {destination}")
    else:
        shutil.copy2(source, destination)
    if digest(destination) != record["sha256"]:
        raise RuntimeError(f"Copy verification failed: {destination}")
    if source.stat().st_mtime_ns != record["mtime_ns"]:
        raise RuntimeError(f"Source changed during copying: {source}")
    record["preserved_path"] = str(destination)
    return record


def successful_dump(path):
    # Прерывание в crash-resume намеренное: смотрим итог возобновления и проверки.
    for name in ("result.json", "verification.json", "report.json"):
        report = path.parent / name
        if not report.is_file():
            continue
        data = json.loads(report.read_text(encoding="utf-8-sig"))
        if not isinstance(data, dict):
            continue
        verified = data.get("verified")
        if data.get("status") == "passed" or data.get("passed") is True:
            return str(report)
        if verified is True or isinstance(verified, str) and verified.startswith("verified "):
            return str(report)
        if data.get("exit_code") == 0 and str(data.get("result", "")).startswith("verified "):
            return str(report)
    return None


def expected_dump_hash(report):
    data = json.loads(Path(report).read_text(encoding="utf-8-sig"))
    if data.get("archive_sha256"):
        return data["archive_sha256"]
    archive = data.get("archive")
    if isinstance(archive, dict) and archive.get("sha256"):
        return archive["sha256"]
    checksum = Path(report).parent / "archive-sha256.json"
    if checksum.is_file():
        return json.loads(checksum.read_text(encoding="utf-8-sig")).get("sha256")
    return None


def extract_evidence(bundle, archive, prepare):
    preserved, checks = [], []
    with zipfile.ZipFile(bundle) as source:
        for entry in source.infolist():
            member = PurePosixPath(entry.filename)
            original_name = entry.orig_filename
            if member.is_absolute() or ".." in member.parts or "\\" in original_name or ":" in original_name:
                raise ValueError(f"Unsafe ZIP member: {bundle}: {entry.filename}")
            if stat.S_ISLNK(entry.external_attr >> 16):
                raise ValueError(f"ZIP symlink is not allowed: {bundle}: {entry.filename}")
            if entry.is_dir():
                continue
            if member.suffix.lower() not in EVIDENCE_SUFFIXES and member.name not in {"stdout", "stderr", "SHA256SUMS", ".gitignore", ".gitattributes"}:
                continue
            loose = checked(bundle.parent.joinpath(*member.parts))
            payload = source.read(entry)
            sha = hashlib.sha256(payload).hexdigest()
            if loose.is_file() and digest(loose) == sha:
                destination = loose
            else:
                # Отдельная ветка не меняет старую историю и сохраняет отличающиеся версии.
                destination = checked(archive / "extracted" / bundle.relative_to(ROOT).with_suffix("") / Path(*member.parts))
                if prepare:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    if destination.exists() and digest(destination) != sha:
                        raise RuntimeError(f"Conflicting extracted evidence: {destination}")
                    if not destination.exists():
                        destination.write_bytes(payload)
                    if digest(destination) != sha:
                        raise RuntimeError(f"Extraction verification failed: {destination}")
                preserved.append({"bundle": str(bundle), "member": entry.filename,
                                  "preserved_path": str(destination), "bytes": len(payload), "sha256": sha})
            checks.append({"member": entry.filename, "preserved_path": str(destination), "sha256": sha})
    return preserved, checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true", help="Copy evidence and write a plan; never delete files")
    parser.add_argument("--archive-date", default=datetime.date.today().isoformat())
    parser.add_argument("--history-date", default="2026-10-24")
    parser.add_argument("--exclude-build", action="append", default=[], choices=sorted(BUILD_DIRS),
                        help="Leave an actively changing build directory out of this plan")
    args = parser.parse_args()
    for value in (args.archive_date, args.history_date):
        datetime.date.fromisoformat(value)
    if args.archive_date == args.history_date:
        raise ValueError("Current and historical archive dates must differ")
    archive = checked(ROOT / "results/history" / args.archive_date)
    history = checked(ROOT / "results/history" / args.history_date)
    output = checked(ROOT / "results/local/imports" / f"cleanup-{args.archive_date}")
    candidates, preserved, bundle_checks = {}, [], []

    def add(path, category, **details):
        relative = path.relative_to(ROOT).as_posix()
        if relative.startswith("target/cleanup"):
            raise ValueError(f"Active cleanup work is protected: {path}")
        candidates[str(path)] = {"category": category, **details}

    def preserve(path, category):
        destination = archive / path.relative_to(ROOT)
        if args.prepare:
            record = copy_verified(path, destination)
        else:
            record = {"path": str(path), "preserved_path": str(destination), "bytes": path.stat().st_size}
        preserved.append(record)
        add(path, category, preserved_path=str(destination))

    # Корни сборки заданы явно; новые каталоги другой задачи не захватываются.
    for name in sorted(BUILD_DIRS):
        if name in args.exclude_build:
            continue
        for path in files(ROOT / "target" / name):
            add(path, "build-cache")
    for path in (ROOT / "target").iterdir():
        if path.name.startswith("cleanup") or path.is_dir():
            continue
        checked(path)
        if path.name == ".rustc_info.json":
            add(path, "build-cache")
        else:
            preserve(path, "archived-current-evidence")
    for path in files(ROOT / "target/stdlib-vendor-repair-20261025-001029429Z"):
        if path.name == "download-manifest.json":
            preserve(path, "archived-current-evidence")
        else:
            add(path, "downloaded-build-dependencies")
    current_dumps = []
    for name in ("local-release-smoke", "linux-windows-archive-verification"):
        for path in files(ROOT / "target" / name):
            if path.name.endswith(".tar.zst"):
                current_dumps.append(path)
            elif "source" in path.relative_to(ROOT / "target" / name).parts:
                add(path, "generated-smoke-fixture")
            elif path.name.endswith(".lock") or path.name == ".fast-dump-open-lock":
                add(path, "generated-smoke-lock")
            else:
                preserve(path, "archived-current-evidence")
    for path in files(ROOT / "releases"):
        if path.name.endswith(".tar.gz"):
            add(path, "release-package")
        else:
            with path.open("rb") as stream:
                magic = stream.read(4)
            if magic.startswith(b"MZ") or magic == b"\x7fELF":
                add(path, "compiled-binary")
            else:
                preserve(path, "archived-release-evidence")

    historical_dumps = []
    for path in files(history):
        if path.name.endswith(".tar.zst"):
            historical_dumps.append(path)
        elif path.name.endswith(".tar.gz"):
            add(path, "release-package")
        elif path.suffix.lower() == ".pdb":
            add(path, "debug-symbols")
        elif path.name in MIXED_ZIPS:
            new, checks = extract_evidence(path, archive, args.prepare)
            preserved.extend(new)
            bundle_checks.append({"path": str(path), "evidence": checks})
            add(path, "mixed-evidence-bundle")
        elif path.suffix.lower() == ".bin" and "compatibility-source" in path.parts:
            add(path, "generated-compatibility-fixture")
        else:
            with path.open("rb") as stream:
                magic = stream.read(4)
            if magic.startswith(b"MZ") or magic == b"\x7fELF":
                add(path, "compiled-binary")

    verified_hashes, unknown = {}, []
    for path in historical_dumps + current_dumps:
        proof = successful_dump(path)
        if proof:
            sha = digest(path)
            expected = expected_dump_hash(proof)
            if expected and expected.lower() != sha:
                raise RuntimeError(f"Generated dump no longer matches its verification: {path}")
            verified_hashes[sha] = proof
            add(path, "verified-generated-dump", verification_path=proof)
        else:
            unknown.append(path)
    for path in unknown:
        sha = digest(path)
        if sha in verified_hashes:
            add(path, "duplicate-verified-generated-dump", verification_path=verified_hashes[sha])
        else:
            print(f"KEEP unverified dump: {path}", flush=True)

    totals = collections.defaultdict(lambda: {"files": 0, "bytes": 0})
    entries = []
    for index, (name, details) in enumerate(sorted(candidates.items()), 1):
        path = checked(Path(name))
        record = metadata(path) if args.prepare else {"path": name, "bytes": path.stat().st_size}
        if args.prepare and "preserved_path" in details and digest(Path(details["preserved_path"])) != record["sha256"]:
            raise RuntimeError(f"Evidence changed after its verified copy: {path}")
        record.update(details)
        entries.append(record)
        totals[details["category"]]["files"] += 1
        totals[details["category"]]["bytes"] += record["bytes"]
        if index % 5000 == 0:
            print(f"Inspected {index}/{len(candidates)} deletion candidates", flush=True)
    summary = {"prepared": args.prepare, "preserved_files": len(preserved),
               "preserved_bytes": sum(row["bytes"] for row in preserved),
               "deletion_files": len(entries), "deletion_bytes": sum(row["bytes"] for row in entries),
               "categories": dict(totals)}
    if args.prepare:
        evidence = {"schema_version": 1, "repository": str(ROOT), "files": preserved, "zip_evidence": bundle_checks}
        evidence_path = output / "preservation-manifest.json"
        write_json(evidence_path, evidence)
        plan = {"schema_version": 1, "repository": str(ROOT),
                "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "history_root": str(history), "archive_root": str(archive),
               "protected_prefixes": ["target/cleanup"], "build_directories": sorted(BUILD_DIRS),
                "excluded_build_directories": args.exclude_build,
                "preservation_manifest": str(evidence_path), "preservation_manifest_sha256": digest(evidence_path),
                "entries": entries, "summary": summary}
        write_json(output / "deletion-plan.json", plan)
        write_json(output / "summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
