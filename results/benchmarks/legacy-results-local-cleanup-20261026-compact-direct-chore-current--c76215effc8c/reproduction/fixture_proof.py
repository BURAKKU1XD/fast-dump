"""Immutable fixture identity proof; reads fixture contents without modifying them."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
from datetime import datetime, timezone


SCHEMA = 1
DOMAIN = b"fast-dump-fixture-proof-v1\0"
CHUNK = 1024 * 1024


def signature(info):
    # atime исключён: само чтение может обновить его без изменения fixture.
    names = (
        "st_dev", "st_ino", "st_mode", "st_nlink", "st_size",
        "st_mtime_ns", "st_ctime_ns", "st_birthtime_ns",
        "st_file_attributes", "st_reparse_tag",
    )
    return {name: getattr(info, name) for name in names if hasattr(info, name)}


def kind(info):
    if stat.S_ISLNK(info.st_mode):
        return "symlink"
    if getattr(info, "st_reparse_tag", 0) == 0xA0000003:
        return "junction"
    if stat.S_ISDIR(info.st_mode):
        return "directory"
    if stat.S_ISREG(info.st_mode):
        return "regular"
    raise RuntimeError(f"unsupported fixture entry mode: {info.st_mode:o}")


def inventory(root):
    entries = {}
    pending = [root]
    while pending:
        directory = pending.pop()
        relative = directory.relative_to(root).as_posix()
        info = directory.lstat()
        if kind(info) != "directory":
            raise RuntimeError(f"fixture directory changed type: {directory}")
        entries[relative] = signature(info)
        with os.scandir(directory) as children:
            for child in children:
                path = Path(child.path)
                relative = path.relative_to(root).as_posix()
                # Windows DirEntry.stat может вернуть нулевые dev/ino/nlink;
                # для сопоставления с открытым handle нужен полный lstat пути.
                info = path.lstat()
                if kind(info) == "directory":
                    pending.append(path)
                else:
                    entries[relative] = signature(info)
    return entries


def hash_regular(path, expected):
    if signature(path.lstat()) != expected:
        raise RuntimeError(f"file changed before open: {path}")
    # На Unix не следуем внезапно появившемуся symlink. На обеих платформах
    # проверяем identity открытого handle до чтения и повторяем stat после него.
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as source:
        opened = signature(os.fstat(source.fileno()))
        # В установленном Windows Python lstat/fstat дают разные ctime для
        # одного файла. Между API сравниваем остальные поля; полные signatures
        # каждого API ниже независимо проверяются до/после чтения.
        identity = lambda value: {key: item for key, item in value.items() if key != "st_ctime_ns"}
        if identity(opened) != identity(expected):
            raise RuntimeError(f"opened handle differs from inventoried file: {path}")
        digest = hashlib.sha256()
        remaining = opened["st_size"]
        while remaining:
            chunk = source.read(min(CHUNK, remaining))
            if not chunk:
                raise RuntimeError(f"short read during fixture hashing: {path}")
            digest.update(chunk)
            remaining -= len(chunk)
        if source.read(1):
            raise RuntimeError(f"file grew during fixture hashing: {path}")
        if signature(os.fstat(source.fileno())) != opened:
            raise RuntimeError(f"opened file changed during fixture hashing: {path}")
    if signature(path.lstat()) != expected:
        raise RuntimeError(f"file changed after hashing: {path}")
    return digest.hexdigest()


def append_field(digest, value):
    if isinstance(value, str):
        value = value.encode("utf-8", errors="surrogatepass")
    digest.update(len(value).to_bytes(8, "little"))
    digest.update(value)


def prove_fixture(root):
    root = root.absolute()
    before = inventory(root)
    records = []
    digest = hashlib.sha256(DOMAIN)
    files = total_bytes = links = directories = 0
    for relative in sorted(before, key=lambda path: path.encode("utf-8", "surrogatepass")):
        path = root if relative == "." else root / relative
        expected = before[relative]
        info = path.lstat()
        if signature(info) != expected:
            raise RuntimeError(f"entry changed before hashing: {path}")
        entry_kind = kind(info)
        record = {"path": relative, "type": entry_kind, "stat": expected}
        append_field(digest, relative)
        append_field(digest, entry_kind)
        if entry_kind == "regular":
            record["size"] = info.st_size
            record["sha256"] = hash_regular(path, expected)
            append_field(digest, info.st_size.to_bytes(8, "little"))
            append_field(digest, bytes.fromhex(record["sha256"]))
            files += 1
            total_bytes += info.st_size
        elif entry_kind in {"symlink", "junction"}:
            record["target"] = os.readlink(path)
            if signature(path.lstat()) != expected:
                raise RuntimeError(f"link changed during fixture hashing: {path}")
            append_field(digest, record["target"])
            links += 1
        else:
            directories += 1
        records.append(record)
    after = inventory(root)
    if before != after:
        changed = [name for name in before.keys() | after.keys() if before.get(name) != after.get(name)]
        raise RuntimeError(f"fixture tree changed while hashing: {root}: {changed[:8]}")
    return {
        "source_path": str(root), "sha256": digest.hexdigest(),
        "file_count": files, "regular_bytes": total_bytes,
        "link_count": links, "directory_count_including_root": directories,
        "stat_pre_post_equal": True, "records": records,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, default=Path(__file__).with_name("fixtures.json"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compare", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(f"refusing to overwrite immutable proof: {args.output}")
    started = datetime.now(timezone.utc).isoformat()
    source = args.fixtures.read_bytes()
    config = json.loads(source)
    fixtures = {}
    for name, value in sorted(config["paths"].items()):
        path = Path(value)
        if not path.is_absolute():
            path = args.fixtures.parent / path
        fixtures[name] = prove_fixture(path)
        result = fixtures[name]
        print(json.dumps({"fixture": name, "sha256": result["sha256"],
                          "files": result["file_count"], "bytes": result["regular_bytes"]}), flush=True)
    if args.fixtures.read_bytes() != source:
        raise RuntimeError("fixtures.json changed during proof generation")
    combined = hashlib.sha256(DOMAIN)
    for name, result in fixtures.items():
        append_field(combined, name)
        append_field(combined, bytes.fromhex(result["sha256"]))
    report = {
        "schema": SCHEMA, "started_utc": started,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "fixtures_config": str(args.fixtures.absolute()),
        "fixtures_config_sha256": hashlib.sha256(source).hexdigest(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "algorithm": "SHA256(domain; length-prefixed sorted relative UTF-8 paths/types; regular u64-LE sizes and SHA256 content; link targets)",
        "cache": config.get("cache", "uncontrolled"),
        "cache_note": "This proof reads every regular file and may warm filesystem caches; cache remains uncontrolled.",
        "combined_sha256": combined.hexdigest(), "fixtures": fixtures,
    }
    if args.compare:
        prior_bytes = args.compare.read_bytes()
        prior = json.loads(prior_bytes)
        equal = (prior["schema"] == SCHEMA
                 and prior["fixtures_config_sha256"] == report["fixtures_config_sha256"]
                 and prior["combined_sha256"] == report["combined_sha256"])
        report["comparison"] = {
            "before_path": str(args.compare.absolute()),
            "before_manifest_sha256": hashlib.sha256(prior_bytes).hexdigest(),
            "content_identity_equal": equal,
        }
    # Эксклюзивное создание сохраняет все прежние результаты даже при гонке имён.
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(report, output, ensure_ascii=True, indent=2)
        output.write("\n")
    print(json.dumps({"proof": str(args.output), "combined_sha256": report["combined_sha256"],
                      "comparison": report.get("comparison")}), flush=True)
    if args.compare and not report["comparison"]["content_identity_equal"]:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
