#!/usr/bin/env python3
"""Run alternating before/after benchmark pairs; keep every sample, including the first."""

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


KEY_FIELDS = ("dataset", "network_profile", "executor", "threads", "frame_target_mb", "read_buffer_kb")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def binary_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_run(directory):
    with (directory / "results_raw.csv").open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        rows, fields = list(reader), reader.fieldnames
    require(rows, f"No completed rows in {directory}")
    for row in rows:
        require(None not in row and all(value is not None for value in row.values()), f"Malformed CSV: {directory}")
        require(row["repeat_index"] == "1", f"Unexpected child repeat index: {directory}")
        for counter in ("skipped_files", "frame_aborts", "frame_rebuilds"):
            require(int(row[counter]) == 0, f"Static-fixture {counter} is nonzero: {directory}")
    payload = (directory / "run.log").read_bytes()
    verified = len(re.findall(rb"(?m)^\[bench-verify\] verified ", payload))
    linked = len(re.findall(rb"(?m)^\[bench-link\] ", payload))
    require(verified == linked == len(rows), f"Verification/link log count differs from CSV: {directory}")
    return fields, rows, payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--allow-identical-binaries", action="store_true",
                        help="allow identical SHA256 only for intentional A/A calibration")
    parser.add_argument("--output", type=Path, default=os.environ.get("FAST_DUMP_BENCH_MATRIX_DIR"))
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--dataset", default="manySmallFiles,bigFiles,mixed")
    parser.add_argument("--source", type=Path, help="use an existing immutable fixture instead of generated datasets")
    parser.add_argument("--network-profile", default="loopback-localhost")
    parser.add_argument("--threads", required=True, help="compression thread count, or comma-separated counts")
    parser.add_argument("--readers", type=int, help="force one reader count on both binaries")
    parser.add_argument("--before-readers", type=int, help="override reader count for the before binary")
    parser.add_argument("--after-readers", type=int, help="override reader count for the after binary")
    parser.add_argument("--frames", default="64")
    parser.add_argument("--buffers", default="1024")
    parser.add_argument("--executor", default="default")
    parser.add_argument("--shuffle-seed", type=int, default=91)
    args = parser.parse_args()
    require(args.output is not None, "Supply --output or FAST_DUMP_BENCH_MATRIX_DIR")
    require(args.repeats > 0, "--repeats must be positive")
    reader_counts = {side: getattr(args, side + '_readers') or args.readers for side in ('before', 'after')}
    require(all(value is None or value > 0 for value in (args.readers, args.before_readers, args.after_readers)), "Reader counts must be positive")
    output = args.output.expanduser().resolve()
    binaries = {side: getattr(args, side).expanduser().resolve() for side in ("before", "after")}
    for binary in binaries.values():
        require(binary.is_file(), f"Benchmark binary does not exist: {binary}")
    require(not output.exists(), f"Output already exists: {output}")
    if args.source is not None:
        args.source = args.source.expanduser().resolve()
        require(args.source.is_dir(), "--source must be an existing directory")
        require(output != args.source and args.source not in output.parents, "Output must not be inside the source fixture")
    hashes = {side: binary_hash(path) for side, path in binaries.items()}
    identical_binaries = hashes["before"] == hashes["after"]
    require(not identical_binaries or args.allow_identical_binaries,
            "Before and after binaries have identical SHA256; use --allow-identical-binaries only for intentional A/A calibration")
    common = ["--repeats", "1", "--shuffle-scenarios", "--shuffle-seed", str(args.shuffle_seed)]
    for name in ("dataset", "network_profile", "threads", "frames", "buffers", "executor"):
        if name == "dataset" and args.source is not None:
            common.extend(["--source", str(args.source)])
            continue
        common.extend(["--" + name.replace("_", "-"), getattr(args, name)])
    metadata = {
        "complete": False, "repeats_per_side": args.repeats,
        "policy": "AB then BA alternating; all samples retained; no automatic warmup or trimming",
        "binary_sha256": hashes,
        "allow_identical_binaries": args.allow_identical_binaries,
        "identical_binaries": identical_binaries,
        "comparison_kind": "A/A" if identical_binaries else "A/B",
        "common_arguments": common,
        "requested_readers": reader_counts,
        "dataset_directory_env": os.environ.get("FAST_DUMP_BENCH_DATASET_DIR"),
        "temp_directory_env": os.environ.get("FAST_DUMP_BENCH_TEMP_DIR"),
        "invocations": [],
    }
    output.mkdir(parents=True, exist_ok=False)
    manifest = output / "paired-run.json"

    def save_manifest():
        manifest.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    save_manifest()
    collected = {"before": [], "after": []}
    expected_fields, expected_keys, fixture_counts = None, None, {}
    for repeat in range(1, args.repeats + 1):
        order = ("before", "after") if repeat % 2 else ("after", "before")
        for side in order:
            directory = output / "runs" / f"{repeat:02d}-{side}"
            directory.mkdir(parents=True)
            environment = os.environ.copy()
            environment["FAST_DUMP_BENCH_MATRIX_DIR"] = str(directory)
            environment["FAST_DUMP_BENCH_VERIFY_ARCHIVE"] = "1"
            command = [str(binaries[side]), *common]
            if reader_counts[side] is not None:
                command.extend(["--readers", str(reader_counts[side])])
            invocation = {"pair": repeat, "side": side, "directory": str(directory), "command": command}
            metadata["invocations"].append(invocation)
            save_manifest()
            print(f"[bench-paired] pair={repeat}/{args.repeats} side={side} log={directory / 'run.log'}", flush=True)
            # Пишем байты subprocess напрямую: PowerShell не меняет кодировку или порядок stderr/stdout.
            with (directory / "run.log").open("wb") as log:
                completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, env=environment, check=False)
            invocation["exit_code"] = completed.returncode
            save_manifest()
            require(completed.returncode == 0, f"Benchmark failed ({completed.returncode}); inspect {directory / 'run.log'}")
            fields, rows, payload = read_run(directory)
            keys = sorted(tuple(row[field] for field in KEY_FIELDS) for row in rows)
            if expected_fields is None:
                expected_fields, expected_keys = fields, keys
            require(fields == expected_fields and keys == expected_keys, f"Scenario/schema mismatch: {directory}")
            for row in rows:
                counts = (int(row["files_count"]), int(row["raw_read_bytes"]))
                previous = fixture_counts.setdefault(row["dataset"], counts)
                require(counts == previous, f"Fixture counts/bytes changed: {directory}")
            invocation["raw_rows"] = len(rows)
            collected[side].append((rows, payload))
            save_manifest()

    for side, runs in collected.items():
        destination = output / side
        destination.mkdir()
        with (destination / "results_raw.csv").open("w", newline="", encoding="utf-8") as csv_stream, \
                (destination / "run.log").open("wb") as log:
            writer = csv.DictWriter(csv_stream, fieldnames=expected_fields)
            writer.writeheader()
            for rows, payload in runs:
                # repeat_index=1 сохраняется вместе с исходным логом; номер пары есть в manifest.
                writer.writerows(rows)
                log.write(payload)
                if not payload.endswith(b"\n"):
                    log.write(b"\n")
    metadata["complete"] = True
    save_manifest()
    print(f"[bench-paired] completed pairs={args.repeats} output={output}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError) as error:
        print(f"paired benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(2)
