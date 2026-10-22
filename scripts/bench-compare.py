#!/usr/bin/env python3
"""Compare every static-fixture run, retaining unmatched scenarios and raw evidence.

Single pair:
  python3 scripts/bench-compare.py --before BEFORE --after AFTER --label linux-netem-1g --output OUT
Multiple pairs:
  python3 scripts/bench-compare.py --pair linux-netem-1g BEFORE1 AFTER1 --pair linux-netem-10g BEFORE2 AFTER2 --output OUT

Inputs may be a results_raw.csv, its directory, or a run directory containing
bench/results_raw.csv. run.log/benchmark.log are discovered beside that input.
"""

import argparse
import csv
import json
import math
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


KEY_FIELDS = ("dataset", "network_profile", "executor", "threads", "frame_target_mb", "read_buffer_kb")
INTEGER_KEYS = {"threads", "frame_target_mb", "read_buffer_kb"}
STAT_NAMES = ("n", "mean_ms", "median_ms", "p95_ms", "min_ms", "max_ms")
KV_PATTERN = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)")
RATE_PATTERN = re.compile(r"(?<![A-Za-z0-9])(10|1)(?:gbps|gbit(?:/s)?|g)(?![A-Za-z0-9])", re.I)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, context):
    try:
        number = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{context}: expected integer, got {value!r}") from error
    require(number >= 0, f"{context}: negative value {number}")
    return number


def finite_float(value, context):
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{context}: expected number, got {value!r}") from error
    require(math.isfinite(number), f"{context}: non-finite value {value!r}")
    return number


def resolve_csv(argument):
    path = Path(argument).expanduser().resolve()
    candidates = [path] if path.is_file() else [path / "results_raw.csv", path / "bench" / "results_raw.csv"]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise ValueError(f"results_raw.csv not found under {path}")


def resolve_log(argument, csv_path, explicit):
    if explicit:
        path = Path(explicit).expanduser().resolve()
        require(path.is_file(), f"log does not exist: {path}")
        return path
    source = Path(argument).expanduser().resolve()
    directories = [source if source.is_dir() else source.parent, csv_path.parent, csv_path.parent.parent]
    seen = set()
    for directory in directories:
        for name in ("run.log", "benchmark.log"):
            candidate = directory / name
            if candidate not in seen and candidate.is_file():
                return candidate
            seen.add(candidate)
    return None


def load_link_lines(path):
    if path is None:
        return []
    # PowerShell redirects могут создавать UTF-16; обычные runner logs используют UTF-8.
    payload = path.read_bytes()
    encoding = "utf-16" if payload.startswith((b"\xff\xfe", b"\xfe\xff")) else "utf-8-sig"
    text = payload.decode(encoding)
    current_run = None
    entries = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if "[bench-matrix] run " in line:
            current_run = dict(KV_PATTERN.findall(line))
        if "[bench-link]" in line:
            entries.append({"line": line_no, "fields": dict(KV_PATTERN.findall(line)), "run_context": current_run})
    return entries


def scenario_key(row):
    return tuple(row[field] for field in KEY_FIELDS)


def verify_link_match(row, entry, source):
    fields = entry["fields"]
    context = f"{source}:{entry['line']}"
    for field in ("profile", "tx_bytes", "rx_bytes", "elapsed_ms"):
        require(field in fields, f"{context}: missing [bench-link] {field}")
    require(fields["profile"] == row["network_profile"], f"{context}: profile does not match raw CSV order")
    elapsed = finite_float(fields["elapsed_ms"], context)
    require(math.isclose(elapsed, row["total_duration_ms"], rel_tol=1e-8, abs_tol=0.0001),
            f"{context}: elapsed_ms differs from raw CSV row {row['csv_line']}")
    parsed = {
        "tx_bytes": integer(fields["tx_bytes"], context),
        "rx_bytes": integer(fields["rx_bytes"], context),
        "elapsed_ms": elapsed,
        "log_line": entry["line"],
    }
    if "wire_bytes" in row["raw"]:
        wire = integer(row["raw"]["wire_bytes"], context)
        require(parsed["tx_bytes"] + parsed["rx_bytes"] == wire,
                f"{context}: TX+RX does not equal CSV wire_bytes")
    for field in ("logical_bytes", "raw_read_bytes"):
        if field in fields:
            parsed[field] = integer(fields[field], context)
            require(parsed[field] == row["raw_read_bytes"],
                    f"{context}: {field} differs from static-fixture CSV bytes")
    if "tx_gbps" in fields:
        reported = finite_float(fields["tx_gbps"], context)
        calculated = parsed["tx_bytes"] * 8 / elapsed / 1_000_000
        require(math.isclose(reported, calculated, rel_tol=1e-6, abs_tol=0.000001),
                f"{context}: logged tx_gbps disagrees with bytes/elapsed")
    preceding = entry["run_context"]
    if preceding:
        expected = {
            "dataset": str(row["dataset"]), "network": str(row["network_profile"]),
            "executor": str(row["executor"]), "threads": str(row["threads"]),
            "frame": f"{row['frame_target_mb']}MiB", "buffer": f"{row['read_buffer_kb']}KiB",
        }
        for name, value in expected.items():
            require(preceding.get(name) == value, f"{context}: preceding matrix {name} disagrees with CSV order")
        if "repeat" in preceding:
            require(integer(preceding["repeat"].split("/")[0], context) == row["repeat_index"],
                    f"{context}: preceding matrix repeat disagrees with CSV order")
    row["link"] = parsed


def load_runs(argument, label, side, explicit_log, fixture_counts, warnings):
    csv_path = resolve_csv(argument)
    log_path = resolve_log(argument, csv_path, explicit_log)
    rows = []
    with csv_path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        required = set(KEY_FIELDS) | {"repeat_index", "files_count", "raw_read_bytes", "skipped_files", "frame_aborts", "frame_rebuilds", "total_duration_ms"}
        require(required.issubset(reader.fieldnames or []), f"{csv_path}: missing columns {sorted(required - set(reader.fieldnames or []))}")
        for line_no, raw in enumerate(reader, 2):
            context = f"{csv_path}:{line_no}"
            require(None not in raw and all(value is not None for value in raw.values()), f"{context}: incomplete CSV row")
            require(raw.get("protocol_runtime", "v4") == "v4", f"{context}: expected v4 runtime")
            row = {field: integer(raw[field], context) if field in INTEGER_KEYS else raw[field] for field in KEY_FIELDS}
            row.update({
                "label": label, "side": side, "csv": str(csv_path), "csv_line": line_no,
                "repeat_index": integer(raw["repeat_index"], context),
                "files_count": integer(raw["files_count"], context),
                "raw_read_bytes": integer(raw["raw_read_bytes"], context),
                "total_duration_ms": finite_float(raw["total_duration_ms"], context),
                "raw": raw, "link": None,
            })
            require(row["total_duration_ms"] > 0, f"{context}: non-positive elapsed time")
            for counter in ("skipped_files", "frame_aborts", "frame_rebuilds"):
                require(integer(raw[counter], context) == 0, f"{context}: static-fixture {counter} is not zero")
            counts = (row["files_count"], row["raw_read_bytes"])
            previous = fixture_counts.setdefault(row["dataset"], counts)
            require(previous == counts, f"{context}: fixture counts/bytes changed for {row['dataset']}: {previous} -> {counts}")
            rows.append(row)
    require(rows, f"{csv_path}: no completed raw rows")
    links = load_link_lines(log_path)
    if log_path:
        require(len(links) == len(rows), f"{log_path}: [bench-link] n={len(links)} differs from raw CSV n={len(rows)}; partial/appended logs cannot be matched safely")
        for row, entry in zip(rows, links):
            verify_link_match(row, entry, log_path)
    else:
        warnings.append(f"{label}/{side}: no run.log or benchmark.log; TX/RX throughput is unmeasured")
    identities = Counter((scenario_key(row), row["repeat_index"]) for row in rows)
    duplicates = sum(count - 1 for count in identities.values() if count > 1)
    if duplicates:
        warnings.append(f"{label}/{side}: {duplicates} repeated scenario/repeat identities retained as raw observations, not deduplicated")
    metadata = {"label": label, "side": side, "csv": str(csv_path), "log": str(log_path) if log_path else None,
                "raw_n": len(rows), "link_log_n": len(links), "log_order_verified": bool(log_path), "duplicate_repeat_identities": duplicates}
    return rows, metadata


def percentile95(values):
    ordered = sorted(values)
    position = (len(ordered) - 1) * 0.95
    low = math.floor(position)
    high = math.ceil(position)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def summarize(rows):
    if not rows:
        return {name: 0 if name == "n" else None for name in STAT_NAMES} | {
            "raw_mib_s": None, "tx_gbps": None, "rx_gbps": None, "link_log_n": 0}
    elapsed = [row["total_duration_ms"] for row in rows]
    elapsed_sum = sum(elapsed)
    linked = [row["link"] for row in rows if row["link"] is not None]
    return {
        "n": len(rows), "mean_ms": statistics.fmean(elapsed), "median_ms": statistics.median(elapsed),
        "p95_ms": percentile95(elapsed), "min_ms": min(elapsed), "max_ms": max(elapsed),
        "raw_mib_s": sum(row["raw_read_bytes"] for row in rows) * 1000 / elapsed_sum / (1024 * 1024),
        "tx_gbps": sum(link["tx_bytes"] for link in linked) * 8 / sum(link["elapsed_ms"] for link in linked) / 1_000_000 if linked else None,
        "rx_gbps": sum(link["rx_bytes"] for link in linked) * 8 / sum(link["elapsed_ms"] for link in linked) / 1_000_000 if linked else None,
        "link_log_n": len(linked),
    }


def rate_hint(label, profile, before, after):
    for text in (profile, label, Path(before).name, Path(after).name):
        rates = set(RATE_PATTERN.findall(text))
        if len(rates) == 1:
            return int(rates.pop())
    return None


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--before")
    parser.add_argument("--after")
    parser.add_argument("--label", help="physical-link/environment label; defaults to the before directory name")
    parser.add_argument("--before-log")
    parser.add_argument("--after-log")
    parser.add_argument("--pair", nargs=3, action="append", metavar=("LABEL", "BEFORE", "AFTER"), default=[])
    parser.add_argument("--output", "--output-dir", dest="output", required=True)
    parser.add_argument("--require-matched", action="store_true", help="reject before-only/after-only scenarios instead of reporting them")
    args = parser.parse_args(argv)
    if args.pair:
        require(not any((args.before, args.after, args.label, args.before_log, args.after_log)), "--pair cannot be combined with single-pair options")
        pairs = [(label, before, after, None, None) for label, before, after in args.pair]
    else:
        require(args.before and args.after, "supply --before and --after, or one or more --pair options")
        before_path = Path(args.before)
        default_label = before_path.parent.name if before_path.is_file() else before_path.name
        pairs = [(args.label or default_label, args.before, args.after, args.before_log, args.after_log)]
    require(len({pair[0] for pair in pairs}) == len(pairs), "comparison labels must be unique")
    return args, pairs


def main(argv=None):
    args, pairs = arguments(argv)
    fixture_counts, warnings, inputs, all_runs, comparisons = {}, [], [], [], []
    for label, before, after, before_log, after_log in pairs:
        sides = {}
        for side, path, explicit_log in (("before", before, before_log), ("after", after, after_log)):
            rows, metadata = load_runs(path, label, side, explicit_log, fixture_counts, warnings)
            inputs.append(metadata)
            all_runs.extend(rows)
            grouped = defaultdict(list)
            for row in rows:
                grouped[scenario_key(row)].append(row)
            sides[side] = grouped
        keys = sorted(set(sides["before"]) | set(sides["after"]))
        for key in keys:
            summaries = {side: summarize(sides[side].get(key, [])) for side in ("before", "after")}
            status = "matched" if all(summary["n"] for summary in summaries.values()) else "before_only" if summaries["before"]["n"] else "after_only"
            require(not args.require_matched or status == "matched", f"{label}: unmatched scenario {key}")
            result = {"label": label, **dict(zip(KEY_FIELDS, key)), "status": status,
                      "rate_hint_gbps": rate_hint(label, key[1], before, after),
                      "files_count": fixture_counts[key[0]][0], "raw_read_bytes": fixture_counts[key[0]][1]}
            for side, summary in summaries.items():
                result.update({f"{side}_{name}": value for name, value in summary.items()})
            before_mean, after_mean = summaries["before"]["mean_ms"], summaries["after"]["mean_ms"]
            result["elapsed_ratio_after_before"] = after_mean / before_mean if status == "matched" else None
            result["speedup_before_after"] = before_mean / after_mean if status == "matched" else None
            result["elapsed_change_pct"] = (after_mean / before_mean - 1) * 100 if status == "matched" else None
            comparisons.append(result)
    output = Path(args.output).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    csv_output, json_output = output / "comparison.csv", output / "comparison.json"
    # Сначала полностью проверяем все входы; ошибка не должна выдавать частичный отчёт.
    with csv_output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(comparisons[0]))
        writer.writeheader()
        writer.writerows(comparisons)
    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "method": {
            "group_by": ["label", *KEY_FIELDS], "elapsed_statistics": "all raw observations; no trimming or deduplication",
            "p95": "linear interpolation at (n-1)*0.95 (inclusive/type-7); interpret with n",
            "ratio": "after mean elapsed / before mean elapsed; speedup is its reciprocal",
            "throughput": "sum(bytes) / sum(elapsed), not mean of per-run throughputs; raw MiB uses 2^20 and TX/RX Gbit uses 10^9",
            "static_fixture_validation": "files_count and raw_read_bytes fixed per dataset across all inputs; skips, aborts, rebuilds must be zero",
            "log_matching": "exact n and original row order; profile, elapsed, TX+RX and available matrix context checked",
            "unmatched": "retained and labelled before_only/after_only, without a ratio",
            "rate_hint": "derived from network_profile, explicit label, or input directory name; not an independent physical-link measurement",
        },
        "inputs": inputs, "warnings": warnings,
        "fixture_counts": {name: {"files_count": values[0], "raw_read_bytes": values[1]} for name, values in fixture_counts.items()},
        "comparisons": comparisons, "runs": all_runs,
    }
    json_output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"comparison_csv": str(csv_output), "comparison_json": str(json_output), "scenarios": len(comparisons), "raw_runs": len(all_runs), "warnings": warnings}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, UnicodeError) as error:
        print(f"comparison failed: {error}", file=sys.stderr)
        raise SystemExit(2)
