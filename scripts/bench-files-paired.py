#!/usr/bin/env python3
"""Pair saved file_pipeline binaries and compare time/resource counts per entry and MiB."""

import argparse
import csv
import ctypes
import hashlib
import json
import math
import os
import platform
import re
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


MIB = 1024 * 1024
METRIC_UNITS = {
    'cpu_cycles': 'cycles',
    'io_read_ops': 'operations', 'io_write_ops': 'operations', 'io_other_ops': 'operations',
    'io_read_bytes': 'bytes', 'io_write_bytes': 'bytes', 'io_other_bytes': 'bytes',
    'io_read_ops_raw': 'operations', 'io_read_bytes_raw': 'bytes',
    'io_observer_read_ops': 'operations', 'io_observer_read_bytes': 'bytes',
    'storage_read_bytes': 'bytes', 'storage_write_bytes': 'bytes',
    'page_faults': 'faults', 'minor_faults': 'faults', 'major_faults': 'faults',
    'voluntary_switches': 'switches', 'involuntary_switches': 'switches',
    'allocs': 'allocations', 'alloc_bytes': 'bytes', 'reallocs': 'reallocations', 'deallocs': 'deallocations',
    'alloc_successes': 'allocations', 'alloc_success_bytes': 'bytes',
    'realloc_successes': 'reallocations', 'realloc_success_bytes': 'bytes',
    'alloc_failures': 'allocations', 'realloc_failures': 'reallocations',
    'dealloc_bytes': 'bytes', 'zeroed_bytes': 'bytes',
    'elapsed_ns': 'ns', 'work_ns': 'ns', 'drop_ns': 'ns',
    'cpu_ns': 'ns', 'user_ns': 'ns', 'kernel_ns': 'ns',
}
OPTIONAL_METRICS = tuple(metric for metric in METRIC_UNITS if not metric.endswith('_ns'))
SUMMARY_METRICS = tuple(METRIC_UNITS) + ('cpu_ns_per_entry', 'elapsed_ns_per_entry')
STATISTICS = ('n', 'mean', 'median', 'p95', 'min', 'max')


def counter_scopes(system=None):
    system = sys.platform if system is None else system
    scopes = dict.fromkeys(METRIC_UNITS, 'unmeasured')
    scopes.update(dict.fromkeys(('elapsed_ns', 'work_ns', 'drop_ns'), 'phase_wall_time'))
    scopes.update(dict.fromkeys(('cpu_ns', 'user_ns', 'kernel_ns'), 'process_cpu_time_all_threads'))
    scopes.update(dict.fromkeys(('allocs', 'alloc_bytes', 'reallocs'), 'diagnostic_allocator_requests'))
    scopes.update({metric: 'diagnostic_allocator_outcomes' for metric in (
        'deallocs', 'alloc_successes', 'alloc_success_bytes', 'realloc_successes',
        'realloc_success_bytes', 'alloc_failures', 'realloc_failures', 'dealloc_bytes', 'zeroed_bytes')})
    if system == 'win32':
        scopes['cpu_cycles'] = 'windows_process_cycle_time'
        scopes['page_faults'] = 'windows_process_combined_page_faults'
        scopes.update({metric: 'windows_process_io_accounting' for metric in METRIC_UNITS if metric.startswith('io_')})
        scopes.update(dict.fromkeys(('io_observer_read_ops', 'io_observer_read_bytes'), 'process_snapshot_observer_reads'))
    elif system.startswith('linux'):
        scopes['io_read_ops'] = 'linux_proc_io_read_family_syscalls_corrected'
        scopes['io_read_bytes'] = 'linux_proc_io_logical_read_bytes_corrected'
        scopes['io_write_ops'] = 'linux_proc_io_write_family_syscalls'
        scopes['io_write_bytes'] = 'linux_proc_io_logical_write_bytes'
        scopes['io_read_ops_raw'] = 'linux_proc_io_read_family_syscalls_raw'
        scopes['io_read_bytes_raw'] = 'linux_proc_io_logical_read_bytes_raw'
        scopes.update(dict.fromkeys(('io_observer_read_ops', 'io_observer_read_bytes'), 'linux_proc_io_snapshot_observer_reads'))
        scopes.update(dict.fromkeys(('storage_read_bytes', 'storage_write_bytes'), 'linux_proc_io_storage_accounting'))
        scopes.update(dict.fromkeys(('minor_faults', 'major_faults', 'page_faults'), 'linux_getrusage_process_page_faults'))
        scopes.update(dict.fromkeys(('voluntary_switches', 'involuntary_switches'), 'linux_getrusage_process_context_switches'))
    return scopes


def normalized_measurement(measurement):
    row = measurement.copy()
    for metric in OPTIONAL_METRICS:
        row.setdefault(metric, None)
    for metric in METRIC_UNITS:
        value = row.get(metric)
        for suffix, denominator in (('entry', row['entries']), ('mib', row['bytes'] / MIB)):
            row[f'{metric}_per_{suffix}'] = value / denominator if value is not None and denominator > 0 else None
    return row


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


def windows_process_cpu(process):
    from ctypes import wintypes
    get_times = ctypes.windll.kernel32.GetProcessTimes
    get_times.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    get_times.restype = wintypes.BOOL
    creation, ended, kernel, user = [wintypes.FILETIME() for _ in range(4)]
    require(get_times(int(process._handle), ctypes.byref(creation), ctypes.byref(ended),
                      ctypes.byref(kernel), ctypes.byref(user)), 'GetProcessTimes failed')
    ns = lambda value: ((value.dwHighDateTime << 32) | value.dwLowDateTime) * 100
    return {'process_user_ns': ns(user), 'process_kernel_ns': ns(kernel)}


def invoke(command, log):
    started = time.perf_counter_ns()
    with log.open('wb') as stream, subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT) as process:
        if os.name == 'posix':
            _, status, usage = os.wait4(process.pid, 0)
            process.returncode = os.waitstatus_to_exitcode(status)
            resources = {'process_user_ns': round(usage.ru_utime * 1e9),
                         'process_kernel_ns': round(usage.ru_stime * 1e9),
                         'process_peak_rss_bytes': usage.ru_maxrss * (1 if sys.platform == 'darwin' else 1024),
                         'process_voluntary_switches': usage.ru_nvcsw,
                         'process_involuntary_switches': usage.ru_nivcsw}
        else:
            process.wait()
            resources = windows_process_cpu(process)
        resources['process_elapsed_ns'] = time.perf_counter_ns() - started
        require(process.returncode == 0, f'Child exited {process.returncode}; inspect {log}')
    return resources


def parse_measurement(log):
    rows = []
    for line in log.read_text(encoding='utf-8').splitlines():
        if line.startswith('[file-perf] '):
            row = dict(re.findall(r'([a-z_]+)=([^\s]+)', line))
            for key, value in row.items():
                if key in ('stage', 'fingerprint', 'cycle_source', 'io_source') or key.endswith('_scope'):
                    continue
                row[key] = None if value == 'unmeasured' else value == 'true' if value in ('true', 'false') else int(value)
            rows.append(row)
    require(len(rows) == 1, f'Expected exactly one [file-perf] row: {log}')
    row = rows[0]
    require(row['verified'] and row['repeat'] == 1, f'Unverified/multiple-repeat child: {log}')
    valid_elapsed = row['elapsed_ns'] >= 0 if row['stage'] == 'noop' else row['elapsed_ns'] > 0
    require((row['entries'] > 0 or row['stage'] == 'noop') and valid_elapsed,
            f'Empty fixture/measurement: {log}')
    require(row['cpu_ns'] == row['user_ns'] + row['kernel_ns'], f'CPU total mismatch: {log}')
    require(row['elapsed_ns'] == row['work_ns'] + row['drop_ns'], f'Elapsed total mismatch: {log}')
    for metric in OPTIONAL_METRICS:
        row.setdefault(metric, None)
    row.setdefault('counters_schema', 1)
    row.setdefault('cycle_source', 'legacy_unreported')
    row.setdefault('io_source', 'legacy_unreported')
    if row['counters_schema'] >= 2:
        require(row['cycle_source'] != 'legacy_unreported' and row['io_source'] != 'legacy_unreported',
                f'Counter source labels missing for schema {row["counters_schema"]}: {log}')
    for suffix in ('ops', 'bytes'):
        corrected, raw, observer = (row[f'io_read_{suffix}'], row[f'io_read_{suffix}_raw'],
                                    row[f'io_observer_read_{suffix}'])
        if corrected is not None and raw is not None and observer is not None:
            require(raw == corrected + observer, f'I/O observer correction mismatch ({suffix}): {log}')
    return row


def measurement_contract(measurement):
    return tuple(measurement[key] for key in ('counters_schema', 'cycle_source', 'io_source'))


def statistics_all(values):
    ordered = sorted(values)
    position = (len(ordered) - 1) * .95
    low, high = math.floor(position), math.ceil(position)
    return {'n': len(values), 'mean': statistics.fmean(values), 'median': statistics.median(values),
            'p95': ordered[low] + (ordered[high] - ordered[low]) * (position - low),
            'min': min(values), 'max': max(values)}


def compare_metric(selected, metric, repeats):
    comparison = {}
    complete = True
    for side in ('before', 'after'):
        runs = [row for row in selected if row['side'] == side]
        values = [row[metric] for row in runs if row.get(metric) is not None]
        measured_all = len(values) == len(runs) and bool(runs)
        complete &= measured_all
        stats = statistics_all(values) if values else {name: None for name in STATISTICS}
        stats['n'] = len(values)
        comparison.update({f'{side}_{name}': value for name, value in stats.items()})
        comparison[f'{side}_complete'] = measured_all
    before_mean, after_mean = comparison['before_mean'], comparison['after_mean']
    comparison['ratio_after_before'] = after_mean / before_mean if complete and before_mean else None
    # Пропущенный счётчик не равен нулю; победы считаем только по полным парам.
    paired = []
    for pair in range(1, repeats + 1):
        values = {row['side']: row.get(metric) for row in selected if row['pair'] == pair}
        if values.get('before') is not None and values.get('after') is not None:
            paired.append(values['after'] < values['before'])
    comparison['paired_n'] = len(paired)
    comparison['after_wins'] = sum(paired) if paired else None
    return comparison


def summarize_scenario(selected, repeats, allocations_diagnostic, scopes):
    first = selected[0]
    summary = {key: first[key] for key in ('stage', 'readers', 'entries', 'files', 'bytes')}
    summary['allocations_diagnostic'] = allocations_diagnostic
    for metric in SUMMARY_METRICS:
        comparison = compare_metric(selected, metric, repeats)
        for key, value in comparison.items():
            if key.startswith(('before_', 'after_')) and key != 'after_wins':
                side, statistic = key.split('_', 1)
                summary[f'{side}_{metric}_{statistic}'] = value
            else:
                summary[f'{metric}_{key}'] = value
    resources = []
    for metric, unit in METRIC_UNITS.items():
        for denominator, suffix in (('total', ''), ('entry', '_per_entry'), ('MiB', '_per_mib')):
            resources.append({
                'stage': first['stage'], 'readers': first['readers'],
                'metric': metric, 'denominator': denominator, 'unit': unit,
                'counter_scope': scopes[metric],
                **compare_metric(selected, metric + suffix, repeats),
            })
    return summary, resources


def write_csv(path, rows):
    fields = list(dict.fromkeys(field for row in rows for field in row))
    with path.open('w', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--after', type=Path, required=True)
    parser.add_argument('--allow-identical-binaries', action='store_true',
                        help='allow identical SHA256 only for intentional A/A calibration')
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repeats', type=int, default=7)
    parser.add_argument('--stages', default='scan,read')
    parser.add_argument('--readers', default='1,4,8,32')
    parser.add_argument('--allocations', action='store_true')
    args = parser.parse_args()
    stages = list(dict.fromkeys(args.stages.split(',')))
    readers = list(dict.fromkeys(int(value) for value in args.readers.split(',')))
    require(args.repeats > 0 and all(value > 0 for value in readers), 'Counts must be positive')
    require(stages and set(stages) <= {'scan', 'read', 'noop'}, '--stages must contain scan, read, or noop')
    source, output = args.source.expanduser().resolve(), args.output.expanduser().resolve()
    binaries = {side: getattr(args, side).expanduser().resolve() for side in ('before', 'after')}
    require(source.is_dir(), f'Source does not exist: {source}')
    require(all(path.is_file() for path in binaries.values()), 'A saved binary does not exist')
    require(not output.is_relative_to(source), 'Output must be outside the source tree to keep the fixture unchanged')
    require(not output.exists(), f'Output already exists: {output}')
    hashes = {side: sha256(path) for side, path in binaries.items()}
    identical_binaries = hashes['before'] == hashes['after']
    require(not identical_binaries or args.allow_identical_binaries,
            'Before and after binaries have identical SHA256; use --allow-identical-binaries only for intentional A/A calibration')
    scenarios = [(stage, count) for stage in stages for count in (readers if stage == 'read' else [1])]
    manifest = {'complete': False, 'created_utc': datetime.now(timezone.utc).isoformat(),
                'source': str(source), 'binary_sha256': hashes, 'platform': platform.platform(),
                'allow_identical_binaries': args.allow_identical_binaries,
                'identical_binaries': identical_binaries,
                'comparison_kind': 'A/A' if identical_binaries else 'A/B',
                'logical_cpus': os.cpu_count(), 'allocations_diagnostic': args.allocations,
                'repeats': args.repeats, 'scenarios': scenarios, 'invocations': [],
                'policy': 'AB/BA alternating pairs; no trimming, implicit warmup, or dropped first samples',
                'cpu_scope': 'phase snapshots include all child threads; process_* additionally includes preparation and verification',
                'elapsed_scope': 'work plus separate result-drop interval, with validation between them excluded',
                'rss_scope': 'process_peak_rss_bytes is lifetime highwater before phase validation; not phase-only memory',
                'allocation_scope': 'diagnostic binary only; allocations include realloc requests, bytes are requested volume, not live memory',
                'counter_scope': counter_scopes(),
                'counter_interval': 'sum of work and result-drop process deltas; all process threads; preparation and validation excluded',
                'normalization': 'entry = discovered entry (regular file or symlink); MiB = 1048576 fixture bytes; zero denominators are unmeasured',
                'missing_policy': 'null/blank means unmeasured; n counts measured samples; ratios require complete data on both sides; wins use paired_n available pairs',
                'cycles_scope': 'Windows QueryProcessCycleTime sum of user/kernel thread cycles; not retired instructions and never converted to nanoseconds',
                'io_scope': 'Windows counts all process I/O (file/network/device); Linux syscr/syscw and rchar/wchar cover read/write families, not open/stat/getdents/close',
                'storage_scope': 'Linux storage-layer read_bytes/write_bytes accounting; cached reads may be zero and writes need not be durable',
                'counter_overhead': 'schema2 Linux read counters subtract the exact BEFORE-snapshot read; raw/observer counters retained; CPU/wall overhead is not subtracted; use noop for calibration',
                'unmeasured_hardware_counters': ['retired_instructions', 'cache_accesses', 'cache_misses', 'branches', 'branch_misses']}
    output.mkdir(parents=True)
    manifest_path = output / 'paired-run.json'
    write_json(manifest_path, manifest)
    samples, fixture_identity, contract = [], {}, None
    for stage, reader_count in scenarios:
        for pair in range(1, args.repeats + 1):
            for position, side in enumerate(('before', 'after') if pair % 2 else ('after', 'before'), 1):
                directory = output / 'runs' / f'{stage}-r{reader_count}' / f'{pair:02d}-{side}'
                directory.mkdir(parents=True)
                command = [str(binaries[side]), '--source', str(source), '--stage', stage,
                           '--readers', str(reader_count), '--repeats', '1']
                if args.allocations:
                    command.append('--allocations')
                invocation = {'stage': stage, 'readers': reader_count, 'pair': pair, 'side': side,
                              'position': position, 'command': command, 'log': str(directory / 'run.log')}
                manifest['invocations'].append(invocation)
                write_json(manifest_path, manifest)
                print(f'[files-paired] stage={stage} readers={reader_count} pair={pair}/{args.repeats} side={side}', flush=True)
                resources = invoke(command, directory / 'run.log')
                measured = parse_measurement(directory / 'run.log')
                current_contract = measurement_contract(measured)
                if contract is None:
                    contract = current_contract
                    manifest['measurement_contract'] = dict(zip(('counters_schema', 'cycle_source', 'io_source'), contract))
                require(contract == current_contract, 'Counter schema/source mismatch: compare binaries built with the same instrumentation')
                require(measured['stage'] == stage and measured['readers'] == reader_count, 'Child scenario mismatch')
                require(measured['allocator_instrumented'] == measured['allocations_enabled'] == args.allocations,
                        'Allocator instrumentation mismatch: use separate timing and diagnostic binaries')
                identity = tuple(measured[key] for key in ('entries', 'files', 'bytes', 'fingerprint'))
                require(fixture_identity.setdefault(stage, identity) == identity, 'Fixture/order/content changed across runs')
                sample = {'pair': pair, 'side': side, 'position': position, **normalized_measurement(measured),
                          'lifetime_user_ns': resources['process_user_ns'],
                          'lifetime_kernel_ns': resources['process_kernel_ns'],
                          'lifetime_elapsed_ns': resources['process_elapsed_ns'], 'log': str(directory / 'run.log')}
                invocation.update({'measurement': measured, 'process_lifetime': resources, 'complete': True})
                samples.append(sample)
                write_json(manifest_path, manifest)
    require(hashes == {side: sha256(path) for side, path in binaries.items()}, 'Saved binaries changed during measurement')
    summaries, resource_comparisons = [], []
    for stage, count in scenarios:
        selected = [row for row in samples if row['stage'] == stage and row['readers'] == count]
        summary, resources = summarize_scenario(selected, args.repeats, args.allocations, manifest['counter_scope'])
        summaries.append(summary)
        resource_comparisons.extend(resources)
    write_csv(output / 'samples.csv', samples)
    write_csv(output / 'comparison.csv', summaries)
    write_csv(output / 'resources.csv', resource_comparisons)
    manifest['complete'] = True
    write_json(output / 'comparison.json', {'method': manifest, 'comparisons': summaries,
                                          'resources': resource_comparisons, 'samples': samples})
    write_json(manifest_path, manifest)
    print(f'[files-paired] complete output={output} samples={len(samples)}', flush=True)
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError) as error:
        print(f'file comparison failed: {error}', file=sys.stderr)
        raise SystemExit(2)
