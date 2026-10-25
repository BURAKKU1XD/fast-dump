#!/usr/bin/env python3
"""Record checks and benchmarks, retain compact evidence, and render local history."""

import argparse
import csv
import hashlib
import html
import io
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCHEMA = 1
METRIC_FILES = {
    'results_raw.csv', 'results_summary.csv', 'samples.csv', 'resources.csv',
    'comparison.csv', 'comparison.json', 'summary.json', 'paired-run.json',
    'exit-codes.json',
}
SCENARIO_FIELDS = (
    'dataset', 'protocol_runtime', 'network_profile', 'executor', 'threads',
    'frame_target_mb', 'read_buffer_kb', 'large_file_single_frame_threshold_mib',
    'manifest_chunk_kb', 'manifest_ack_records', 'manifest_ack_kb',
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def digest_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def read_json(path):
    with Path(path).open(encoding='utf-8-sig') as stream:
        return json.load(stream)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def number(value):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def truth(value):
    if isinstance(value, bool):
        return value
    return {'true': True, '1': True, 'false': False, '0': False}.get(str(value).lower())


def stats(values, unit, direction='lower'):
    observed = [value for value in values if value is not None and math.isfinite(value)]
    if not observed:
        return {'n': 0, 'mean': None, 'median': None, 'min': None, 'max': None, 'p95': None,
                'unit': unit, 'direction': direction}
    ordered = sorted(observed)
    position = (len(ordered) - 1) * .95
    low, high = math.floor(position), math.ceil(position)
    return {'n': len(observed), 'mean': statistics.fmean(observed), 'median': statistics.median(observed),
            'min': ordered[0], 'max': ordered[-1],
            'p95': ordered[low] + (ordered[high] - ordered[low]) * (position - low),
            'unit': unit, 'direction': direction}


def read_csv(path):
    with Path(path).open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames):
            raise ValueError(f'Invalid CSV header: {path}')
        rows = list(reader)
        if any(None in row or any(value is None for value in row.values()) for row in rows):
            raise ValueError(f'Incomplete CSV row: {path}')
        return rows


def csv_bytes(rows):
    buffer = io.StringIO(newline='')
    fields = list(dict.fromkeys(key for row in rows for key in row))
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode('utf-8')


def slug(value):
    return re.sub(r'[^A-Za-z0-9._-]+', '-', value).strip('-.')[:60] or 'run'


def relative(path, root):
    try:
        return Path(path).resolve().relative_to(Path(root).resolve()).as_posix()
    except ValueError:
        return Path(path).as_posix()


def command_text(command, cwd=ROOT):
    try:
        result = subprocess.run(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                check=False, timeout=15)
        if result.returncode == 0:
            return result.stdout.decode('utf-8', errors='replace').strip()
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def provenance(repo):
    # Результаты сами меняют Git working tree, но не исходники измеряемой программы.
    paths = command_text(['git', 'ls-files', '-c', '-o', '--exclude-standard', '-z'], repo)
    digest = hashlib.sha256()
    included = []
    if paths is not None:
        for name in sorted(set(paths.split('\0'))):
            if not name or name.startswith(('results/', 'releases/')):
                continue
            path = repo / name
            if path.is_file():
                included.append(name)
                digest.update(name.encode('utf-8') + b'\0' + digest_file(path).encode('ascii') + b'\n')
    status = command_text(['git', 'status', '--porcelain', '--untracked-files=all', '--', '.',
                           ':(exclude)results/**', ':(exclude)releases/**'], repo)
    harness = hashlib.sha256()
    harness_files = sorted((repo / 'benches').rglob('*.rs')) if (repo / 'benches').is_dir() else []
    for path in harness_files:
        harness.update(relative(path, repo).encode('utf-8') + b'\0' + digest_file(path).encode('ascii'))
    return {
        'commit': command_text(['git', 'rev-parse', 'HEAD'], repo),
        'source_dirty': bool(status) if status is not None else None,
        'source_sha256': digest.hexdigest() if included else None,
        'benchmark_harness_sha256': harness.hexdigest() if harness_files else None,
        'cargo_lock_sha256': digest_file(repo / 'Cargo.lock') if (repo / 'Cargo.lock').is_file() else None,
        'rustc': command_text(['rustc', '--version', '--verbose'], repo),
        'host': {'node': platform.node(), 'system': platform.system(), 'release': platform.release(),
                 'machine': platform.machine(), 'processor': platform.processor(), 'logical_cpus': os.cpu_count()},
    }


def matrix_entries(rows, metadata, side='', summary_only=False):
    groups = defaultdict(list)
    for row in rows:
        if 'dataset' not in row or 'total_duration_ms' not in row:
            continue
        key = tuple(row.get(field) or None for field in SCENARIO_FIELDS)
        groups[key].append(row)
    entries = []
    for key, selected in groups.items():
        scenario = dict(zip(SCENARIO_FIELDS, key))
        readers = metadata.get('requested_readers') or {}
        scenario['readers'] = readers.get(side) if isinstance(readers, dict) else readers
        scenario['side'] = side or None
        elapsed = [number(row.get('total_duration_ms')) for row in selected]
        counts = {row.get('files_count') for row in selected}
        sizes = {row.get('raw_read_bytes') for row in selected}
        clean = all(number(row.get(field)) == 0 for row in selected
                    for field in ('skipped_files', 'frame_aborts', 'frame_rebuilds'))
        metrics = {'elapsed_ms': stats(elapsed, 'ms'),
                   'throughput_mib_s': stats([number(row.get('total_mib_s')) for row in selected], 'MiB/s', 'higher')}
        for name, field, unit in [('cpu_pct', 'avg_cpu_pct', '%'), ('rss_mib', 'max_rss_mib', 'MiB'),
                                  ('first_bundle_ms', 'time_to_first_frame_ms', 'ms')]:
            values = [number(row.get(field)) for row in selected]
            # Старый matrix пишет нули вместо отсутствующих OS resource counters.
            if name in ('cpu_pct', 'rss_mib'):
                values = [value if value and value > 0 else None for value in values]
            metrics[name] = stats(values, unit)
        for field, unit in (('cpu_cycles', 'cycles'), ('cpu_user_ns', 'ns'), ('cpu_kernel_ns', 'ns'),
                            ('io_read_ops', 'operations'), ('io_read_bytes', 'bytes'),
                            ('receiver_write_io_ns', 'ns'), ('receiver_periodic_sync_ns', 'ns'),
                            ('receiver_periodic_sync_calls', 'calls'), ('receiver_clean_sync_skips', 'calls'),
                            ('tx_gbps', 'Gbit/s'), ('rx_gbps', 'Gbit/s')):
            if any(field in row for row in selected):
                metrics[field] = stats([number(row.get(field)) for row in selected], unit,
                                       'higher' if field.endswith('gbps') else 'lower')
        if summary_only:
            for metric in metrics.values():
                metric.update(n=None, median=None, min=None, max=None, p95=None)
        count = next(iter(counts)) if len(counts) == 1 else None
        size = next(iter(sizes)) if len(sizes) == 1 else None
        entries.append({'family': 'matrix', 'case': scenario['dataset'], 'side': side,
                        'scenario': scenario, 'metrics': metrics,
                        'sample_count': None if summary_only else len(selected),
                        'reported_repeats': selected[0].get('completed_repeats') if summary_only else None,
                        'evidence': 'summary_only' if summary_only else 'raw',
                        'fixture': {'entries': number(count), 'bytes': number(size), 'fingerprint': None},
                        'verified': (clean and metadata.get('archive_verified') is True) if not summary_only else None,
                        'static_fixture_clean': clean,
                        'counter_contract': None,
                        'notes': ['Fixture content fingerprint unavailable; historical comparisons require context.']})
    return entries


def file_entries(rows, metadata):
    groups = defaultdict(list)
    for row in rows:
        if row.get('stage') not in ('scan', 'read', 'noop') or 'elapsed_ns' not in row:
            continue
        key = tuple(row.get(field) for field in ('stage', 'readers', 'side', 'counters_schema', 'cycle_source', 'io_source'))
        groups[key].append(row)
    entries = []
    for key, selected in groups.items():
        stage, readers, side, schema, cycles, io_source = key
        allocation = truth(selected[0].get('allocator_instrumented'))
        if allocation is None:
            allocation = metadata.get('allocations_diagnostic')
        metrics = {}
        for name, field, divisor, unit in (
            ('elapsed_ms', 'elapsed_ns', 1e6, 'ms'), ('cpu_ms', 'cpu_ns', 1e6, 'ms'),
            ('cpu_cycles', 'cpu_cycles', 1, 'cycles'), ('rss_mib', 'process_peak_rss_bytes', 1048576, 'MiB'),
            ('allocations', 'allocs', 1, 'requests'), ('allocated_bytes', 'alloc_bytes', 1, 'bytes'),
            ('read_operations', 'io_read_ops', 1, 'operations'), ('read_bytes', 'io_read_bytes', 1, 'bytes'),
        ):
            values = [number(row.get(field)) for row in selected]
            metrics[name] = stats([value / divisor if value is not None else None for value in values], unit)
        identities = {tuple(str(row.get(field)) for field in ('entries', 'bytes', 'fingerprint')) for row in selected}
        first = selected[0]
        entries.append({'family': 'file', 'case': stage, 'side': side or '',
                        'scenario': {'stage': stage, 'readers': number(readers), 'allocations': allocation},
                        'metrics': metrics, 'sample_count': len(selected), 'evidence': 'raw',
                        'fixture': {'entries': number(first.get('entries')), 'bytes': number(first.get('bytes')),
                                    'fingerprint': str(first.get('fingerprint')) if first.get('fingerprint') is not None and len(identities) == 1 else None,
                                    'scope': 'entry_order' if stage == 'scan' else 'content'},
                        'verified': all(truth(row.get('verified')) is True for row in selected),
                        'counter_contract': {'schema': schema or 1, 'cycles': cycles, 'io': io_source}, 'notes': []})
    return entries


def criterion_entry(sample, benchmark, estimates):
    iters, times = sample.get('iters', []), sample.get('times', [])
    if not iters or len(iters) != len(times):
        raise ValueError('Criterion iters/times lengths do not match')
    values = []
    for count, elapsed in zip(iters, times):
        count, elapsed = number(count), number(elapsed)
        if count is None or count <= 0 or elapsed is None or elapsed < 0:
            raise ValueError('Invalid Criterion iteration sample')
        values.append(elapsed / count)
    metric = stats(values, 'ns/iteration')
    metric['criterion_estimate'] = estimates.get('slope') or estimates.get('mean')
    return {'family': 'criterion', 'case': benchmark.get('full_id') or benchmark.get('title') or 'unknown',
            'side': '', 'scenario': {'sampling_mode': sample.get('sampling_mode'), 'throughput': benchmark.get('throughput')},
            'metrics': {'time_per_iteration_ns': metric}, 'sample_count': len(values), 'evidence': 'raw',
            'fixture': None, 'verified': None, 'counter_contract': None,
            'notes': ['Criterion samples contain total time and iteration count, not independent whole-session runs.']}


def file_perf_rows(log):
    rows = []
    with Path(log).open(encoding='utf-8', errors='replace') as stream:
        for line in stream:
            if '[file-perf] ' in line:
                rows.append(dict(re.findall(r'([A-Za-z_][A-Za-z_0-9]*)=([^\s]+)', line.split('[file-perf] ', 1)[1])))
    return rows


def matrix_log_samples(text, rows):
    samples, pending = [], {}
    prefixes = {'work': 'cpu_', 'io': 'io_', 'receiver-work': 'receiver_', 'commit': 'commit_'}
    for line in text.splitlines():
        match = re.match(r'\[bench-(work|io|receiver-work|commit|link)\] (.*)', line)
        if not match:
            continue
        kind, body = match.groups()
        fields = dict(re.findall(r'([A-Za-z_][A-Za-z_0-9]*)=([^\s]+)', body))
        if kind == 'link':
            pending.update(fields)
            elapsed = number(fields.get('elapsed_ms'))
            received = number(fields.get('rx_bytes'))
            pending['rx_gbps'] = received * 8 / elapsed / 1e6 if elapsed and received is not None else None
            samples.append(pending)
            pending = {}
        else:
            for key, value in fields.items():
                name = key if key == 'cpu_cycles' else prefixes[kind] + key
                pending[name] = value
    if len(samples) != len(rows) or not rows:
        return None
    for sample, row in zip(samples, rows):
        duration, expected = number(sample.get('elapsed_ms')), number(row.get('total_duration_ms'))
        if duration is None or expected is None or abs(duration - expected) > .01:
            return None
        if row.get('network_profile') and sample.get('profile') != row['network_profile']:
            return None
        if number(sample.get('raw_read_bytes')) != number(row.get('raw_read_bytes')):
            return None
        if row.get('wire_bytes') and number(sample.get('tx_bytes')) is not None and number(sample.get('rx_bytes')) is not None:
            if int(sample['tx_bytes']) + int(sample['rx_bytes']) != int(row['wire_bytes']):
                return None
        sample['repeat_index'] = row.get('repeat_index')
        for field in SCENARIO_FIELDS:
            if field in row:
                sample[field] = row[field]
    return samples


def matrix_from_path(path, metadata, side='', summary_only=False, capture=None, fallback_log=None, context=None):
    rows = read_csv(path)
    metadata = dict(metadata)
    logs = [Path(path).parent / name for name in ('run.log', 'benchmark.log', 'output.log')]
    if fallback_log:
        logs.append(fallback_log)
    for log in logs:
        if not log.is_file():
            continue
        if context is not None:
            context.append(log)
        text = log.read_text(encoding='utf-8', errors='replace')
        if len(re.findall(r'(?m)^\[bench-verify\] verified ', text)) == len(rows) and rows:
            metadata['archive_verified'] = True
        readers = set(re.findall(r'(?m)^\[bench-readers\] requested=(\d+)', text))
        if len(readers) == 1 and not metadata.get('requested_readers'):
            metadata['requested_readers'] = {side: int(next(iter(readers)))}
        supplemental = matrix_log_samples(text, rows) if not summary_only else None
        if supplemental is not None:
            rows = [{**row, **{key: value for key, value in sample.items() if key not in row}}
                    for row, sample in zip(rows, supplemental)]
            if capture:
                capture(log, (side + '/' if side else '') + 'resource_samples.csv', csv_bytes(supplemental))
        break
    return matrix_entries(rows, metadata, side, summary_only)


def experiment(directory, origin_root, fallback_log=None, policy_root=None):
    """Read one existing report without modifying it or trusting executable content."""
    directory = Path(directory)
    metadata, entries, warnings, artifacts, context = {}, [], [], [], []
    test_summary = None
    paired = directory / 'paired-run.json'
    if paired.is_file():
        metadata = read_json(paired)
        if not isinstance(metadata, dict):
            raise ValueError(f'Invalid paired manifest: {paired}')
        context.append(paired)

    def add(path, destination=None, payload=None):
        path = Path(path)
        artifacts.append({'path': path, 'name': destination or path.name, 'payload': payload})

    # Верхний paired report уже содержит все child samples: вторично их не импортируем.
    matrix_children = list((directory / 'runs').rglob('results_raw.csv')) if paired.is_file() else []
    if any((directory / side / 'results_raw.csv').is_file() for side in ('before', 'after')) or matrix_children:
        for side in ('before', 'after'):
            path = directory / side / 'results_raw.csv'
            if path.is_file():
                entries.extend(matrix_from_path(path, metadata, side, capture=add, context=context))
                add(path, side + '/results_raw.csv')
            else:
                metadata['complete'] = False
                for child in sorted(path for path in matrix_children if path.parent.name.endswith('-' + side)):
                    prefix = side + '/' + child.parent.name + '/'
                    def add_child(log, destination, payload, prefix=prefix):
                        add(log, prefix + Path(destination).name, payload)
                    entries.extend(matrix_from_path(child, metadata, side, capture=add_child, context=context))
                    add(child, prefix + 'results_raw.csv')
    elif (directory / 'samples.csv').is_file():
        path = directory / 'samples.csv'
        rows = read_csv(path)
        entries.extend(file_entries(rows, metadata))
        if any(truth(row.get('valid_for_threshold_comparison')) is False for row in rows):
            metadata['valid_for_threshold_comparison'] = False
        add(path)
        if not entries:
            warnings.append('Unrecognized samples.csv schema; original measurements retained without inferred semantics.')
    elif (directory / 'results_raw.csv').is_file():
        path = directory / 'results_raw.csv'
        entries.extend(matrix_from_path(path, metadata, capture=add, fallback_log=fallback_log, context=context))
        add(path)
    elif (directory / 'results_summary.csv').is_file():
        path = directory / 'results_summary.csv'
        entries.extend(matrix_from_path(path, metadata, summary_only=True))
        add(path)
    elif (directory / 'new' / 'sample.json').is_file() or (directory / 'sample.json').is_file():
        base = directory / 'new' if (directory / 'new' / 'sample.json').is_file() else directory
        sample = read_json(base / 'sample.json')
        benchmark = read_json(base / 'benchmark.json') if (base / 'benchmark.json').is_file() else {}
        estimates = read_json(base / 'estimates.json') if (base / 'estimates.json').is_file() else {}
        entries.append(criterion_entry(sample, benchmark, estimates))
        for name in ('sample.json', 'benchmark.json', 'estimates.json'):
            if (base / name).is_file():
                add(base / name)
    elif (directory / 'comparison.json').is_file():
        path = directory / 'comparison.json'
        report = read_json(path)
        if isinstance(report, dict) and isinstance(report.get('samples'), list) and report['samples']:
            metadata = report.get('method', metadata)
            rows = report['samples']
            entries.extend(file_entries(rows, metadata))
            add(path, 'samples.csv', csv_bytes(rows))
        elif isinstance(report, dict) and isinstance(report.get('runs'), list) and report['runs']:
            groups = defaultdict(list)
            for row in report['runs']:
                groups[(row.get('label', ''), row.get('side', ''))].append(row.get('raw', row))
            for index, ((label, side), rows) in enumerate(sorted(groups.items())):
                entries.extend(matrix_entries(rows, metadata, side))
                add(path, f'{index:02d}-{slug(label)}-{slug(side)}/results_raw.csv', csv_bytes(rows))
        elif isinstance(report, dict):
            # Неизвестный summary не превращаем в выдуманные сырые наблюдения.
            add(path)
            warnings.append('Legacy summary-only report; no supported raw observations.')
        else:
            warnings.append('Unrecognized comparison JSON.')
    else:
        for name in ('comparison.csv', 'resources.csv', 'summary.json'):
            if (directory / name).is_file():
                add(directory / name)
        if artifacts:
            warnings.append('Legacy report schema retained for inspection; no inferred sample counts or comparisons.')

    test_path = directory / 'summary.json'
    if not entries and test_path.is_file():
        saved = read_json(test_path)
        if isinstance(saved, dict) and isinstance(saved.get('checks'), dict):
            test_summary = {key: saved[key] for key in ('checks', 'python_tests', 'additional_checks', 'stress', 'limitations') if key in saved}
            metadata['commit'] = saved.get('commit')
            artifacts = []
            add(test_path, 'checks.json', (json.dumps(test_summary, ensure_ascii=False, indent=2) + '\n').encode('utf-8'))
            warnings = []
    if not artifacts and (directory / 'exit-codes.json').is_file():
        test_path = directory / 'exit-codes.json'
        saved = read_json(test_path)
        if isinstance(saved, dict) and all(isinstance(value, int) for value in saved.values()):
            test_summary = {'checks': [{'check': key, 'exit_code': value} for key, value in saved.items()]}
            add(test_path)

    if not artifacts and paired.is_file():
        # У failed paired run samples.csv ещё нет; завершённые измерения есть в manifest/logs.
        rows, used_logs = [], set()
        available_logs = list((directory / 'runs').rglob('run.log'))
        for invocation in metadata.get('invocations', []):
            if not isinstance(invocation, dict):
                continue
            measured = invocation.get('measurement')
            if isinstance(measured, dict):
                rows.append({**measured, **{key: invocation[key] for key in ('pair', 'side', 'position') if key in invocation}})
                candidate = str(invocation.get('log') or invocation.get('directory', '')).replace('\\', '/').rstrip('/')
                used_logs.update(log for log in available_logs if candidate.endswith(relative(log, directory)) or
                                 candidate.endswith(relative(log.parent, directory)))
        for log in available_logs:
            if log in used_logs:
                continue
            observed = file_perf_rows(log)
            match = re.match(r'(\d+)-(before|after)$', log.parent.name)
            for row in observed:
                if match:
                    row.update(pair=int(match[1]), side=match[2])
                rows.append(row)
            if observed:
                context.append(log)
        if rows:
            add(paired, 'samples.csv', csv_bytes(rows))
            entries.extend(file_entries(rows, metadata))
            metadata['complete'] = False

    # При незавершённом file benchmark stdout может быть единственным источником samples.
    if not artifacts:
        for name in ('output.log', 'run.log'):
            path = directory / name
            if path.is_file():
                rows = file_perf_rows(path)
                if rows:
                    add(path, 'samples.csv', csv_bytes(rows))
                    entries.extend(file_entries(rows, metadata))
                    break
    # Исключения берём только из явных отчётов автора, не угадываем по имени каталога.
    origin_root = Path(origin_root).resolve()
    policy_root = Path(policy_root or origin_root).resolve()
    for ancestor in (directory, *directory.parents):
        if ancestor != policy_root and policy_root not in ancestor.parents:
            break
        for name in ('analysis-summary.json', 'summary.json'):
            path = ancestor / name
            if not path.is_file():
                continue
            try:
                policy = read_json(path)
                if not isinstance(policy, dict):
                    continue
                excluded = policy.get('excluded_directories', [])
                method = policy.get('method')
                if isinstance(method, dict):
                    excluded = [*excluded, *method.get('excluded_directories', [])]
                for value in excluded:
                    if isinstance(value, str) and directory.is_relative_to((ancestor / value).resolve()):
                        metadata['valid_for_comparison'] = False
                        warnings.append(f'Explicitly excluded by {relative(path, origin_root)}: {value}')
                        context.append(path)
            except (OSError, ValueError, TypeError):
                continue
    return {'directory': directory, 'origin': relative(directory, origin_root), 'metadata': metadata,
            'entries': entries, 'warnings': warnings, 'artifacts': artifacts, 'context': list(dict.fromkeys(context)),
            'paired': paired if paired.is_file() else None, 'test_summary': test_summary}


def discover(root):
    """Choose primary sample sets, avoiding Criterion/paired aggregate copies."""
    root = Path(root).resolve()
    candidates, perf_logs = set(), set()
    for directory, subdirs, names in os.walk(root):
        subdirs[:] = [name for name in subdirs if name not in ('.git', 'node_modules', '__pycache__')]
        path = Path(directory)
        names = set(names)
        if names & METRIC_FILES:
            candidates.add(path)
        for name in names & {'run.log', 'output.log'}:
            if file_perf_rows(path / name):
                perf_logs.add(path)
                break
        if 'sample.json' in names and 'benchmark.json' in names:
            if path.name == 'new':
                candidates.add(path.parent)
            elif path.name == 'base':
                new = path.parent / 'new'
                if not (new / 'sample.json').is_file() or digest_file(path / 'sample.json') != digest_file(new / 'sample.json'):
                    candidates.add(path)
            elif path.name != 'change':
                candidates.add(path)
    candidates.update(path for path in perf_logs if not any(path != child and path in child.parents for child in perf_logs))
    paired = [path for path in candidates if (path / 'paired-run.json').is_file()]
    result = []
    for path in sorted(candidates):
        if any(parent != path and parent in path.parents for parent in paired):
            continue
        result.append(path)
    return result


def compact_metadata(metadata):
    keys = ('complete', 'created_utc', 'binary_sha256', 'comparison_kind', 'requested_readers',
            'common_arguments', 'platform', 'logical_cpus', 'allocations_diagnostic', 'measurement_contract',
            'counter_scope', 'repeats', 'repeats_per_side', 'policy', 'scenarios',
            'valid', 'valid_for_comparison', 'valid_for_threshold_comparison', 'comparison_note', 'status', 'commit')
    result = {key: metadata[key] for key in keys if key in metadata}
    invocations = metadata.get('invocations')
    if isinstance(invocations, list):
        result['order'] = [{key: row[key] for key in ('pair', 'side', 'stage', 'readers', 'exit_code') if key in row}
                           for row in invocations if isinstance(row, dict)]
    return result


def explicitly_invalid(metadata):
    for field in ('valid', 'valid_for_comparison', 'valid_for_threshold_comparison'):
        if field in metadata and truth(metadata[field]) is False:
            return True
    return metadata.get('status') in ('invalid', 'invalidated', 'rejected')


def option_value(command, name):
    for index, argument in enumerate(command):
        if argument.startswith(name + '='):
            return argument.split('=', 1)[1]
        if argument == name and index + 1 < len(command):
            return command[index + 1]
    return None


def cargo_bench_from_repository(command, repo):
    if not command or Path(command[0]).stem.lower() != 'cargo':
        return False
    arguments = list(command[1:])
    if arguments and arguments[0].startswith('+'):
        arguments.pop(0)
    while arguments:
        option = arguments.pop(0)
        if option in ('-q', '--quiet', '-v', '-vv', '--verbose', '--locked', '--offline', '--frozen'):
            continue
        if option in ('--color', '--config', '-Z') and arguments:
            arguments.pop(0)
            continue
        if option.startswith(('--color=', '--config=', '-Z')):
            continue
        if option != 'bench':
            return False
        break
    else:
        return False
    arguments = arguments[:arguments.index('--')] if '--' in arguments else arguments
    if any(option == '-C' or option.startswith('-C') for option in arguments):
        return False
    manifest = option_value(arguments, '--manifest-path')
    return manifest is None or (Path(repo) / manifest).resolve() == (Path(repo) / 'Cargo.toml').resolve()


def fixture_digest(directory):
    """Hash an explicitly selected immutable fixture after measurement, never in its timer."""
    digest = hashlib.sha256()
    directory = Path(directory)
    for base, subdirs, files in os.walk(directory, followlinks=False):
        for name in sorted(subdirs + files):
            path = Path(base) / name
            if path.is_symlink():
                digest.update(b'L\0' + path.relative_to(directory).as_posix().encode('utf-8', errors='surrogateescape') + b'\0')
                digest.update(os.readlink(path).encode('utf-8', errors='surrogateescape') + b'\n')
            elif path.is_file():
                digest.update(b'F\0' + path.relative_to(directory).as_posix().encode('utf-8', errors='surrogateescape') + b'\0')
                digest.update(digest_file(path).encode('ascii') + b'\n')
        subdirs.sort()
    return digest.hexdigest()


def enrich_recorded_entries(summary, manifest, local, command, repo):
    entries = [entry for entry in summary['entries'] if entry['family'] == 'matrix']
    if not entries:
        return
    output = (local / 'output.log').read_text(encoding='utf-8', errors='replace')
    raw_count = sum(entry.get('sample_count') or 0 for entry in entries)
    archive_verified = raw_count > 0 and len(re.findall(r'(?m)^\[bench-verify\] verified ', output)) == raw_count
    readers = option_value(command, '--readers')
    source = option_value(command, '--source')
    source_path = (repo / source).resolve() if source else None
    fingerprint = fixture_digest(source_path) if source_path and source_path.is_dir() else None
    cargo_bench = cargo_bench_from_repository(command, repo)
    actual_fixtures = {}
    for dataset, path in re.findall(r'(?m)^\[bench-matrix\] dataset=(\S+) path=(.+?) scenarios=', output):
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = repo / candidate
        if candidate.is_dir():
            actual_fixtures.setdefault(dataset, set()).add(candidate.resolve())
    fixture_hashes = {dataset: fixture_digest(next(iter(paths))) for dataset, paths in actual_fixtures.items() if len(paths) == 1}
    for entry in entries:
        if entry['scenario'].get('readers') is None:
            if readers is not None:
                entry['scenario']['readers'] = number(readers)
            elif cargo_bench:
                entry['scenario']['readers'] = 'auto'
        if archive_verified:
            entry['verified'] = entry.get('static_fixture_clean') is True
        if fingerprint:
            entry['fixture']['fingerprint'] = fingerprint
            entry['fixture']['scope'] = 'content_sha256_after_run'
        elif entry['case'] in fixture_hashes:
            entry['fixture']['fingerprint'] = fixture_hashes[entry['case']]
            entry['fixture']['scope'] = 'content_sha256_after_run'
    manifest['fixture_identity_policy'] = 'Actual explicit/logged fixture content is hashed after timing; generator source and matching file counts are never substituted for content identity.'


def artifact_identity(item):
    artifact = {'source': item['path'].as_posix(), 'sha256': digest_file(item['path']),
                'bytes': item['path'].stat().st_size}
    if item['payload'] is not None:
        artifact['extracted_sha256'] = hashlib.sha256(item['payload']).hexdigest()
    return artifact


def publish_experiments(experiments, destination, origin_root):
    entries, warnings, evidence = [], [], []
    for index, report in enumerate(experiments):
        prefix = '' if len(experiments) == 1 else f'{index:04d}-{slug(report["directory"].name)}/'
        paths = []
        for item in report['artifacts']:
            output = destination / 'raw' / (prefix + item['name'])
            output.parent.mkdir(parents=True, exist_ok=True)
            if item['payload'] is None:
                shutil.copyfile(item['path'], output)
            else:
                output.write_bytes(item['payload'])
            original = artifact_identity(item)
            original['source'] = relative(item['path'], origin_root)
            original['saved'] = relative(output, destination)
            evidence.append(original)
            paths.append(original['saved'])
        for row in report['entries']:
            row = dict(row)
            row['files'] = paths
            row['experiment'] = report['origin']
            row['pair_context'] = report['origin'] if report['paired'] or report['metadata'].get('comparison_kind') in ('A/A', 'A/B') else None
            entries.append(row)
        warnings.extend(report['warnings'])
    return {'schema_version': SCHEMA, 'entries': entries, 'warnings': list(dict.fromkeys(warnings))}, evidence


def import_signature(measurements, context, parameters):
    def normalized(items):
        keys = ('source', 'sha256', 'bytes', 'extracted_sha256')
        return sorted(({key: item[key] for key in keys if key in item} for item in items),
                      key=lambda value: json.dumps(value, sort_keys=True))
    payload = {'measurements': normalized(measurements), 'context': normalized(context), 'parameters': parameters}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()


def known_imports(results_root):
    known = {}
    for kind in ('benchmarks', 'tests'):
        for path in (results_root / kind).glob('*/run.json'):
            manifest = read_json(path)
            if not manifest.get('import_sha256'):
                continue
            origin = results_root.parent / manifest['origin']
            measurements = [{**item, 'source': relative(origin / item['source'], results_root.parent)}
                            for item in manifest.get('evidence', [])]
            context = [{**item, 'source': relative(origin / item['source'], results_root.parent)}
                       for item in manifest.get('context_evidence', [])]
            known[import_signature(measurements, context, manifest.get('parameters', {}))] = path.parent
    return known


def import_results(source, results_root):
    source, results_root = Path(source).resolve(), Path(results_root).resolve()
    if not source.is_dir():
        raise ValueError(f'Import source is not a directory: {source}')
    if source == results_root or source in results_root.parents:
        raise ValueError('Import source must not contain the destination results tree')
    created, existing, failed = 0, 0, []
    known = known_imports(results_root)
    for directory in discover(source):
        try:
            boundary = results_root.parent if directory.is_relative_to(results_root.parent) else source
            report = experiment(directory, source, policy_root=boundary)
            if not report['artifacts'] and not report['paired']:
                continue
            identities = [{**artifact_identity(item), 'source': relative(item['path'], source)} for item in report['artifacts']]
            context = [{'source': relative(path, source), 'sha256': digest_file(path), 'bytes': path.stat().st_size}
                       for path in report['context']]
            canonical_measurements = [{**item, 'source': relative(source / item['source'], results_root.parent)} for item in identities]
            canonical_context = [{**item, 'source': relative(source / item['source'], results_root.parent)} for item in context]
            identity = import_signature(canonical_measurements, canonical_context, compact_metadata(report['metadata']))
            if identity in known:
                existing += 1
                continue
            experiment_path = relative(directory, results_root.parent)
            run_id = 'legacy-' + slug(experiment_path.replace('/', '-')) + '-' + identity[:12]
            kind = 'tests' if report['test_summary'] is not None else 'benchmarks'
            destination = results_root / kind / run_id
            if destination.exists():
                manifest = read_json(destination / 'run.json')
                if manifest.get('import_sha256') != identity:
                    raise ValueError(f'Import ID collision: {run_id}')
                existing += 1
                continue
            # Отчёт сначала собирается отдельно: ошибка не оставляет якобы завершённый импорт.
            staging = results_root / 'local' / 'imports' / (run_id + '-' + uuid.uuid4().hex)
            staging.mkdir(parents=True)
            summary, evidence = publish_experiments([report], staging, source)
            if report['test_summary'] is not None:
                summary.update(report['test_summary'])
            modified = min(path.stat().st_mtime for path in [*[item['path'] for item in report['artifacts']], *report['context']])
            manifest = {'schema_version': SCHEMA, 'id': run_id, 'kind': 'test' if kind == 'tests' else 'benchmark', 'label': report['origin'],
                        'status': 'invalid' if explicitly_invalid(report['metadata']) else 'incomplete' if report['metadata'].get('complete') is False else 'imported',
                        'started_utc': report['metadata'].get('created_utc') or datetime.fromtimestamp(modified, timezone.utc).isoformat(),
                        'time_source': 'manifest' if report['metadata'].get('created_utc') else 'original_file_mtime',
                        'imported_utc': utc_now(),
                        'experiment_path': experiment_path,
                        'import_sha256': identity, 'origin': relative(source, results_root.parent),
                        'source': None, 'exit_code': None, 'parameters': compact_metadata(report['metadata']),
                        'evidence': evidence, 'context_evidence': context}
            atomic_json(staging / 'run.json', manifest)
            atomic_json(staging / 'summary.json', summary)
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.rename(staging, destination)
            known[identity] = destination
            created += 1
        except (OSError, ValueError, KeyError, TypeError) as error:
            failed.append({'path': relative(directory, source), 'error': str(error)})
    if failed:
        atomic_json(results_root / 'local' / 'imports' / 'last-errors.json', failed)
    build_index(results_root)
    return {'imported': created, 'existing': existing, 'errors': failed}


def compare_entries(left, right, left_run, right_run, metric):
    """Comparison gate shared by tests and the generated browser report."""
    reasons = []
    if left_run.get('status') not in ('completed', 'imported') or right_run.get('status') not in ('completed', 'imported'):
        reasons.append('run incomplete or failed')
    for field in ('family', 'case', 'counter_contract'):
        if left.get(field) != right.get(field):
            reasons.append(field + ' differs')
    a = {key: value for key, value in left.get('scenario', {}).items() if key != 'side'}
    b = {key: value for key, value in right.get('scenario', {}).items() if key != 'side'}
    if a != b:
        reasons.append('scenario differs')
    if left.get('evidence') != 'raw' or right.get('evidence') != 'raw':
        reasons.append('raw samples unavailable')
    paired = (left_run.get('id') == right_run.get('id') and left.get('side') and right.get('side') and
              left.get('side') != right.get('side') and left.get('pair_context') and
              left.get('pair_context') == right.get('pair_context') and left.get('experiment') == right.get('experiment'))
    if not paired:
        host_a = (left_run.get('source') or {}).get('host')
        host_b = (right_run.get('source') or {}).get('host')
        if not host_a or host_a != host_b:
            reasons.append('same host not established')
    fixture_a, fixture_b = left.get('fixture'), right.get('fixture')
    if left.get('family') != 'criterion':
        if not fixture_a or not fixture_b or fixture_a != fixture_b:
            reasons.append('fixture differs or unknown')
        elif not fixture_a.get('fingerprint') and not paired:
            reasons.append('fixture content identity unknown')
        if left.get('verified') is not True or right.get('verified') is not True:
            reasons.append('verification unavailable or failed')
    if left.get('family') == 'matrix' and (not a.get('protocol_runtime') or a.get('readers') is None):
        reasons.append('protocol or reader configuration unknown')
    if left.get('family') == 'criterion' and not paired:
        source_a = (left_run.get('source') or {}).get('benchmark_harness_sha256')
        source_b = (right_run.get('source') or {}).get('benchmark_harness_sha256')
        if not source_a or source_a != source_b:
            reasons.append('Criterion fixture/harness identity unknown')
    first = left.get('metrics', {}).get(metric, {})
    second = right.get('metrics', {}).get(metric, {})
    if first.get('unit') != second.get('unit') or first.get('direction') != second.get('direction'):
        reasons.append('metric scope differs')
    if first.get('n') != left.get('sample_count') or second.get('n') != right.get('sample_count'):
        reasons.append('incomplete metric coverage')
    x, y = first.get('mean'), second.get('mean')
    if x is None or y is None or x == 0:
        reasons.append('metric unavailable or zero baseline')
    return {'reasons': reasons, 'ratio': y / x if not reasons else None}


def build_index(results_root):
    results_root = Path(results_root).resolve()
    runs = []
    for kind in ('benchmarks', 'tests'):
        for path in sorted((results_root / kind).glob('*/run.json')):
            try:
                run = read_json(path)
                summary_path = path.with_name('summary.json')
                run['summary'] = read_json(summary_path) if summary_path.is_file() else {'entries': [], 'warnings': []}
                run['path'] = relative(path.parent, results_root)
                log = results_root / 'local' / kind / run['id'] / 'output.log'
                run['log'] = relative(log, results_root) if log.is_file() else None
                runs.append(run)
            except (OSError, ValueError, KeyError, TypeError):
                continue
    runs.sort(key=lambda run: (run.get('started_utc') or '', run.get('id') or ''), reverse=True)
    latest = {}
    for run in runs:
        if run.get('import_sha256'):
            key = run.get('experiment_path') or (Path(run.get('origin', '')) / run.get('label', '')).as_posix()
            if key not in latest or (run.get('imported_utc') or '') > (latest[key].get('imported_utc') or ''):
                latest[key] = run
    for run in runs:
        key = run.get('experiment_path') or (Path(run.get('origin', '')) / run.get('label', '')).as_posix()
        if run.get('import_sha256') and latest[key] is not run:
            run['recorded_status'] = run['status']
            run['status'] = 'superseded'
            run['superseded_by'] = latest[key]['id']
    payload = json.dumps(runs, ensure_ascii=False, allow_nan=False).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
    template = Path(__file__).with_name('results-template.html').read_text(encoding='utf-8')
    page = template.replace('__RESULTS_DATA__', payload).replace('__GENERATED_AT__', html.escape(utc_now()))
    results_root.mkdir(parents=True, exist_ok=True)
    temporary = results_root / ('index.html.tmp-' + uuid.uuid4().hex)
    temporary.write_text(page, encoding='utf-8')
    os.replace(temporary, results_root / 'index.html')
    return len(runs)


def record(args, repo=ROOT):
    results_root = Path(args.results_root).resolve()
    command = args.command[1:] if args.command and args.command[0] == '--' else args.command
    if not command:
        raise ValueError('Supply a command after --')
    kind = 'tests' if args.kind == 'test' else 'benchmarks'
    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ') + '-' + slug(args.label) + '-' + uuid.uuid4().hex[:6]
    local = results_root / 'local' / kind / run_id
    destination = results_root / kind / run_id
    local.mkdir(parents=True, exist_ok=False)
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {'schema_version': SCHEMA, 'id': run_id, 'kind': args.kind, 'label': args.label,
                'suite': args.suite, 'status': 'running', 'started_utc': utc_now(), 'command': command,
                'source': provenance(repo), 'exit_code': None, 'parameters': {}, 'evidence': []}
    if not cargo_bench_from_repository(command, repo):
        # Для произвольного готового бинарника текущий checkout не доказывает версию его harness.
        manifest['source']['benchmark_harness_sha256'] = None
    atomic_json(destination / 'run.json', manifest)
    environment = os.environ.copy()
    environment['FAST_DUMP_RESULTS_RUN_DIR'] = str(local)
    # Явные настройки сохраняются; внешние результаты не импортируем незаметно.
    external_outputs = []
    for name, folder in (('FAST_DUMP_BENCH_MATRIX_DIR', 'matrix'), ('CRITERION_HOME', 'criterion'),
                         ('FAST_DUMP_NETEM_DIR', 'netem')):
        if name in environment:
            external_outputs.append(f'{name}={environment[name]}: explicit output retained; use results.py import PATH if it is outside this run.')
        else:
            environment[name] = str(local / folder)
    command = [str(local / 'output') if value == '{output}' else value for value in command]
    manifest['command'] = command
    manifest['output_environment'] = {key: environment[key] for key in (
        'FAST_DUMP_RESULTS_RUN_DIR', 'FAST_DUMP_BENCH_MATRIX_DIR', 'CRITERION_HOME', 'FAST_DUMP_NETEM_DIR')}
    atomic_json(destination / 'run.json', manifest)
    exit_code, interrupted = 1, False
    try:
        with (local / 'output.log').open('wb') as log:
            process = subprocess.Popen(command, cwd=repo, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            try:
                while True:
                    block = process.stdout.read1(65536)
                    if not block:
                        break
                    log.write(block)
                    log.flush()
                    try:
                        sys.stdout.buffer.write(block)
                        sys.stdout.buffer.flush()
                    except (AttributeError, BrokenPipeError):
                        pass
                exit_code = process.wait()
            except KeyboardInterrupt:
                interrupted = True
                # Продолжаем читать cleanup output, иначе заполненный pipe остановит завершение.
                while True:
                    try:
                        block = process.stdout.read1(65536)
                        if not block:
                            break
                        log.write(block)
                    except KeyboardInterrupt:
                        continue
                exit_code = process.wait()
            finally:
                process.stdout.close()
    except OSError as error:
        (local / 'output.log').write_text(str(error) + '\n', encoding='utf-8')
    manifest['finished_utc'] = utc_now()
    manifest['exit_code'] = exit_code
    manifest['status'] = 'incomplete' if interrupted else 'completed' if exit_code == 0 else 'failed'
    collection_errors = []
    if args.kind == 'benchmark':
        reports = []
        warnings = list(external_outputs)
        candidates = discover(local)
        if not candidates:
            candidates = [local]
        for path in candidates:
            try:
                report = experiment(path, local, fallback_log=local / 'output.log' if len(candidates) == 1 else None)
                if report['artifacts'] or report['paired']:
                    reports.append(report)
            except (OSError, ValueError, KeyError, TypeError) as error:
                collection_errors.append(f'{relative(path, local)}: {error}')
        try:
            summary, evidence = publish_experiments(reports, destination, local)
            enrich_recorded_entries(summary, manifest, local, command, repo)
        except (OSError, ValueError, KeyError, TypeError) as error:
            collection_errors.append(f'Could not collect measurements: {error}')
            summary, evidence = {'schema_version': SCHEMA, 'entries': [], 'warnings': []}, []
        warnings.extend(collection_errors)
        summary['warnings'].extend(warnings)
        if not summary['entries']:
            summary['warnings'].append('No supported measurements found. Inspect the local output log.')
        manifest['parameters'] = [compact_metadata(report['metadata']) for report in reports]
        manifest['evidence'] = evidence
        if exit_code == 0 and any(explicitly_invalid(report['metadata']) for report in reports):
            manifest['status'] = 'invalid'
        if any(report['metadata'].get('complete') is False for report in reports):
            collection_errors.append('One or more experiment manifests are incomplete.')
            summary['warnings'].append(collection_errors[-1])
    else:
        text = (local / 'output.log').read_text(encoding='utf-8', errors='replace')
        counts = [{'status': match[0], 'passed': int(match[1]), 'failed': int(match[2]), 'ignored': int(match[3])}
                  for match in re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)]
        python_count = re.search(r'Ran (\d+) tests? in', text)
        summary = {'schema_version': SCHEMA, 'command': command, 'exit_code': exit_code,
                   'rust_test_suites': counts, 'python_tests': int(python_count[1]) if python_count else None,
                   'entries': [], 'warnings': []}
    if collection_errors:
        manifest['collection_errors'] = collection_errors
        if exit_code == 0:
            manifest['status'] = 'incomplete'
    atomic_json(destination / 'summary.json', summary)
    atomic_json(destination / 'run.json', manifest)
    build_index(results_root)
    print(f'\n[results] {manifest["status"]}: {destination}', flush=True)
    return 130 if interrupted else exit_code or (2 if collection_errors else 0)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results-root', type=Path, default=ROOT / 'results')
    subcommands = parser.add_subparsers(dest='action', required=True)
    runner = subcommands.add_parser('record', help='Run a command and automatically retain compact results')
    runner.add_argument('--kind', choices=('test', 'benchmark'), required=True)
    runner.add_argument('--label', required=True)
    runner.add_argument('--suite')
    runner.add_argument('command', nargs=argparse.REMAINDER)
    importer = subcommands.add_parser('import', help='Import existing metrics without modifying their source')
    importer.add_argument('path', type=Path)
    subcommands.add_parser('index', help='Rebuild results/index.html for file:// viewing')
    args = parser.parse_args(argv)
    if args.action == 'record':
        return record(args)
    if args.action == 'import':
        report = import_results(args.path, args.results_root)
        print(json.dumps(report, ensure_ascii=False))
        return 1 if report['errors'] else 0
    count = build_index(args.results_root)
    print(f'[results] indexed {count} runs: {args.results_root / "index.html"}')
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f'results: {error}', file=sys.stderr)
        raise SystemExit(2)
