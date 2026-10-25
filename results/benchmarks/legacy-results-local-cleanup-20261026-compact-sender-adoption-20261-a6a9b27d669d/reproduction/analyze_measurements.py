"""Analyze independent paired experiments without mixing measurement scopes."""
import argparse
import csv
import hashlib
import json
import math
import random
import re
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SEED = 20261025
BOOTSTRAPS = 10_000
KV = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)')
FLAGS = ('--threads', '--readers', '--before-readers', '--after-readers',
         '--frames', '--buffers', '--executor', '--network-profile', '--stages',
         '--case', '--iterations')
ALLOCATION_FIELDS = ('allocs', 'alloc_bytes', 'reallocs', 'deallocs', 'alloc_successes',
    'alloc_success_bytes', 'realloc_successes', 'realloc_success_bytes',
    'alloc_failures', 'realloc_failures', 'dealloc_bytes', 'zeroed_bytes')


def number(value):
    if value is None or str(value).lower() in ('', 'null', 'none', 'unmeasured'):
        return None
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f'non-finite metric: {value}')
    return result


def scaled(value, divisor):
    parsed = number(value)
    return None if parsed is None else parsed / divisor


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def read_csv(path):
    with path.open(newline='', encoding='utf-8-sig') as stream:
        rows = list(csv.DictReader(stream))
    if any(None in row or None in row.values() for row in rows):
        raise ValueError(f'malformed CSV: {path}')
    return rows


def command_config(command):
    result = {}
    for flag in FLAGS:
        if flag in command:
            result[flag[2:]] = command[command.index(flag) + 1]
    result['allocations'] = '--allocations' in command
    return result


def tagged(log, tag):
    return [dict(KV.findall(line)) for line in log.splitlines()
            if line.startswith('[' + tag + '] ')]


def resource_data(directory, log):
    rows = read_csv(directory / 'resource_samples.csv') if (directory / 'resource_samples.csv').exists() else []
    if len(rows) > 1:
        raise ValueError('expected one resource sample per custom child')
    saved = rows[0] if rows else {}
    result = {'saved': saved}
    for tag in ('bench-work', 'bench-io', 'bench-link', 'bench-receiver-work', 'bench-commit'):
        values = tagged(log, tag)
        if len(values) > 1:
            raise ValueError(f'expected one {tag} record per custom child')
        result[tag] = values[0] if values else {}
    return result


def e2e_observation(directory):
    rows = read_csv(directory / 'results_raw.csv')
    if len(rows) != 1:
        raise ValueError('expected exactly one custom-fixture result row')
    row = rows[0]
    log_path = directory / 'run.log'
    log = log_path.read_text(encoding='utf-8', errors='replace')
    resources = resource_data(directory, log)
    work, saved = resources['bench-work'], resources['saved']
    cpu_scope = work.get('scope', saved.get('cpu_scope'))
    if cpu_scope not in (None, 'sender_receiver_process'):
        raise ValueError(f'unexpected E2E CPU scope: {cpu_scope}')
    metrics = {
        'elapsed_ms': number(row.get('total_duration_ms')),
        'first_bundle_ms': number(row.get('time_to_first_frame_ms')),
        'user_ms': scaled(work.get('user_ns', saved.get('cpu_user_ns')), 1e6),
        'kernel_ms': scaled(work.get('kernel_ns', saved.get('cpu_kernel_ns')), 1e6),
        'cpu_cycles': number(work.get('cpu_cycles', saved.get('cpu_cycles'))),
        'rss_mib': number(row.get('max_rss_mib')) or None,
        'private_mib': number(row.get('max_private_mib')) or None,
        'archive_bytes': None,
    }
    metrics['cpu_ms'] = (metrics['user_ms'] + metrics['kernel_ms']
                         if metrics['user_ms'] is not None and metrics['kernel_ms'] is not None else None)
    for field in ('compressed_payload_bytes', 'wire_bytes', 'raw_read_bytes', 'tar_input_bytes',
                  'files_count', 'skipped_files', 'frame_aborts', 'frame_rebuilds', 'sent_frames',
                  'read_duration_ms', 'compress_duration_ms', 'send_duration_ms', 'ack_wait_ms'):
        metrics[field] = number(row.get(field))
    for field in ('tx_bytes', 'rx_bytes', 'logical_bytes'):
        metrics[field] = number(resources['bench-link'].get(field, saved.get(field)))
    for field in ('read_ops', 'write_ops', 'other_ops', 'read_bytes', 'write_bytes', 'other_bytes', 'page_faults'):
        metrics['io_' + field] = number(resources['bench-io'].get(field, saved.get('io_' + field)))
    identity = {field: row.get(field) for field in ('files_count', 'raw_read_bytes')}
    config = {field: row.get(field) for field in (
        'dataset', 'protocol_runtime', 'network_profile', 'executor', 'threads',
        'frame_target_mb', 'read_buffer_kb', 'large_file_single_frame_threshold_mib')}
    verified = re.findall(r'^\[bench-verify\] verified (\d+) entries, (\d+) regular-file bytes', log, re.M)
    errors = []
    if len(verified) != 1 or len(tagged(log, 'bench-link')) != 1:
        errors.append('missing or duplicate archive verification/link records')
    elif tuple(map(int, verified[0])) != (int(row['files_count']), int(row['raw_read_bytes'])):
        errors.append('archive verification counts disagree with CSV')
    if any(number(row.get(field)) != 0 for field in ('skipped_files', 'frame_aborts', 'frame_rebuilds')):
        errors.append('static fixture has skips/aborts/rebuilds or missing counters')
    if metrics['elapsed_ms'] is None or metrics['elapsed_ms'] <= 0:
        errors.append('missing/nonpositive elapsed time')
    return {'metrics': metrics, 'raw': row, 'resources': resources, 'identity': identity,
            'config': config, 'contract': {'cpu_scope': cpu_scope,
                'io_source': resources['bench-io'].get('source', saved.get('io_source'))},
            'errors': errors, 'log': str(log_path)}


def stage_observation(row):
    metrics = {'elapsed_ms': scaled(row.get('elapsed_ns'), 1e6),
               'rss_mib': scaled(row.get('process_peak_rss_bytes'), 1024 ** 2)}
    for name in ('cpu', 'user', 'kernel', 'work', 'drop'):
        metrics[name + '_ms'] = scaled(row.get(name + '_ns'), 1e6)
    for name in ('cpu_cycles', 'io_read_ops', 'io_write_ops', 'io_other_ops', 'io_read_bytes',
                 'io_write_bytes', 'io_other_bytes', 'storage_read_bytes', 'storage_write_bytes',
                 'io_read_ops_raw', 'io_read_bytes_raw', 'io_observer_read_ops', 'io_observer_read_bytes',
                 'page_faults', 'minor_faults', 'major_faults', 'voluntary_switches', 'involuntary_switches',
                 'entries', 'files', 'bytes', 'iterations', 'entries_per_message',
                 'symlinks_per_message', 'packet_bytes', 'body_bytes', *ALLOCATION_FIELDS):
        metrics[name] = number(row.get(name))
    operations = number(row.get('iterations')) if row.get('stage') == 'control' else 1
    metrics['operations'] = operations
    for name in ('elapsed_ms', 'cpu_ms', 'user_ms', 'kernel_ms', 'cpu_cycles', *ALLOCATION_FIELDS):
        for denominator, value in [('entry', metrics['entries']), ('operation', operations)]:
            metrics[f'{name}_per_{denominator}'] = (metrics[name] / value
                if metrics[name] is not None and value is not None and value > 0 else None)
    errors = []
    if str(row.get('verified')).lower() != 'true':
        errors.append('stage sample is not verified')
    if metrics['elapsed_ms'] is None or metrics['elapsed_ms'] <= 0:
        errors.append('missing/nonpositive elapsed time')
    return {'metrics': metrics, 'raw': row,
            'identity': {key: row.get(key) for key in ('entries', 'files', 'bytes', 'fingerprint',
                         'entries_per_message', 'symlinks_per_message', 'packet_bytes', 'body_bytes')},
            'config': {key: row.get(key) for key in ('stage', 'readers', 'case', 'iterations',
                       'allocator_instrumented', 'allocations_enabled')},
            'contract': {key: row.get(key) for key in ('counters_schema', 'cycle_source', 'io_source', 'scope', 'interval')},
            'errors': errors, 'log': row.get('log')}


def load_run(root, label):
    directory = root / 'measurements' / label
    report = {'label': label, 'observations': [], 'errors': [], 'complete': False, 'valid_evidence': False}
    try:
        metadata = read_json(root / 'driver-logs' / (label + '.json'))
        report['metadata'] = metadata
        comparison = metadata.get('comparison_id')
        if comparison not in ('m2', 'm3-control', 'm3-control-confirm'):
            report['errors'].append('missing or unexpected incremental comparison_id')
        else:
            provenance_path = root / 'provenance' / (comparison + '.json')
            provenance = read_json(provenance_path)
            if hashlib.sha256(provenance_path.read_bytes()).hexdigest() != metadata.get('provenance_sha256'):
                report['errors'].append('pinned provenance hash disagrees with driver')
            if metadata.get('accepted_head') != provenance.get('accepted_head'):
                report['errors'].append('accepted baseline commit differs from pinned provenance')
            source_hashes = dict(provenance['source_hashes'])
            if metadata.get('binary_hashes', {}).get('before') == metadata.get('binary_hashes', {}).get('after'):
                source_hashes['after'] = source_hashes['before']
            if metadata.get('source_hashes') != source_hashes:
                report['errors'].append('source identities differ from pinned provenance')
        if metadata.get('exit_code') != 0:
            report['errors'].append('driver missing successful exit status')
        manifest = read_json(directory / 'paired-run.json')
        report['manifest'] = manifest
        report['complete'] = manifest.get('complete') is True
        if not report['complete']:
            report['errors'].append('paired driver did not complete')
        if metadata.get('binary_hashes') != manifest.get('binary_sha256'):
            report['errors'].append('driver/paired-manifest binary hashes disagree')
        stage = metadata['stage']
        expected_scope = 'sender_receiver_process' if stage == 'e2e' else 'isolated_sender_stage'
        if metadata.get('scope') != expected_scope:
            report['errors'].append('unexpected driver metric scope')
        for invocation in manifest.get('invocations', []):
            if stage == 'e2e' and invocation.get('exit_code') != 0:
                report['errors'].append('child invocation missing successful exit status')
            elif stage != 'e2e' and invocation.get('complete') is not True:
                report['errors'].append('stage invocation did not complete')
        if stage == 'e2e':
            for child in sorted((directory / 'runs').glob('*')):
                match = re.fullmatch(r'(\d+)-(before|after)', child.name)
                if not child.is_dir() or not match:
                    continue
                try:
                    observation = e2e_observation(child)
                    observation.update(pair=int(match[1]), side=match[2])
                    report['observations'].append(observation)
                except (OSError, ValueError, KeyError, TypeError) as error:
                    report['errors'].append(f'{child.name}: {error}')
        else:
            # Незавершённый driver ещё не записывает samples.csv: сохраняем его
            # уже завершённые measurements из manifest, но не считаем серию валидной.
            rows = (read_csv(directory / 'samples.csv') if (directory / 'samples.csv').exists()
                    else [{**invocation.get('measurement', {}),
                           **{key: invocation.get(key) for key in ('pair', 'side', 'stage', 'readers', 'log')}}
                          for invocation in manifest.get('invocations', []) if invocation.get('complete')])
            for row in rows:
                observation = stage_observation(row)
                if any(str(row.get(key, '')).lower() not in ('true', 'false')
                       for key in ('allocator_instrumented', 'allocations_enabled')):
                    observation['errors'].append('missing or unknown allocation instrumentation flags')
                instrumented = str(row.get('allocator_instrumented')).lower() == 'true'
                enabled = str(row.get('allocations_enabled')).lower() == 'true'
                if instrumented != metadata.get('allocations', False) or enabled != instrumented:
                    observation['errors'].append('driver/sample allocation instrumentation mismatch')
                if stage == 'control':
                    if row.get('case') != metadata['fixture'] or row.get('stage') != 'control':
                        observation['errors'].append('control case/stage differs from driver')
                    if row.get('scope') != 'control_encode_process' or row.get('interval') != 'iterations_only':
                        observation['errors'].append('unexpected control counter scope')
                observation.update(pair=int(row['pair']), side=row['side'])
                report['observations'].append(observation)
        for observation in report['observations']:
            report['errors'].extend(f"pair{observation['pair']} {observation['side']}: {error}"
                                    for error in observation['errors'])
        repetitions = int(manifest.get('repeats_per_side', manifest.get('repeats', 0)))
        observed = [(item['pair'], item['side']) for item in report['observations']]
        expected = [(pair, side) for pair in range(1, repetitions + 1) for side in ('before', 'after')]
        if repetitions <= 0 or sorted(observed) != sorted(expected):
            report['errors'].append('missing/duplicate/unexpected pair identities')
        signatures = {json.dumps({key: item[key] for key in ('identity', 'config', 'contract')}, sort_keys=True)
                      for item in report['observations']}
        if len(signatures) != 1:
            report['errors'].append('fixture/config/counter contract changed within series')
        if report['observations']:
            item = report['observations'][0]
            report['configuration'] = {'command': command_config(metadata['command']),
                'scenario': item['config'], 'identity': item['identity'], 'counter_contract': item['contract']}
        hashes = metadata.get('binary_hashes', {})
        if not hashes.get('before') or not hashes.get('after'):
            report['errors'].append('binary hash unavailable')
        report['comparison_kind'] = 'A/A' if hashes.get('before') == hashes.get('after') else 'A/B'
        if report['comparison_kind'] == 'A/A' and manifest.get('comparison_kind') != 'A/A':
            report['errors'].append('identical binaries not declared as A/A')
        report['valid_evidence'] = not report['errors']
        report['timing_evidence'] = report['valid_evidence'] and not metadata.get('allocations', False)
    except (OSError, ValueError, KeyError, TypeError) as error:
        report['errors'].append(str(error))
    return report


def percentile(values, fraction):
    position = (len(values) - 1) * fraction
    low, high = math.floor(position), math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


@lru_cache(maxsize=32)
def resample_indices(count, iterations):
    randomizer = random.Random(SEED)
    return tuple(tuple(randomizer.randrange(count) for _ in range(count)) for _ in range(iterations))


def paired_summary(pairs, iterations=BOOTSTRAPS):
    values = [(before, after) for before, after in pairs if before is not None and after is not None]
    result = {'paired_n': len(values), 'missing_pairs': len(pairs) - len(values)}
    if not values:
        return result
    before, after = zip(*values)
    result.update(before_mean=statistics.mean(before), after_mean=statistics.mean(after),
                  before_median=statistics.median(before), after_median=statistics.median(after),
                  before_min=min(before), before_max=max(before), after_min=min(after), after_max=max(after),
                  after_wins=sum(right < left for left, right in values))
    result['absolute_reduction'] = result['before_mean'] - result['after_mean']
    result['paired_reductions'] = [left - right for left, right in values]
    if result['before_mean'] <= 0:
        result.update(improvement_pct=None, ci95_improvement_pct=None)
        return result
    result['improvement_pct'] = 100 * (1 - result['after_mean'] / result['before_mean'])
    if len(values) < 2:
        result['ci95_improvement_pct'] = None
        return result
    if all(left == before[0] and right == after[0] for left, right in values):
        result['ci95_improvement_pct'] = [result['improvement_pct']] * 2
        return result
    draws = []
    for indices in resample_indices(len(values), iterations):
        sampled = [values[index] for index in indices]
        denominator = sum(pair[0] for pair in sampled)
        if denominator > 0:
            draws.append(100 * (1 - sum(pair[1] for pair in sampled) / denominator))
    draws.sort()
    result['ci95_improvement_pct'] = [percentile(draws, 0.025), percentile(draws, 0.975)] if draws else None
    return result


def stable_key(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def summarize_groups(runs):
    collected = defaultdict(list)
    for run in runs:
        if not run['valid_evidence']:
            continue
        metadata = run['metadata']
        identity = {key: metadata[key] for key in ('candidate', 'fixture', 'stage', 'scope')}
        identity.update({key: metadata.get(key) for key in ('comparison_id', 'accepted_head', 'source_hashes')})
        identity['allocations_diagnostic'] = metadata.get('allocations', False)
        identity.update(binary_hashes=metadata['binary_hashes'], configuration=run['configuration'])
        collected[stable_key(identity)].append(run)
    groups = []
    for key, members in sorted(collected.items()):
        group = json.loads(key)
        group.update(labels=[run['label'] for run in members], comparison_kind=members[0]['comparison_kind'])
        pairs = []
        for run in members:
            by_pair = defaultdict(dict)
            for item in run['observations']:
                by_pair[item['pair']][item['side']] = item
            pairs.extend({'label': run['label'], 'pair': pair, **sides} for pair, sides in sorted(by_pair.items()))
        metric_names = sorted({name for pair in pairs for side in ('before', 'after') for name in pair[side]['metrics']})
        group['pairs'] = [{'label': pair['label'], 'pair': pair['pair']} for pair in pairs]
        group['metrics'] = {name: paired_summary([(pair['before']['metrics'].get(name), pair['after']['metrics'].get(name))
                                                  for pair in pairs]) for name in metric_names}
        midpoint = len(pairs) // 2
        group['fixed_order_halves'] = []
        for title, subset in [('first', pairs[:midpoint]), ('second', pairs[midpoint:])]:
            group['fixed_order_halves'].append({'half': title, 'pairs': len(subset),
                'metrics': {name: paired_summary([(pair['before']['metrics'].get(name), pair['after']['metrics'].get(name))
                                                 for pair in subset])
                            for name in ('elapsed_ms', 'cpu_cycles', 'alloc_successes', 'realloc_successes')}})
        group['calibration_key'] = stable_key({
            key: group[key] for key in ('fixture', 'stage', 'scope', 'configuration', 'allocations_diagnostic')
        } | {'baseline_hash': group['binary_hashes']['before'],
             'comparison_id': group.get('comparison_id'), 'accepted_head': group.get('accepted_head'),
             'baseline_source_hash': (group.get('source_hashes') or {}).get('before')})
        groups.append(group)
    calibrations = defaultdict(list)
    for group in groups:
        if group['comparison_kind'] == 'A/A':
            for summary in group['metrics'].values():
                interval = summary.get('ci95_improvement_pct')
                summary['noise_floor_pct'] = max(map(abs, interval)) if interval else None
            calibrations[group['calibration_key']].append(group)
    for group in groups:
        matches = calibrations.get(group['calibration_key'], [])
        group['calibration_labels'] = [label for item in matches for label in item['labels']]
        if group['comparison_kind'] == 'A/A':
            group['provisional_verdict'] = 'calibration'
            continue
        for name, summary in group['metrics'].items():
            noise = [item['metrics'].get(name, {}).get('noise_floor_pct') for item in matches]
            summary['aa_noise_floor_pct'] = max((value for value in noise if value is not None), default=None)
        if group['allocations_diagnostic']:
            # Instrumentation меняет тайминги: из этой группы принимаются только
            # наблюдаемые операции allocator, никогда speed/CPU verdict.
            reduced = [name for name in ('alloc_successes', 'realloc_successes', 'alloc_success_bytes', 'realloc_success_bytes')
                       if group['metrics'].get(name, {}).get('absolute_reduction', 0) > 0]
            group['allocation_reduction_metrics'] = reduced
            group['provisional_verdict'] = 'allocation_reduction_observed' if reduced else 'no_allocation_reduction_observed'
            group['timing_evidence'] = False
            continue
        group['timing_evidence'] = True
        time = group['metrics']['elapsed_ms']
        gain, interval, noise = (time.get('improvement_pct'), time.get('ci95_improvement_pct'), time.get('aa_noise_floor_pct'))
        def clear_gain(metric):
            value = group['metrics'].get(metric, {})
            change, ci, floor = value.get('improvement_pct'), value.get('ci95_improvement_pct'), value.get('aa_noise_floor_pct')
            return change is not None and ci is not None and floor is not None and change > 0 and ci[0] > floor
        regressions = []
        for name in ('elapsed_ms', 'cpu_ms', 'cpu_cycles', 'rss_mib'):
            value = group['metrics'].get(name, {})
            change, ci, floor = value.get('improvement_pct'), value.get('ci95_improvement_pct'), value.get('aa_noise_floor_pct')
            if change is not None and ci is not None and floor is not None and ci[1] < -floor:
                regressions.append(name)
        group['confirmed_regression_metrics'] = regressions
        conditional = [name for name in ('first_bundle_ms', 'cpu_ms', 'cpu_cycles') if clear_gain(name)]
        group['conditional_metrics'] = conditional
        if regressions:
            verdict = 'regression_above_aa_requires_rejection_or_review'
        elif clear_gain('elapsed_ms'):
            verdict = 'works_provisional'
        elif conditional:
            verdict = 'conditional_startup_or_cpu_only'
        else:
            verdict = 'inconclusive'
        group['provisional_verdict'] = verdict
    for group in groups:
        del group['calibration_key']
    return groups


def allocation_expectation_comparisons(root, groups):
    path = root / 'allocation-expectations.json'
    if not path.exists():
        return {'available': False}
    expectations = read_json(path)
    comparisons = []
    for group in groups:
        if not group['allocations_diagnostic'] or group['stage'] != 'scan':
            continue
        fixture = expectations['fixtures'].get(group['fixture'], {})
        expected = fixture.get('expected_alloc_success_reduction_per_scan', {}).get(group['candidate'])
        if expected is None:
            continue
        observed = group['metrics'].get('alloc_successes', {})
        reductions = observed.get('paired_reductions', [])
        comparisons.append({'candidate': group['candidate'], 'fixture': group['fixture'], 'labels': group['labels'],
            'expected_reduction_per_scan': expected, 'observed_mean_reduction_per_scan': observed.get('absolute_reduction'),
            'paired_reductions': reductions,
            'every_pair_matches_theoretical_count': bool(reductions) and all(value == expected for value in reductions),
            'interpretation': 'Other runtime allocations and scheduling may vary; report any mismatch rather than treating the topology prediction as measured evidence.'})
    return {'available': True, 'source': expectations, 'comparisons': comparisons}


def self_test():
    equal = paired_summary([(10, 10), (20, 20), (30, 30)])
    assert equal['improvement_pct'] == 0 and equal['ci95_improvement_pct'] == [0, 0]
    proportional = paired_summary([(10, 9), (20, 18), (30, 27)])
    assert abs(proportional['improvement_pct'] - 10) < 1e-10
    assert all(abs(value - 10) < 1e-10 for value in proportional['ci95_improvement_pct'])
    assert paired_summary([(10, 11), (20, 22)])['ci95_improvement_pct'][1] < 0
    assert paired_summary([(0, 1), (0, 2)])['improvement_pct'] is None
    assert paired_summary([(10, None), (None, 9)])['paired_n'] == 0
    assert paired_summary([(10, 9)])['ci95_improvement_pct'] is None
    assert paired_summary([(10, 8), (20, 18)]) == paired_summary([(10, 8), (20, 18)])
    assert number('unmeasured') is None and number('0') == 0
    row = {'elapsed_ns': '2000000', 'cpu_ns': '3000000', 'user_ns': '1000000',
           'kernel_ns': '2000000', 'process_peak_rss_bytes': '1048576', 'verified': 'True'}
    stage = stage_observation(row)
    assert stage['metrics']['elapsed_ms'] == 2 and stage['metrics']['cpu_ms'] == 3
    assert stage['metrics']['rss_mib'] == 1 and not stage['errors']
    def toy_run(label, candidate, after_hash, after_value):
        return {'valid_evidence': True, 'label': label,
                'metadata': {'candidate': candidate, 'fixture': 'toy', 'stage': 'read',
                             'scope': 'isolated_sender_stage', 'binary_hashes': {'before': 'A', 'after': after_hash}},
                'configuration': {'fixture': 'same'}, 'comparison_kind': 'A/A' if after_hash == 'A' else 'A/B',
                'observations': [{'pair': pair, 'side': side, 'metrics': {'elapsed_ms': value}}
                                 for pair in (1, 2) for side, value in (('before', 100), ('after', after_value))]}
    groups = summarize_groups([toy_run('aa', 'baseline', 'A', 100),
                               toy_run('primary', 'candidate', 'B', 90),
                               toy_run('confirm', 'candidate', 'B', 90),
                               toy_run('different_binary', 'candidate', 'C', 90)])
    assert len(groups) == 3
    pooled = next(group for group in groups if group['binary_hashes']['after'] == 'B')
    assert pooled['metrics']['elapsed_ms']['paired_n'] == 4
    assert pooled['provisional_verdict'] == 'works_provisional'
    assert pooled['calibration_labels'] == ['aa']
    control = stage_observation({'stage': 'control', 'iterations': '10', 'entries': '20',
        'bytes': '500', 'elapsed_ns': '1000000', 'cpu_ns': '1000000', 'user_ns': '1000000',
        'kernel_ns': '0', 'alloc_successes': '30', 'verified': 'true'})
    assert control['metrics']['alloc_successes_per_operation'] == 3
    assert control['metrics']['alloc_successes_per_entry'] == 1.5
    diagnostic = toy_run('alloc', 'candidate', 'B-alloc', 1)
    diagnostic['metadata']['allocations'] = True
    for row in diagnostic['observations']:
        row['metrics']['alloc_successes'] = 100 if row['side'] == 'before' else 80
    only = summarize_groups([diagnostic])[0]
    assert only['provisional_verdict'] == 'allocation_reduction_observed'
    assert only['timing_evidence'] is False
    print('toy-array self-tests passed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    root = args.root.resolve()
    labels = {path.stem for path in (root / 'driver-logs').glob('*.json')}
    labels.update(path.name for path in (root / 'measurements').glob('*') if path.is_dir())
    runs = [load_run(root, label) for label in sorted(labels)]
    groups = summarize_groups(runs)
    result = {'schema': 2, 'generated_utc': datetime.now(timezone.utc).isoformat(),
        'method': {'bootstrap': 'percentile paired bootstrap, whole before/after pairs resampled together',
                   'iterations': BOOTSTRAPS, 'seed': SEED, 'positive_improvement': '100 * (1 - after_mean / before_mean)',
                   'noise_floor': 'max(abs(A/A CI95 endpoints)), matched fixture/stage/config/baseline/counter scope',
                   'review_signal_gate': 'Strict gate carried forward unchanged from the prior independently reviewed micro experiment and fixed before adoption measurements: gain CI95 lower must exceed the matched A/A floor; regression CI95 upper must be below its negative. A point estimate alone cannot pass. This is a descriptive conservative gate, not a calibrated joint hypothesis test.',
                   'retention': 'all observations retained; incomplete/failed series excluded; no trimming or warmup removal',
                   'confirmations': 'pool complete labels only with identical comparison_id, accepted_head, source/binary hashes, config, fixture identity and counter contract',
                   'allocation_diagnostics': 'Instrumented time/CPU are retained but cannot establish speed or regression. Counts can establish saved work; successful alloc/realloc counts are distinct.',
                   'acceptance': 'Useful reproducible allocation/CPU reduction with simple implementation; E2E speedup is not required. Confirmed uninstrumented regressions above matching A/A need rejection or explicit review.',
                   'fixed_order_halves': 'Descriptive first/second halves by pair number within each label; pooled labels use lexical label order, not necessarily chronology. Current groups each contain one label. These are not independently run or selected confirmation suites.',
                   'inference': 'exploratory descriptive intervals; no multiple-comparison correction; human final verdict required'},
        'scope_notes': ['E2E CPU and memory cover combined sender+receiver process, not sender-only.',
            'Stage CPU excludes preparation/verification; stage RSS is process lifetime highwater, not phase-only.',
            'Control wall/allocator intervals cover only the iterations loop; CPU/cycle snapshots are slightly wider and include measurement hooks/instrumentation. Noop is reported without subtraction.',
            'Control preparation encodes/decodes a reference before timing and warms code/allocator state. RSS includes fixture/reference preparation. This is not cold-start or socket timing.',
            'Control case, iterations, packet/body byte counts and fingerprint must match across binaries; no pooling across allocator flags or cases.',
            'first_bundle_ms measures BundlePlan readiness, not first payload delivery or acknowledgement.',
            'compressed_payload_bytes is not exact archive size; archive_bytes stays unknown without direct evidence.',
            'Stage durations overlap and must not be added as a total-time decomposition.',
            'Provisional works does not establish platform portability or absence of regressions on other fixtures.'],
        'runs': runs, 'groups': groups,
        'allocation_expectations': allocation_expectation_comparisons(root, groups)}
    manifest = root / 'fixtures.json'
    result['fixture_manifest_sha256'] = hashlib.sha256(manifest.read_bytes()).hexdigest() if manifest.exists() else None
    output = root / 'analysis.json'
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    for group in groups:
        if group['allocations_diagnostic']:
            counts = group['metrics'].get('alloc_successes_per_operation', {})
            reallocs = group['metrics'].get('realloc_successes_per_operation', {})
            print(f"{group['candidate']:20} {group['fixture']:16} {group['stage']:7} DIAGNOSTIC "
                  f"alloc/op={counts.get('before_mean')}->{counts.get('after_mean')} "
                  f"realloc/op={reallocs.get('before_mean')}->{reallocs.get('after_mean')} "
                  f"{group['provisional_verdict']}; timing not interpreted")
            continue
        timing = group['metrics']['elapsed_ms']
        before, after = timing.get('before_mean'), timing.get('after_mean')
        gain = timing.get('improvement_pct')
        change = f'{gain:+.2f}%' if gain is not None else 'unknown'
        print(f"{group['candidate']:20} {group['fixture']:16} {group['stage']:5} n={timing['paired_n']:2} "
              f"{before:.3f}->{after:.3f} ms gain={change} CI={timing.get('ci95_improvement_pct')} "
              f"{group['provisional_verdict']}")
    for run in runs:
        if not run['valid_evidence']:
            print('EXCLUDED', run['label'], '; '.join(run['errors']))
    print(output)


if __name__ == '__main__':
    main()
