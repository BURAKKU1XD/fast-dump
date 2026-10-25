"""Package adoption evidence locally; root owns decisions, commits and results import."""
import argparse
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import re
import shutil

import run_measurements as driver
import run_control_confirmation as confirmation

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def identity(path):
    return {'path': path.relative_to(ROOT).as_posix(), 'sha256': driver.sha(path), 'bytes': path.stat().st_size}


def checks_summary():
    groups, failures = {}, []
    for path in sorted((ROOT / 'checks').rglob('checks.json')):
        original = load(path)
        records = []
        for item in original if isinstance(original, list) else [original]:
            row = dict(item)
            log = path.parent / (str(row.get('check', 'build')) + '.log')
            if log.exists():
                text = log.read_text(encoding='utf-8', errors='replace')
                row['log_evidence'] = identity(log)
                row['rust_suite_counts'] = [dict(zip(('passed', 'failed', 'ignored'), map(int, counts)))
                    for counts in re.findall(r'test result: .*? (\d+) passed; (\d+) failed; (\d+) ignored', text)]
                if row.get('exit_code') != 0:
                    row['failure_excerpt'] = text[-8000:]
            if row.get('exit_code') != 0:
                failures.append({'source': path.relative_to(ROOT).as_posix(), **row})
            records.append(row)
        group = {'source': identity(path), 'observations': records}
        environment = path.parent / 'environment.json'
        if environment.exists():
            group['environment'] = load(environment)
        groups[path.relative_to(ROOT / 'checks').as_posix()] = group
    return groups, failures


def final_ready(analysis, decisions):
    expected = {item[0] for suite in ('m2', 'm3-control') for item in driver.cases(suite)}
    expected.update(item[0] for item in confirmation.cases())
    runs = {run['label']: run for run in analysis['runs']}
    if set(runs) != expected:
        raise ValueError('expected exactly 16 planned series; missing/extra: ' + repr(sorted(set(runs) ^ expected)))
    invalid = [run['label'] for run in analysis['runs'] if not run['valid_evidence']]
    if invalid:
        raise ValueError('incomplete/invalid series: ' + repr(invalid))
    if sum(len(run['observations']) for run in analysis['runs']) != 200:
        raise ValueError('expected 200 retained observations')
    if {item.get('candidate') for item in decisions.get('decisions', [])} != {'m1', 'm2', 'm3'}:
        raise ValueError('adoption-decisions.json needs m1, m2 and m3 decisions')
    if not decisions.get('conclusion') or any(not item.get('verdict') or not item.get('evidence')
                                           for item in decisions['decisions']):
        raise ValueError('human conclusion, verdict and evidence are required')
    analyzed = (ROOT / 'analysis.json').stat().st_mtime_ns
    for path in (ROOT / 'driver-logs').glob('*.json'):
        if path.stat().st_mtime_ns > analyzed:
            raise ValueError('analysis predates finished driver: ' + str(path))
    for path in (ROOT / 'measurements').rglob('*'):
        if path.is_file() and path.name in ('samples.csv', 'paired-run.json', 'results_raw.csv', 'run.log') and path.stat().st_mtime_ns > analyzed:
            raise ValueError('measurement evidence newer than analysis: ' + str(path))


def verified_provenance(analysis):
    result = {}
    specs = load(ROOT / 'comparison-specs.json')
    for name in sorted({run['metadata']['comparison_id'] for run in analysis['runs']}):
        path = ROOT / 'provenance' / (name + '.json')
        saved = load(path)
        current = (confirmation.preflight(load(ROOT / 'control-confirmation-spec.json'))
                   if name == confirmation.COMPARISON else driver.preflight(name, specs[name]))
        if current != saved:
            raise ValueError('pinned source/compiler/binary/driver provenance changed: ' + name)
        result[name] = {'evidence': identity(path), 'accepted_head': saved['accepted_head'],
            'source_hashes': saved['source_hashes'], 'artifacts': saved['artifacts'],
            'compiler': saved['compiler'], 'compiler_binary': saved['compiler_binary'],
            'scripts': saved['scripts'], 'driver_sha256': saved['driver_sha256']}
    for run in analysis['runs']:
        metadata = run['metadata']
        pin = result[metadata['comparison_id']]
        if pin['evidence']['sha256'] != metadata['provenance_sha256']:
            raise ValueError('observation provenance hash mismatch: ' + run['label'])
        mode = 'allocation' if metadata['allocations'] else 'timing'
        executable = {'e2e': 'loopback_send_dump', 'scan': 'file_pipeline', 'control': 'control_messages'}[metadata['stage']]
        sides = ('before', 'before') if run['comparison_kind'] == 'A/A' else ('before', 'after')
        expected = {side: pin['artifacts'][artifact_side][mode][executable]['sha256']
                    for side, artifact_side in zip(('before', 'after'), sides)}
        if metadata['binary_hashes'] != expected:
            raise ValueError('observation binary hash mismatch: ' + run['label'])
    return result


def observations(analysis):
    result = []
    for run in analysis['runs']:
        metadata = run.get('metadata', {})
        for item in run['observations']:
            result.append({'label': run['label'], 'valid_evidence': run['valid_evidence'],
                'timing_evidence': run.get('timing_evidence', False),
                **{key: metadata.get(key) for key in ('comparison_id', 'accepted_head', 'candidate', 'fixture',
                    'stage', 'scope', 'allocations', 'source_hashes', 'binary_hashes')},
                **{key: item.get(key) for key in ('pair', 'side', 'metrics', 'identity', 'config', 'contract', 'errors')}})
    return result


def fmt(value):
    return '—' if value is None else f'{value:.3f}'


def render(report):
    esc = html.escape
    decisions = ''.join('<tr>' + ''.join('<td>' + esc(str(item.get(key, '—'))) + '</td>'
        for key in ('candidate', 'verdict', 'evidence', 'commit')) + '</tr>'
        for item in report['adoption_decisions'].get('decisions', []))
    rows = []
    for group in report['groups']:
        metric, diagnostic = group['metrics']['elapsed_ms'], group['allocations_diagnostic']
        ci = metric.get('ci95_improvement_pct')
        values = [group['comparison_id'], group['comparison_kind'], group['fixture'], group['stage'],
            'allocator' if diagnostic else 'timing', metric['paired_n'],
            'диагностика' if diagnostic else fmt(metric.get('before_mean')),
            'диагностика' if diagnostic else fmt(metric.get('after_mean')),
            'не оценивается' if diagnostic else fmt(metric.get('improvement_pct')),
            '—' if diagnostic or ci is None else ' .. '.join(fmt(value) for value in ci),
            '—' if diagnostic else fmt(metric.get('aa_noise_floor_pct', metric.get('noise_floor_pct'))),
            '—' if diagnostic else fmt(group['metrics'].get('cpu_cycles', {}).get('improvement_pct')),
            fmt(group['metrics'].get('alloc_successes_per_operation', {}).get('absolute_reduction')),
            fmt(group['metrics'].get('alloc_success_bytes_per_operation', {}).get('absolute_reduction'))]
        rows.append('<tr>' + ''.join('<td>' + esc(str(value)) + '</td>' for value in values) + '</tr>')
    headers = ('Фаза', 'A/A или A/B', 'Fixture/case', 'Стадия', 'Режим', 'Пар', 'До, мс', 'После, мс',
               'Сокращение времени, %', '95% CI, %', 'Шум A/A, %', 'Сокращение cycles, %', '−alloc/scan', '−alloc bytes/scan')
    links = ''.join('<li><a href="reproduction/' + esc(item['path']) + '">' + esc(item['path']) + '</a></li>'
                    for item in report['reproduction_files'])
    return ('<!doctype html><html lang="ru"><meta charset="utf-8"><title>Внедрение микрооптимизаций sender</title>'
        '<style>body{font:15px system-ui;margin:24px;color:#17212b}table{border-collapse:collapse;font-variant-numeric:tabular-nums}'
        'td,th{border:1px solid #cbd5df;padding:7px;vertical-align:top;text-align:left}th{background:#edf3f8}'
        'pre{white-space:pre-wrap;font-size:12px}.scroll{overflow-x:auto}a{color:#135ba1}</style>'
        '<h1>Внедрение микрооптимизаций sender</h1><p><strong>' + esc(report['adoption_decisions'].get('conclusion', 'Черновик без окончательного решения.')) +
        '</strong></p><p>M2 сравнивается с принятым M1; M3 — с последним принятым состоянием после решения по M2. '
        'Фиксированы threads=4, readers=8, level=3, frame=64 MiB. Выводы относятся к этой Windows-машине.</p>'
        '<table><tr><th>Вариант</th><th>Решение</th><th>Основание</th><th>Commit</th></tr>' + decisions + '</table>'
        '<p><a href="comparison.json">Все метрики и наблюдения</a> · <a href="tests/summary.json">Проверки Windows/Linux</a></p>'
        '<h2>Сопоставления</h2><p>Положительный процент означает уменьшение затрат. Интервал целиком должен выйти за соответствующий '
        'диапазон A/A, чтобы пройти строгий описательный критерий. Instrumented timing/CPU не используются как доказательство скорости. '
        'Шумное E2E не доказывает отсутствие небольшой регрессии.</p><div class="scroll"><table><tr>' +
        ''.join('<th>' + esc(title) + '</th>' for title in headers) + '</tr>' + ''.join(rows) + '</table></div>'
        '<h2>Allocation counts</h2><p>Для M2 прогноз по topology: 46 удалённых allocations за scan manySmallFiles и 1024 за wide-tree. '
        'Whole-scan включает динамические allocations каналов и синхронизации; точное совпадение каждого delta с прогнозом не является критерием принятия.</p><pre>' +
        esc(json.dumps(report['allocation_expectations'].get('comparisons', []), ensure_ascii=False, indent=2)) + '</pre>'
        '<h2>Ограничения</h2><ul>' + ''.join('<li>' + esc(item) + '</li>' for item in report['limitations']) + '</ul>'
        '<h2>Воспроизведение и provenance</h2><p>Исходные payloads и подробные logs сохраняются локально; компактные результаты '
        'импортируются отдельно через scripts/results.py. Пути в скриптах относятся к исходному workspace.</p><ul>' + links + '</ul>'
        '<details><summary>Хэши sources, compiler и binaries</summary><pre>' +
        esc(json.dumps(report['provenance'], ensure_ascii=False, indent=2)) + '</pre></details>'
        '<h2>Неудачные попытки проверок</h2><pre>' + esc(json.dumps(report['validation_failures'], ensure_ascii=False, indent=2)) + '</pre></html>')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--final', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    output = (args.output or ROOT / ('report-final' if args.final else 'report-draft')).resolve()
    if output.exists():
        raise ValueError('refusing to overwrite report; choose a new --output')
    analysis = load(ROOT / 'analysis.json')
    decisions = load(ROOT / 'adoption-decisions.json') if (ROOT / 'adoption-decisions.json').exists() else {}
    if args.final:
        final_ready(analysis, decisions)
    provenance = verified_provenance(analysis)
    checks, failures = checks_summary()
    samples = observations(analysis)
    plan = load(ROOT / 'measurement-plan.json')
    evidence_paths = [ROOT / name for name in ('run_measurements.py', 'analyze_measurements.py', 'package_report.py',
        'run_control_confirmation.py', 'control-confirmation-spec.json', 'verify_windows.py', 'verify_linux.py',
        'measurement-plan.json', 'comparison-specs.json', 'fixtures.json', 'allocation-expectations.json',
        'experiment.json', 'adoption-decisions.json', 'm1-acceptance.json', 'm1-bench-reuse.json',
        'm1-source.json', 'm2-source.json', 'm2-bench-source.json', 'm2-acceptance.json',
        'm3-source.json', 'm3-bench-source.json', 'm3-acceptance.json', 'm3-rejection.json')]
    evidence_paths += sorted((ROOT / 'provenance').glob('*.json'))
    evidence_paths += sorted((ROOT / 'patches').glob('*.patch'))
    evidence_paths = [path for path in evidence_paths if path.exists()]
    m1_acceptance = load(ROOT / 'm1-acceptance.json')
    prior_report = Path(m1_acceptance['prior_performance_report'])
    prior_report_identity = {'path': str(prior_report), 'sha256': driver.sha(prior_report),
                             'bytes': prior_report.stat().st_size}
    prior_tracked = REPO / 'results/benchmarks/legacy-results-local-experiments-sender-micro-20261025-report-final-158ee4096162/raw/comparison.json'
    if prior_tracked.exists():
        if driver.sha(prior_tracked) != prior_report_identity['sha256']:
            raise ValueError('prior tracked/local micro reports differ')
        prior_report_identity['tracked_path'] = prior_tracked.relative_to(REPO).as_posix()
    report = {'schema': 1, 'status': 'final_adoption_report' if args.final else 'draft',
        'generated_utc': datetime.now(timezone.utc).isoformat(), 'experiment': load(ROOT / 'experiment.json'),
        'adoption_decisions': decisions, 'method': analysis['method'], 'provenance': provenance,
        'prior_evidence': {'m1_acceptance': m1_acceptance,
            'micro_experiment_report': prior_report_identity,
            'reuse_scope': 'M1 measurements are carried from the prior experiment; new M2 and M3 samples are incremental, not independent original-baseline comparisons.'},
        'consistency': {'series': len(analysis['runs']), 'valid_series': sum(run['valid_evidence'] for run in analysis['runs']),
            'observations': len(samples), 'e2e_observations': sum(item['stage'] == 'e2e' for item in samples),
            'allocation_observations': sum(bool(item['allocations']) for item in samples),
            'e2e_issue_totals': {field: sum(item['metrics'].get(field) or 0 for item in samples if item['stage'] == 'e2e')
                                 for field in ('skipped_files', 'frame_aborts', 'frame_rebuilds')}},
        'groups': analysis['groups'], 'observations': samples,
        'measurement_runs': [{'label': run['label'], 'metadata': run.get('metadata'), 'valid_evidence': run['valid_evidence'],
                  'errors': run['errors'], 'raw_manifest': identity(ROOT / 'measurements' / run['label'] / 'paired-run.json')}
                 for run in analysis['runs']],
        'allocation_expectations': analysis['allocation_expectations'], 'validation_failures': failures,
        'limitations': plan['scope_limits'] + analysis['scope_notes'] + decisions.get('limitations', []),
        'reproduction_files': [identity(path) for path in evidence_paths]}
    output.mkdir(parents=True)
    for path in evidence_paths:
        target = output / 'reproduction' / path.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
    save(output / 'tests/summary.json', {'commit': report['experiment'].get('final_commit', report['experiment'].get('baseline_commit')),
        'checks': checks, 'failures': failures, 'decisions': decisions})
    save(output / 'comparison.json', report)
    (output / 'comparison.html').write_text(render(report), encoding='utf-8')
    print(json.dumps({'output': str(output), 'counts': report['consistency'],
                      'comparison_sha256': driver.sha(output / 'comparison.json')}, indent=2))


if __name__ == '__main__':
    main()
