"""Package local JSON/HTML evidence after measurements; never infer final decisions."""
import argparse
import difflib
import hashlib
import html
import json
from pathlib import Path
import re
import shutil
import subprocess
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]
CANDIDATES = ('m1-scan-tail', 'm2-subtree-move', 'm3-control-capacity')
CASES = ('commit', 'plan-short-1', 'plan-long-1', 'plan-short-256', 'plan-long-256',
         'plan-short-4096', 'plan-long-4096', 'noop')
PATCHES = ('common-harness', *CANDIDATES)
NAMES = {'m1-scan-tail': 'M1: последний scan batch без нового буфера',
         'm2-subtree-move': 'M2: перенос subtree descriptor вместо clone',
         'm3-control-capacity': 'M3: capacity буфера control message'}
LIMITATIONS = [
    'Производительность измерена на одной Windows-машине. Linux checks проверяют корректность, а не Linux performance.',
    'E2E использует localhost; source/output находятся на одном томе. Кэш и фоновая нагрузка не контролируются; verification может прогреть следующие samples.',
    'Все первые samples и выбросы сохранены. Парный bootstrap описательный, без поправки на множественные сравнения.',
    'Instrumented allocator timing/CPU сохранены только как диагностика и не используются для доказательства ускорения или регрессии.',
    'Control wall time и allocator counts охватывают только цикл. CPU/cycles snapshots немного шире и включают hooks/instrumentation. Noop не вычитается автоматически.',
    'До control timing выполняются reference encode/decode: allocator и код прогреваются подготовкой. Это не cold-start измерение. RSS — lifetime peak, включая fixture/reference.',
    'Control benchmark включает запись сериализованных bytes в заранее выделенный memory sink; он не измеряет задержку сети.',
    'E2E CPU/RSS относятся к общему процессу sender+receiver. Scan CPU измеряет стадию, её RSS остаётся lifetime peak.',
    'Allocation counts относятся к Rust global allocator; это не live memory и не подсчёт всех native zstd allocations. Reallocation bytes — полный новый размер.',
    'Windows process cycles не равны retired instructions; process I/O не равен физическому I/O накопителя.',
    'Снижение локальной работы не доказывает ускорение всей передачи. Принятие микрооптимизации допускает неподтверждённый E2E gain, но требует рассмотрения подтверждённых регрессий.',
    'Три candidate patches независимы относительно baseline с общим benchmark harness. Common harness сохранён отдельным patch; mainline source не изменён.',
]


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def identity(path):
    return {'path': path.relative_to(ROOT).as_posix(), 'sha256': digest(path), 'bytes': path.stat().st_size}


def required_labels():
    labels = {'aa-scan-noop'}
    for fixture in ('manySmallFiles', 'wide-tree'):
        labels.add('aa-scan-' + fixture)
        for candidate in CANDIDATES[:2]:
            labels.update((candidate + '-scan-' + fixture, candidate + '-alloc-' + fixture))
    for case in CASES:
        labels.add('aa-control-' + case)
        if case != 'noop':
            labels.update(('m3-control-' + case, 'm3-alloc-' + case))
    for fixture in ('manySmallFiles', 'mixed', 'bigFiles'):
        labels.add('aa-e2e-' + fixture)
        labels.update(candidate + '-e2e-' + fixture for candidate in CANDIDATES)
    return labels


def final_ready(analysis, reviewed):
    runs = {run['label']: run for run in analysis['runs']}
    missing = sorted(label for label in required_labels() if not runs.get(label, {}).get('valid_evidence'))
    if missing:
        raise ValueError('missing/incomplete required series: ' + ', '.join(missing))
    if not reviewed.get('conclusion') or {row.get('candidate') for row in reviewed.get('decisions', [])} != set(CANDIDATES):
        raise ValueError('reviewed-decisions.json must contain conclusion and decisions for all three candidates')
    for row in reviewed['decisions']:
        if not row.get('verdict') or not row.get('evidence'):
            raise ValueError('each reviewed decision needs verdict and evidence')
    analysis_time = (ROOT / 'analysis.json').stat().st_mtime_ns
    for path in (ROOT / 'measurements').iterdir():
        if not path.is_dir():
            continue
        if path.name not in runs or not runs[path.name]['valid_evidence']:
            raise ValueError('extra incomplete measurement requires explicit review: ' + path.name)
        driver = ROOT / 'driver-logs' / (path.name + '.json')
        if not driver.exists() or driver.stat().st_mtime_ns > analysis_time:
            raise ValueError('analysis is stale or driver unfinished: ' + path.name)


def source_provenance():
    manifests = load(ROOT / 'source-manifests.json')
    paths = {'baseline_original': REPO,
             **{name: ROOT / 'sources' / name for name in ('baseline', *CANDIDATES)}}
    result = {}
    for name, directory in paths.items():
        manifest = manifests[name]
        actual = {relative: digest(directory / relative) for relative in manifest['files']}
        computed = hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()
        if actual != manifest['files'] or computed != manifest['source_sha256']:
            raise ValueError('source differs from recorded manifest: ' + name)
        result[name] = {'source_sha256': computed, 'changed_paths': manifest.get('changed_paths', [])}
    patches = {}
    for name in PATCHES:
        before_name = 'baseline_original' if name == 'common-harness' else 'baseline'
        after_name = 'baseline' if name == 'common-harness' else name
        before, after = manifests[before_name]['files'], manifests[after_name]['files']
        changed = sorted(relative for relative in before.keys() | after.keys() if before.get(relative) != after.get(relative))
        text = []
        for relative in changed:
            left = paths[before_name] / relative
            right = paths[after_name] / relative
            text.extend(difflib.unified_diff(
                left.read_text(encoding='utf-8').splitlines(keepends=True) if relative in before else [],
                right.read_text(encoding='utf-8').splitlines(keepends=True) if relative in after else [],
                fromfile='a/' + relative if relative in before else '/dev/null',
                tofile='b/' + relative if relative in after else '/dev/null'))
        patch = ROOT / 'patches' / (name + '.patch')
        if patch.read_bytes() != ''.join(text).encode('utf-8'):
            raise ValueError('patch does not exactly match its independent source diff: ' + name)
        patches[name] = {**identity(patch), 'base': before_name, 'candidate': after_name,
                         'base_source_sha256': result[before_name]['source_sha256'],
                         'candidate_source_sha256': result[after_name]['source_sha256'], 'changed_paths': changed}
    return {'snapshots': result, 'patches': patches, 'manifest': identity(ROOT / 'source-manifests.json')}


def binary_provenance(analysis):
    binaries = {}
    for path in sorted((ROOT / 'artifacts').glob('*/hashes.json')):
        values = load(path)
        for name, expected in values.items():
            if digest(path.parent / (name + '.exe')) != expected:
                raise ValueError('artifact binary changed: ' + str(path))
        binaries[path.parent.name] = values
    for run in analysis['runs']:
        if not run['valid_evidence']:
            continue
        metadata = run['metadata']
        suffix = '-alloc' if metadata.get('allocations') else ''
        executable = {'e2e': 'loopback_send_dump', 'control': 'control_messages',
                      'scan': 'file_pipeline', 'noop': 'file_pipeline'}[metadata['stage']]
        expected = {'before': binaries['baseline' + suffix][executable],
                    'after': binaries[metadata['candidate'] + suffix][executable]}
        if expected != metadata['binary_hashes']:
            raise ValueError('measured binary does not match artifact: ' + run['label'])
    return binaries


def compiler_provenance(experiment):
    sysroot = Path(subprocess.check_output(['rustc', '--print', 'sysroot'], text=True).strip())
    path = sysroot / 'bin/rustc.exe'
    current = subprocess.check_output([str(path), '-vV'], text=True)
    if current != experiment['compiler']:
        raise ValueError('current compiler identity differs from recorded build compiler')
    return {'recorded_windows_identity': experiment['compiler'],
            'identity_text_sha256': hashlib.sha256(experiment['compiler'].encode()).hexdigest(),
            'executable_path': str(path), 'executable_sha256': digest(path),
            'executable_observed_utc': datetime.now(timezone.utc).isoformat(),
            'scope': 'Executable hashed during final packaging, not at build time; identity matches recorded build rustc -vV. Linux compiler identities are retained per check environment.'}


def checks_summary():
    groups, failures = {}, []
    for path in sorted((ROOT / 'checks').rglob('checks.json')):
        original = load(path)
        rows = original if isinstance(original, list) else [original]
        records = []
        for original_row in rows:
            row = dict(original_row)
            log = path.parent / (str(row.get('check', 'build')) + '.log')
            if log.exists():
                row['log_evidence'] = identity(log)
                text = log.read_text(encoding='utf-8', errors='replace')
                row['rust_suite_counts'] = [dict(zip(('passed', 'failed', 'ignored'), map(int, item)))
                    for item in re.findall(r'test result: .*? (\d+) passed; (\d+) failed; (\d+) ignored', text)]
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


def compact_observations(analysis):
    observations = []
    for run in analysis['runs']:
        metadata = run.get('metadata', {})
        for item in run['observations']:
            observations.append({'label': run['label'], 'valid_evidence': run['valid_evidence'],
                'timing_evidence': run.get('timing_evidence', False),
                **{key: metadata.get(key) for key in ('candidate', 'fixture', 'stage', 'scope', 'allocations')},
                **{key: item.get(key) for key in ('pair', 'side', 'metrics', 'identity', 'config', 'contract', 'errors')}})
    return observations


def consistency(analysis, observations):
    e2e = [row for row in observations if row['stage'] == 'e2e']
    fields = ('compressed_payload_bytes', 'files_count', 'raw_read_bytes')
    return {'series': len(analysis['runs']), 'valid_series': sum(run['valid_evidence'] for run in analysis['runs']),
        'observations': len(observations), 'e2e_observations': len(e2e),
        'timing_observations': sum(row['timing_evidence'] for row in observations),
        'allocation_observations': sum(bool(row['allocations']) for row in observations),
        'e2e_counter_totals': {key: sum(row['metrics'].get(key) or 0 for row in e2e)
                               for key in ('skipped_files', 'frame_aborts', 'frame_rebuilds')},
        'e2e_equal_count_checks': {run['label']: {field: sorted({item['metrics'][field] for item in run['observations']})
                                                for field in fields}
                                  for run in analysis['runs'] if run.get('metadata', {}).get('stage') == 'e2e'},
        'control_packet_identity': 'Analyzer requires case/iterations/packet_bytes/body_bytes/fingerprint equality across every before/after sample within each series.'}


def show(value):
    return '—' if value is None else f'{value:.3f}'


def render(report):
    esc = html.escape
    reviewed = report['reviewed_decisions']
    decisions = ''.join('<tr><td>' + esc(NAMES.get(row['candidate'], row['candidate'])) + '</td><td><strong>' +
        esc(row['verdict']) + '</strong></td><td>' + esc(row['evidence']) + '</td></tr>'
        for row in reviewed.get('decisions', []))
    if not decisions:
        decisions = '<tr><td colspan="3">Черновик: итоговые решения ещё не записаны.</td></tr>'
    rows = []
    for group in report['groups']:
        metric = group['metrics']['elapsed_ms']
        diagnostic = group['allocations_diagnostic']
        ci = metric.get('ci95_improvement_pct')
        values = [group['candidate'], group['fixture'], group['stage'], 'allocations' if diagnostic else 'timing',
            metric['paired_n'], 'diagnostic only' if diagnostic else show(metric.get('before_mean')),
            'diagnostic only' if diagnostic else show(metric.get('after_mean')),
            'не используется' if diagnostic else show(metric.get('improvement_pct')),
            '—' if diagnostic or ci is None else ' .. '.join(show(value) for value in ci),
            '—' if diagnostic else show(metric.get('aa_noise_floor_pct', metric.get('noise_floor_pct'))),
            show(group['metrics'].get('alloc_successes_per_operation', {}).get('absolute_reduction')),
            show(group['metrics'].get('realloc_successes_per_operation', {}).get('absolute_reduction')),
            group['provisional_verdict']]
        rows.append('<tr>' + ''.join('<td>' + esc(str(value)) + '</td>' for value in values) + '</tr>')
    headers = ['Вариант', 'Fixture/case', 'Стадия', 'Режим', 'Пар', 'До мс', 'После мс', 'Gain %',
               '95% CI %', 'A/A %', '−alloc/operation', '−realloc/operation', 'Автоматическая классификация']
    return ('<!doctype html><html lang="ru"><meta charset="utf-8"><title>Микрооптимизации sender</title>'
        '<style>body{font:15px system-ui;margin:24px;color:#17212b}table{border-collapse:collapse;font-variant-numeric:tabular-nums}'
        'td,th{border:1px solid #cbd5df;padding:7px;text-align:left;vertical-align:top}th{background:#edf3f8}'
        'pre{white-space:pre-wrap;font-size:12px}.scroll{overflow-x:auto}summary{cursor:pointer}a{color:#135ba1}</style>'
        '<h1>Микрооптимизации sender</h1><p><strong>' + esc(reviewed.get('conclusion', 'Черновик без окончательного вердикта.')) +
        '</strong></p><p>Фиксированы threads=4, readers=8, level=3, frame=64 MiB. Измерения и allocator diagnostics разделены. '
        'Снижение числа операций полезно само по себе; E2E ускорение указывается только при достаточных данных.</p>'
        '<table><thead><tr><th>Вариант</th><th>Итог ревью</th><th>Основание</th></tr></thead><tbody>' + decisions + '</tbody></table>'
        '<p><a href="comparison.json">Все компактные данные и наблюдения</a> · <a href="tests/summary.json">Проверки</a></p>'
        '<h2>Проверка ожидаемого числа allocations</h2><p>manySmallFiles: M1 экономит ожидаемые 24 allocations за scan, '
        'M2 — 46; wide-tree: 512 и 1024 соответственно. Это прогноз по реальной topology, не измеренный результат.</p><pre>' +
        esc(json.dumps(report['allocation_expectations'].get('comparisons', []), ensure_ascii=False, indent=2)) + '</pre>'
        '<h2>Сопоставления</h2><p>Положительный gain означает сокращение времени/работы. '
        'Автоматическая классификация не заменяет итоговое ревью. Скорость instrumented сборок не интерпретируется.</p>'
        '<div class="scroll"><table><thead><tr>' + ''.join('<th>' + esc(value) + '</th>' for value in headers) +
        '</tr></thead><tbody>' + ''.join(rows) + '</tbody></table></div>'
        '<h2>Ограничения</h2><ul>' + ''.join('<li>' + esc(value) + '</li>' for value in report['limitations']) + '</ul>'
        '<h2>Независимые patches</h2><ul>' + ''.join('<li><a href="patches/' + name + '.patch">' + name +
            '</a></li>' for name in PATCHES) + '</ul>'
        '<h2>Сохранённые неудачные попытки проверок</h2><pre>' +
        esc(json.dumps(report['validation_failures'], ensure_ascii=False, indent=2)) + '</pre>'
        '<details><summary>Provenance и hashes</summary><pre>' +
        esc(json.dumps(report['provenance'], ensure_ascii=False, indent=2)) + '</pre></details></html>')


def attach_patches(output):
    report = load(output / 'comparison.json')
    if report['status'] != 'final_experiment_report':
        raise ValueError('only final report can be attached')
    relative = output.relative_to(REPO).as_posix()
    expected = digest(output / 'comparison.json')
    matches = []
    for path in (REPO / 'results/benchmarks').glob('*/run.json'):
        manifest = load(path)
        if manifest.get('experiment_path') == relative and any(item.get('sha256') == expected for item in manifest.get('evidence', [])):
            matches.append(path.parent)
    if len(matches) != 1:
        raise ValueError('import final report using scripts/results.py before attaching patches')
    target = matches[0] / 'patches'
    target.mkdir(exist_ok=True)
    for name in PATCHES:
        source = output / 'patches' / (name + '.patch')
        destination = target / source.name
        if destination.exists() and digest(destination) != digest(source):
            raise ValueError('refusing different existing patch')
        if not destination.exists():
            shutil.copyfile(source, destination)
    provenance = report['provenance']['sources']
    manifest = target / 'manifest.json'
    if manifest.exists() and load(manifest) != provenance:
        raise ValueError('refusing different existing provenance')
    if not manifest.exists():
        save(manifest, provenance)
    print(target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--final', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--attach-patches', action='store_true')
    args = parser.parse_args()
    output = (args.output or ROOT / ('report-final' if args.final or args.attach_patches else 'report-draft')).resolve()
    if args.attach_patches:
        attach_patches(output)
        return
    if output.exists():
        raise ValueError('refusing overwrite; choose a new --output directory')
    analysis = load(ROOT / 'analysis.json')
    decision_path = ROOT / 'reviewed-decisions.json'
    reviewed = load(decision_path) if decision_path.exists() else {}
    if args.final:
        final_ready(analysis, reviewed)
    experiment = load(ROOT / 'experiment.json')
    checks, failures = checks_summary()
    observations = compact_observations(analysis)
    provenance = {'baseline_commit': experiment['baseline_commit'], 'compiler': compiler_provenance(experiment),
        'sources': source_provenance(), 'binary_sha256': binary_provenance(analysis),
        'analysis': identity(ROOT / 'analysis.json'), 'fixtures': load(ROOT / 'fixtures.json'),
        'analysis_script': identity(ROOT / 'analyze_measurements.py'),
        'measurement_driver': identity(ROOT / 'run_measurements.py')}
    patch_checks = ROOT / 'patch-checks.json'
    if patch_checks.exists():
        provenance['patch_apply_checks'] = load(patch_checks)
    if decision_path.exists():
        provenance['reviewed_decisions_source'] = identity(decision_path)
    report = {'schema': 2, 'status': 'final_experiment_report' if args.final else 'draft',
        'generated_utc': datetime.now(timezone.utc).isoformat(), 'experiment': experiment,
        'reviewed_decisions': reviewed, 'provenance': provenance, 'method': analysis['method'],
        'groups': analysis['groups'], 'observations': observations,
        'measurement_runs': [{key: run.get(key) for key in ('label', 'complete', 'valid_evidence', 'timing_evidence', 'errors', 'metadata')}
                             for run in analysis['runs']],
        'allocation_expectations': analysis['allocation_expectations'],
        'consistency': consistency(analysis, observations), 'validation_failures': failures,
        'limitations': LIMITATIONS + reviewed.get('limitations', [])}
    save(output / 'comparison.json', report)
    save(output / 'tests/summary.json', {'commit': experiment['baseline_commit'], 'checks': checks,
        'additional_checks': {'provenance': provenance, 'all_failed_attempts_retained': failures,
                              'measurement_consistency': report['consistency']}, 'limitations': report['limitations']})
    (output / 'patches').mkdir()
    for name in PATCHES:
        shutil.copyfile(ROOT / 'patches' / (name + '.patch'), output / 'patches' / (name + '.patch'))
    (output / 'report.html').write_text(render(report), encoding='utf-8')
    print(output / 'report.html')
    print('Local artifacts only. Import measurements and this report with scripts/results.py, then --attach-patches.')


if __name__ == '__main__':
    main()
