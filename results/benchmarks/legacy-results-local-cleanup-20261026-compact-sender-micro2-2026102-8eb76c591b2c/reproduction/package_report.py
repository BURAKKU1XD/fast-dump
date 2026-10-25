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


def decisions():
    path = ROOT / 'adoption-decisions.json'
    if path.exists():
        return load(path)
    return {'conclusion': '', 'decisions': [load(path) for path in sorted(ROOT.glob('*-decision.json'))]}


def final_ready(analysis, reviewed, specs):
    rows = analysis['measurement_runs']
    found = {row['label']: row for row in rows}
    wanted = {item['label'] for spec in specs.values() for item in driver.series(spec, 'screen')}
    phases = load(ROOT / 'phase-decisions.json') if (ROOT / 'phase-decisions.json').exists() else {}
    for name, spec in specs.items():
        if phases.get(name, {}).get('screen_pass') is True:
            wanted.update(item['label'] for item in driver.series(spec, 'e2e'))
    if set(found) != wanted or any(not row['valid_evidence'] for row in rows):
        raise ValueError('missing/extra/invalid measurement series: ' + repr(sorted(set(found) ^ wanted)))
    expected_candidates = {spec['candidate'] for spec in specs.values()}
    if len(expected_candidates) != 4 or {item.get('candidate') for item in reviewed.get('decisions', [])} != expected_candidates:
        raise ValueError('four explicit human candidate decisions are required')
    for item in reviewed['decisions']:
        if not (item.get('verdict') or item.get('status')) or not (item.get('evidence') or item.get('reason')):
            raise ValueError('each decision needs verdict/status and evidence/reason')
    analyzed = (ROOT / 'analysis.json').stat().st_mtime_ns
    for directory in ('measurements', 'driver-logs'):
        for path in (ROOT / directory).rglob('*'):
            if path.is_file() and path.stat().st_mtime_ns > analyzed:
                raise ValueError('analysis is stale: ' + str(path))


def verified_provenance(analysis, specs):
    result = {}
    for row in analysis['measurement_runs']:
        metadata = row['metadata']
        filename = metadata['provenance_file']
        if filename in result:
            continue
        path = ROOT / 'provenance' / filename
        pin = load(path)
        current = driver.preflight(metadata['comparison_id'], specs[metadata['comparison_id']], metadata['phase'])
        if pin != current:
            raise ValueError('source/compiler/artifact/driver provenance changed: ' + filename)
        result[filename] = {'evidence': identity(path), 'comparison_id': pin['comparison_id'], 'phase': pin['phase'],
            'accepted_head': pin['accepted_head'], 'source_hashes': pin['source_hashes'], 'artifacts': pin['artifacts'],
            'compiler': pin['compiler'], 'compiler_binary': pin['compiler_binary'],
            'driver_sha256': pin['driver_sha256'], 'scripts': pin['scripts']}
    for row in analysis['measurement_runs']:
        meta = row['metadata']
        pin = result[meta['provenance_file']]
        if pin['evidence']['sha256'] != meta['provenance_sha256']:
            raise ValueError('measurement provenance hash mismatch: ' + row['label'])
        mode = 'allocation' if meta['allocations'] else 'timing'
        executable = meta['group_spec']['executable']
        side_map = ('before', 'before') if row['comparison_kind'] == 'A/A' else ('before', 'after')
        expected = {side: pin['artifacts'][source_side][mode][executable]['sha256']
                    for side, source_side in zip(('before', 'after'), side_map)}
        if expected != meta['binary_hashes']:
            raise ValueError('measurement binary mismatch: ' + row['label'])
    return result


def compact_observations(analysis):
    result = []
    for row in analysis['measurement_runs']:
        meta = row.get('metadata', {})
        for item in row['observations']:
            result.append({'label': row['label'], 'valid_evidence': row['valid_evidence'],
                'timing_evidence': row.get('timing_evidence', False),
                **{key: meta.get(key) for key in ('comparison_id', 'phase', 'group_id', 'accepted_head', 'candidate',
                    'fixture', 'stage', 'scope', 'allocations', 'source_hashes', 'binary_hashes')},
                **{key: item.get(key) for key in ('pair', 'side', 'metrics', 'identity', 'config', 'contract', 'errors')},
                'raw_measurement': {key: value for key, value in item.get('raw', {}).items() if key != 'log'}})
    return result


def fmt(value):
    return '—' if value is None else f'{value:.3f}'


def verdict(item):
    if item.get('verdict'):
        return item['verdict']
    status = item.get('status', '')
    if status.startswith('accepted'):
        return 'Внедрён'
    if 'effect_unproven' in status:
        return 'Не внедрён: эффект не доказан'
    if 'regression' in status:
        return 'Не внедрён: регрессия'
    return status or 'Решение ещё не принято'


def render(report):
    esc = html.escape
    reviewed = report['adoption_decisions']
    names = {'arc': '1. Последний shared Arc', 'progress': '2. Borrowed progress guard',
             'counter': '3. Счётчик terminal skips', 'commit': '4. Stack BundleCommit'}
    top = ''.join('<tr><td>' + esc(names.get(item['candidate'], item['candidate'])) + '</td><td><strong>' +
        esc(verdict(item)) + '</strong></td><td>' + esc(str(item.get('evidence', item.get('reason', '')))) +
        '</td><td>' + esc(str(item.get('commit', '—'))) + '</td></tr>' for item in reviewed.get('decisions', []))
    rows = []
    for group in report['groups']:
        m = group['metrics']['elapsed_ms']
        d = group['allocations_diagnostic']
        ci = m.get('ci95_improvement_pct')
        config = group['configuration']['scenario']
        values = [group['comparison_id'], group['comparison_kind'], group['fixture'], group['stage'],
            config.get('iterations') or 1, 'allocator' if d else 'timing', m['paired_n'],
            'диагностика' if d else fmt(m.get('before_mean')), 'диагностика' if d else fmt(m.get('after_mean')),
            'не оценивается' if d else fmt(m.get('improvement_pct')),
            '—' if d or ci is None else ' .. '.join(fmt(value) for value in ci),
            '—' if d else fmt(m.get('aa_noise_floor_pct', m.get('noise_floor_pct'))),
            '—' if d else fmt(group['metrics'].get('cpu_cycles', {}).get('improvement_pct')),
            fmt(group['metrics'].get('alloc_successes_per_operation', {}).get('absolute_reduction')),
            fmt(group['metrics'].get('realloc_successes_per_operation', {}).get('absolute_reduction'))]
        rows.append('<tr>' + ''.join('<td>' + esc(str(value)) + '</td>' for value in values) + '</tr>')
    headers = ('Кандидат', 'Сравнение', 'Fixture/case', 'Стадия', 'Итераций', 'Режим', 'Пар', 'До, мс', 'После, мс',
               'Сокращение времени, %', 'CI95, %', 'A/A, %', 'Сокращение cycles, %', '−alloc/op', '−realloc/op')
    return ('<!doctype html><html lang="ru"><meta charset="utf-8"><title>Четыре микрооптимизации sender</title>'
        '<style>body{font:15px system-ui;margin:24px;color:#17212b}table{border-collapse:collapse;font-variant-numeric:tabular-nums}'
        'td,th{border:1px solid #cbd5df;padding:7px;vertical-align:top;text-align:left}th{background:#edf3f8}'
        'pre{white-space:pre-wrap;font-size:12px}.scroll{overflow-x:auto}a{color:#135ba1}</style>'
        '<h1>Четыре микрооптимизации sender</h1><p><strong>' + esc(reviewed.get('conclusion') or 'Решения по отдельным кандидатам приведены ниже.') +
        '</strong></p><p>Каждый кандидат сравнивается с последним принятым состоянием. Исходная база — 2a7c8eee (M1+M2). '
        'Короткие и длинные циклы, scopes и allocator diagnostics анализируются отдельно.</p>'
        '<p><strong>Результаты control относятся к сериализации отдельного сообщения через write_message в заранее выделенный Vec sink. '
        'Сокращение времени BundleCommit на 92–95% не означает такого ускорения передачи дампа: полная передача измеряется отдельно в E2E.</strong></p>'
        '<table><tr><th>Кандидат</th><th>Решение</th><th>Основание</th><th>Commit</th></tr>' + top + '</table>'
        '<p><a href="comparison.json">Все наблюдения и метрики</a> · <a href="tests/summary.json">Проверки и попытки сборок</a></p>'
        '<h2>Покрытие</h2><pre>' + esc(json.dumps(report['coverage'], ensure_ascii=False, indent=2)) + '</pre>'
        '<h2>Измерения</h2><p>Положительный процент означает уменьшение затрат. Автоматическая классификация в JSON '
        'не заменяет решение по совокупности целевого сценария и отрицательных контролей.</p><div class="scroll"><table><tr>' +
        ''.join('<th>' + esc(title) + '</th>' for title in headers) + '</tr>' + ''.join(rows) + '</table></div>'
        '<h2>Ограничения</h2><ul>' + ''.join('<li>' + esc(item) + '</li>' for item in report['limitations']) + '</ul>'
        '<h2>Воспроизведение</h2><p>Подробные measurement logs остаются локально и импортируются отдельно. '
        'Сохранённые копии скриптов относятся к исходному workspace; перед запуском требуется восстановить его layout и артефакты.</p><ul>' +
        ''.join('<li><a href="reproduction/' + esc(item['path']) + '">' + esc(item['path']) + '</a></li>'
                for item in report['reproduction_files']) + '</ul>'
        '<details><summary>Source/compiler/binary hashes</summary><pre>' + esc(json.dumps(report['provenance'], ensure_ascii=False, indent=2)) +
        '</pre></details><h2>Исправления подготовки</h2><pre>' + esc(json.dumps(report['setup_corrections'], ensure_ascii=False, indent=2)) +
        '</pre><h2>Неудачные проверки</h2><pre>' + esc(json.dumps(report['validation_failures'], ensure_ascii=False, indent=2)) + '</pre></html>')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--final', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    output = (args.output or ROOT / ('report-final' if args.final else 'report-draft')).resolve()
    if output.exists():
        raise ValueError('refusing to overwrite report; choose a different --output')
    analysis, specs = load(ROOT / 'analysis.json'), load(ROOT / 'comparisons.json')
    reviewed = decisions()
    if args.final:
        final_ready(analysis, reviewed, specs)
        if not re.fullmatch(r'[0-9a-f]{40}', load(ROOT / 'experiment.json').get('final_commit', '')):
            raise ValueError('final report requires experiment.final_commit')
    provenance = verified_provenance(analysis, specs)
    checks, failures = checks_summary()
    samples = compact_observations(analysis)
    paths = sorted(path for path in ROOT.iterdir() if path.is_file() and path.suffix in ('.py', '.json')
                   and 'analysis' not in path.name)
    paths += sorted((ROOT / 'provenance').glob('*.json'))
    paths += sorted((ROOT / 'patches').glob('*'))
    paths = [path for path in paths if path.is_file()]
    phase_decisions = load(ROOT / 'phase-decisions.json') if (ROOT / 'phase-decisions.json').exists() else {}
    report = {'schema': 1, 'status': 'final_experiment_report' if args.final else 'draft',
        'generated_utc': datetime.now(timezone.utc).isoformat(), 'adoption_decisions': reviewed,
        'experiment': load(ROOT / 'experiment.json') if (ROOT / 'experiment.json').exists() else {},
        'method': analysis['method'], 'provenance': provenance, 'groups': analysis['groups'], 'observations': samples,
        'measurement_runs': [{'label': row['label'], 'metadata': row.get('metadata'), 'valid_evidence': row['valid_evidence'],
            'errors': row['errors'], 'raw_manifest': identity(ROOT / 'measurements' / row['label'] / 'paired-run.json')}
            for row in analysis['measurement_runs']],
        'counts': {'series': len(analysis['measurement_runs']), 'valid_series': sum(row['valid_evidence'] for row in analysis['measurement_runs']),
            'observations': len(samples), 'allocation_observations': sum(bool(item['allocations']) for item in samples),
            'e2e_observations': sum(item['stage'] in ('e2e', 'observed-e2e') for item in samples)},
        'coverage': {name: {'screen_planned_series': len(driver.series(spec, 'screen')),
            'screen_measured_series': sum(row['metadata']['comparison_id'] == name and row['metadata']['phase'] == 'screen' for row in analysis['measurement_runs']),
            'e2e_enabled_after_screen': phase_decisions.get(name, {}).get('screen_pass') is True,
            'e2e_measured_series': sum(row['metadata']['comparison_id'] == name and row['metadata']['phase'] == 'e2e' for row in analysis['measurement_runs'])}
            for name, spec in specs.items()},
        'setup_corrections': [load(path) for path in sorted(ROOT.glob('*correction*.json'))],
        'validation_failures': failures, 'reproduction_files': [identity(path) for path in paths],
        'limitations': [
            'Performance измеряется на этой Windows-машине. Linux correctness checks не являются Linux performance измерениями.',
            'Atomic/refcount операции здесь напрямую не подсчитываются. Их удаление в коде не является измеренным выигрышем; Arc/progress принимаются только по timing/CPU/cycles.',
            'Allocation builds служат только диагностикой requests/outcomes. Их wall time/CPU не используются как доказательство ускорения или регрессии.',
            'Уменьшение allocator requests/bytes не равно уменьшению live memory/RSS. Whole-phase runtime/channel allocations могут колебаться.',
            'Progress micro измеряет реальный sender guard вместе с одинаковым channel/cancellation путём; None и same-state являются отдельными отрицательными контролями.',
            'Подготовленный observed E2E использует TEMP на C:, принудительный первый snapshot и затем observer 5 Hz; его запуск зависит от screen gate и показан в coverage. Для отклонённого progress он не запускался. Обычный loopback CPU включает sender и receiver без observer.',
            'Planner resume измеряет constructor/finish/drop/join, new-* — record/drain/finish/drop. Cleanup возвращённых descriptors исключён; deallocations могут относиться к подготовленным до интервала объектам.',
            'Control sink заранее выделен; проверка packet/body/fingerprint и reference encode/decode происходят до/после интервала. Это не cold-start/network latency.',
            'Control wall/allocator интервал ограничен циклом, CPU/cycles snapshots немного шире и включают measurement hooks. RSS — пик за время процесса, включая подготовку fixture/reference. Noop не вычитается.',
            'В короткой серии commit новая петля длится около 13 ms: Windows CPU time квантован (встречаются 0 и 15,625 ms). Wall/cycles и отдельно длинная серия дают более информативную проверку; allocation timing доказательством скорости не служит.',
            'Короткие и длинные циклы не объединяются; положительная длинная серия не отменяет отрицательную короткую.',
            'Все первые samples и выбросы сохранены. Кэш/фоновые процессы не контролируются, verification может прогревать следующие samples.',
            'Strict gate: CI gain lower > matching A/A floor, либо CI upper < negative floor для регрессии. Это описательный критерий без multiplicity correction; отсутствие небольшой регрессии не доказано.',
        ] + reviewed.get('limitations', [])}
    output.mkdir(parents=True)
    for path in paths:
        destination = output / 'reproduction' / path.relative_to(ROOT)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
    save(output / 'tests/summary.json', {'commit': report['experiment'].get('final_commit'),
        'checks': checks, 'failures': failures, 'decisions': reviewed})
    save(output / 'comparison.json', report)
    (output / 'comparison.html').write_text(render(report), encoding='utf-8')
    print(json.dumps({'output': str(output), 'counts': report['counts'], 'sha256': driver.sha(output / 'comparison.json')}, indent=2))


if __name__ == '__main__':
    main()


