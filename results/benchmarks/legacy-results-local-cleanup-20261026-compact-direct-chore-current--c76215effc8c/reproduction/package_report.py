"""Preserve the direct exact-commit comparison; never build or run benchmarks."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import html
import json
from pathlib import Path
import re
import shutil

import run_measurements as driver

ROOT = Path(__file__).resolve().parent
COMMITS = {'before': '4c58969a1ac1d4fbf18675077cac105485f2f2ab',
           'after': 'bb5004255e8cc23c5d7543d5e6d6f4499960b27f'}
FIXTURES = ('manySmallFiles', 'mixed', 'bigFiles')


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def identity(path):
    return {'path': path.relative_to(ROOT).as_posix(), 'sha256': driver.sha(path), 'bytes': path.stat().st_size}


def preserve(path, output):
    original = identity(path)
    compressed = path.name in ('fixture-before.json', 'fixture-after.json')
    relative = original['path'] + ('.gz' if compressed else '')
    destination = output / 'reproduction' / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if not compressed:
        shutil.copyfile(path, destination)
        return original
    # Полный proof остаётся проверяемым, но не добавляет в tracked results десятки MB JSON.
    with path.open('rb') as source, destination.open('xb') as stored:
        with gzip.GzipFile(filename='', mode='wb', fileobj=stored, compresslevel=6, mtime=0) as stream:
            shutil.copyfileobj(source, stream)
    digest = hashlib.sha256()
    with gzip.open(destination, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    if digest.hexdigest() != original['sha256']:
        raise ValueError('compressed proof does not roundtrip: ' + relative)
    return {'path': relative, 'sha256': driver.sha(destination), 'bytes': destination.stat().st_size,
            'encoding': 'gzip; mtime=0; empty original filename', 'source_path': original['path'],
            'source_sha256': original['sha256'], 'source_bytes': original['bytes'],
            'decompression_sha256_verified': True}


def validate(analysis, spec, final):
    wanted = {item['label']: item for item in driver.series(spec, 'screen')}
    runs = analysis['measurement_runs']
    if final and ({row['label'] for row in runs} != set(wanted) or len(runs) != 6):
        raise ValueError('final report needs exactly six planned series')
    counts = Counter()
    archive_checks = 0
    for row in runs:
        if not row['valid_evidence'] or not row['complete'] or row['errors']:
            raise ValueError('invalid/incomplete series: ' + row['label'])
        expected = wanted[row['label']]
        observations = row['observations']
        if len(observations) != 2 * expected['repeats']:
            raise ValueError('unexpected observation count: ' + row['label'])
        pairs = {(item['pair'], item['side']) for item in observations}
        if pairs != {(pair, side) for pair in range(1, expected['repeats'] + 1) for side in ('before', 'after')}:
            raise ValueError('missing/duplicate pair side: ' + row['label'])
        payload_identities = set()
        for item in observations:
            raw, metrics = item['raw'], item['metrics']
            counts[expected['fixture']] += 1
            if item['errors'] or any(int(raw[key]) for key in ('skipped_files', 'frame_aborts', 'frame_rebuilds')):
                raise ValueError('unclean static fixture sample')
            if (raw['threads'], raw['frame_target_mb'], raw['read_buffer_kb'], raw['protocol_runtime'], raw['network_profile']) != ('4', '64', '1024', 'v4', 'loopback-localhost'):
                raise ValueError('unexpected runtime configuration')
            log = Path(item['log']).read_text(encoding='utf-8', errors='replace')
            verified = re.findall(r'^\[bench-verify\] verified (\d+) entries, (\d+) regular-file bytes', log, re.M)
            if len(verified) != 1 or tuple(map(int, verified[0])) != (int(raw['files_count']), int(raw['raw_read_bytes'])):
                raise ValueError('archive verification disagrees with raw measurements')
            if re.findall(r'^\[bench-readers\] requested=(\d+)$', log, re.M) != ['8']:
                raise ValueError('reader override not confirmed by actual run log')
            archive_checks += 1
            if metrics['cpu_ms'] != metrics['user_ms'] + metrics['kernel_ms']:
                raise ValueError('CPU total differs from user + kernel')
            if item['contract']['cpu_scope'] != 'sender_receiver_process':
                raise ValueError('unexpected CPU scope')
            payload_identities.add(tuple(raw[key] for key in ('files_count', 'raw_read_bytes', 'compressed_payload_bytes', 'sent_frames')))
        if len(payload_identities) != 1:
            raise ValueError('fixture/payload/frame counts changed within series')
    if final and (archive_checks != 90 or counts != Counter({name: 30 for name in FIXTURES})):
        raise ValueError('expected 90 verified archives, 30 per fixture')
    analysis_time = (ROOT / 'analysis.json').stat().st_mtime_ns
    for directory in ('measurements', 'driver-logs'):
        for path in (ROOT / directory).rglob('*'):
            if path.is_file() and path.stat().st_mtime_ns > analysis_time:
                raise ValueError('analysis predates measurement: ' + str(path))
    return {'series': len(runs), 'observations': archive_checks,
            'verified_archives': archive_checks, 'observations_per_fixture': dict(counts)}


def provenance(analysis, spec):
    pin_path = ROOT / 'provenance/direct-screen.json'
    pin = load(pin_path)
    if driver.preflight('direct', spec, 'screen') != pin:
        raise ValueError('source/binary/compiler/driver differs from pinned provenance')
    for side, commit in COMMITS.items():
        source = load(ROOT / (side + '-source.json'))
        if source.get('commit') != commit or source.get('exact_git_blob_contents') is not True:
            raise ValueError('source is not the exact requested commit: ' + side)
        environment = load(driver.resolved(spec[side]['compiler_manifest']))
        if environment.get('source_sha256') != source['source_sha256'] or environment.get('compiler_binary_sha256') != pin['compiler_binary']['sha256']:
            raise ValueError('build-time source/compiler hash differs: ' + side)
        if environment.get('package_artifacts_seeded') is not False:
            raise ValueError('package artifacts were reused: ' + side)
    for row in analysis['measurement_runs']:
        meta = row['metadata']
        if meta['provenance_sha256'] != driver.sha(pin_path):
            raise ValueError('run pin hash differs')
        expected = {side: pin['artifacts'][('before' if row['comparison_kind'] == 'A/A' else side)]['timing']['loopback_send_dump']['sha256'] for side in ('before', 'after')}
        if meta['binary_hashes'] != expected:
            raise ValueError('run executable hash differs')
    return {'pin': pin, 'identity': identity(pin_path),
            'source_manifests': {side: load(ROOT / (side + '-source.json')) for side in COMMITS},
            'build_environments': {side: load(driver.resolved(spec[side]['compiler_manifest'])) for side in COMMITS}}


def checks():
    result = {}
    for path in sorted((ROOT / 'checks').rglob('checks.json')):
        rows = load(path)
        for row in rows:
            log = path.parent / (row['check'] + '.log')
            if not log.exists() and row['check'] == 'release-build':
                log = path.parent / 'build.log'
            if log.exists():
                row['log'] = identity(log)
        result[path.relative_to(ROOT / 'checks').as_posix()] = {'source': identity(path), 'observations': rows}
    return result


def compact_observations(analysis):
    result = []
    for row in analysis['measurement_runs']:
        meta = row['metadata']
        for item in row['observations']:
            result.append({'label': row['label'], 'comparison_kind': row['comparison_kind'],
                'fixture': meta['fixture'], 'scope': meta['scope'], 'source_hashes': meta['source_hashes'],
                'binary_hashes': meta['binary_hashes'], 'valid_evidence': row['valid_evidence'],
                **{key: item[key] for key in ('pair', 'side', 'metrics', 'raw', 'resources', 'config', 'contract', 'errors')}})
    return result


def fixture_proofs(spec, final):
    result = {}
    for name in ('fixture-before.json', 'fixture-after.json'):
        path = ROOT / name
        if not path.exists():
            if final:
                raise ValueError('missing fixture proof: ' + name)
            continue
        proof = load(path)
        # Полные records сохраняются один раз в attachments; в отчёте — identity и counts.
        result[name] = {'evidence': identity(path),
            **{key: value for key, value in proof.items() if key != 'fixtures'},
            'fixtures': {key: {field: value for field, value in item.items() if field != 'records'}
                         for key, item in proof['fixtures'].items()}}
    before = result['fixture-before.json']
    pinned = spec['fixture_before']
    if before['evidence']['sha256'] != pinned['sha256'] or before['combined_sha256'] != pinned['combined_sha256'] or before['script_sha256'] != pinned['script_sha256']:
        raise ValueError('before fixture proof differs from pinned comparison spec')
    if before['fixtures_config_sha256'] != driver.sha(ROOT / 'fixtures.json'):
        raise ValueError('fixture paths/config changed')
    if 'fixture-after.json' in result:
        after = result['fixture-after.json']
        if (after.get('comparison', {}).get('content_identity_equal') is not True
                or after['comparison']['before_manifest_sha256'] != before['evidence']['sha256']
                or after['combined_sha256'] != before['combined_sha256']
                or after['fixtures_config_sha256'] != before['fixtures_config_sha256']
                or after['script_sha256'] != before['script_sha256']):
            raise ValueError('fixture identity changed between before/after proofs')
    return result


def fmt(value):
    return '—' if value is None else f'{value:.3f}'


def render(report):
    esc = html.escape
    decisions = {item['fixture']: item for item in report['reviewed_conclusion'].get('fixtures', [])}
    rows = []
    for group in report['groups']:
        if group['comparison_kind'] != 'A/B':
            continue
        m = group['metrics']; wall = m['elapsed_ms']; speed = m['total_mib_s']; reviewed = decisions.get(group['fixture'], {})
        values = [group['fixture'], f"{fmt(wall['before_mean'])} → {fmt(wall['after_mean'])}",
                  f"{fmt(wall['before_median'])} → {fmt(wall['after_median'])}",
                  fmt(wall['improvement_pct']), ' .. '.join(fmt(x) for x in wall['ci95_improvement_pct']),
                  fmt(wall['aa_noise_floor_pct']), f"{fmt(speed['before_mean'])} → {fmt(speed['after_mean'])}",
                  f"{fmt(speed['before_median'])} → {fmt(speed['after_median'])}",
                  reviewed.get('verdict', 'Ожидает оценки'), reviewed.get('evidence', '')]
        rows.append('<tr>' + ''.join('<td>' + esc(str(value)) + '</td>' for value in values) + '</tr>')
    detail = []
    for group in report['groups']:
        for metric in ('elapsed_ms', 'total_mib_s', 'first_bundle_ms', 'cpu_ms', 'user_ms', 'kernel_ms', 'cpu_cycles', 'rss_mib'):
            item = group['metrics'][metric]; ci = item.get('ci95_improvement_pct')
            values = [group['fixture'], group['comparison_kind'], metric, item['paired_n'],
                      fmt(item.get('before_mean')), fmt(item.get('after_mean')), fmt(item.get('before_median')), fmt(item.get('after_median')),
                      fmt(item.get('improvement_pct')), '—' if ci is None else ' .. '.join(fmt(x) for x in ci),
                      fmt(item.get('aa_noise_floor_pct', item.get('noise_floor_pct')))]
            detail.append('<tr>' + ''.join('<td>' + esc(str(value)) + '</td>' for value in values) + '</tr>')
    def table(headers, body):
        return '<div class="scroll"><table><tr>' + ''.join('<th>' + esc(x) + '</th>' for x in headers) + '</tr>' + ''.join(body) + '</table></div>'
    top_headers = ('Fixture', 'Среднее время, ms', 'Медиана времени, ms', 'Сокращение времени, %', 'CI95, %', 'A/A, %',
                   'Средняя скорость, MiB/s', 'Медиана скорости, MiB/s', 'Вывод', 'Основание')
    metric_headers = ('Fixture', 'Серия', 'Метрика', 'Пар', 'Среднее до', 'Среднее после', 'Медиана до', 'Медиана после', 'Эффект, %', 'CI95, %', 'A/A, %')
    return ('<!doctype html><html lang="ru"><meta charset="utf-8"><title>Прямое сравнение fast-dump</title>'
        '<style>body{font:15px system-ui;margin:24px;color:#17212b}table{border-collapse:collapse;font-variant-numeric:tabular-nums}'
        'td,th{border:1px solid #cbd5df;padding:7px;vertical-align:top;text-align:left}th{background:#edf3f8}.scroll{overflow-x:auto}'
        'pre{white-space:pre-wrap;font-size:12px}a{color:#135ba1}</style><h1>Прямое сравнение fast-dump</h1>'
        '<p><strong>' + esc(report['reviewed_conclusion'].get('conclusion', 'Ожидает итоговой оценки.')) + '</strong></p>'
        '<p>До: <code>' + COMMITS['before'] + '</code> — chore: reorganize project and preserve benchmark history.<br>'
        'После: <code>' + COMMITS['after'] + '</code>. Свежие release-сборки точных исходников; harness одинаковый, дополнительных патчей нет.</p>'
        '<p>Windows, localhost; readers 8, threads 4, zstd level 3, frame 64 MiB, buffer 1024 KiB. На каждый fixture: 5 A/A и 10 A/B пар с AB/BA; '
        'сохранены все 90 прогонов и проверены все 90 архивов. Проценты прежних микрооптимизаций не складываются.</p>' + table(top_headers, rows) +
        '<p>Положительный эффект означает уменьшение затрат; для MiB/s — увеличение скорости. CI относится к отношению средних по парному bootstrap. '
        'Медианы приведены описательно. Основной критерий — время всей передачи; отсутствие убедительного эффекта не доказывает равенство версий.</p>'
        '<p><a href="comparison.json">Все исходные наблюдения, метрики и provenance</a> · <a href="tests/summary.json">Сборки и проверка архивов</a></p>'
        '<h2>Полные метрики</h2>' + table(metric_headers, detail) + '<h2>Ограничения</h2><ul>' +
        ''.join('<li>' + esc(value) + '</li>' for value in report['limitations']) + '</ul><h2>Воспроизведение</h2><ul>' +
        ''.join('<li><a href="reproduction/' + esc(item['path']) + '">' + esc(item['path']) + '</a></li>' for item in report['reproduction_files']) +
        '</ul><details><summary>Source/compiler/binary hashes</summary><pre>' + esc(json.dumps(report['provenance'], ensure_ascii=False, indent=2)) +
        '</pre></details></html>')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--final', action='store_true')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    output = (args.output or ROOT / ('report-final' if args.final else 'report-draft')).resolve()
    if output.exists():
        raise ValueError('refusing to overwrite report')
    analysis = load(ROOT / 'analysis.json'); spec = load(ROOT / 'comparisons.json')['direct']
    counts = validate(analysis, spec, args.final)
    reviewed = load(ROOT / 'reviewed-conclusion.json') if (ROOT / 'reviewed-conclusion.json').exists() else {}
    if args.final and (not reviewed.get('conclusion') or {x.get('fixture') for x in reviewed.get('fixtures', [])} != set(FIXTURES)):
        raise ValueError('final report needs reviewed-conclusion.json with conclusion and three fixture verdicts')
    evidence = provenance(analysis, spec)
    fixture_evidence = fixture_proofs(spec, args.final)
    paths = sorted(path for path in ROOT.iterdir() if path.is_file() and path.suffix in ('.py', '.json') and path.name != 'analysis.json')
    paths += sorted((ROOT / 'provenance').glob('*.json'))
    paths += sorted(path for path in (ROOT / 'checks').rglob('*.json') if path.is_file())
    build_checks = checks()
    output.mkdir(parents=True)
    preserved = [preserve(path, output) for path in paths]
    report = {'schema': 1, 'status': 'final_direct_comparison' if args.final else 'draft',
        'generated_utc': datetime.now(timezone.utc).isoformat(), 'commits': COMMITS,
        'experiment': load(ROOT / 'experiment.json') if (ROOT / 'experiment.json').exists() else {},
        'reviewed_conclusion': reviewed, 'method': analysis['method'], 'counts': counts,
        'groups': analysis['groups'], 'observations': compact_observations(analysis),
        'measurement_runs': [{'label': row['label'], 'metadata': row['metadata'], 'valid_evidence': row['valid_evidence'],
            'raw_manifest': identity(ROOT / 'measurements' / row['label'] / 'paired-run.json')} for row in analysis['measurement_runs']],
        'provenance': evidence, 'fixture_proofs': fixture_evidence, 'reproduction_files': preserved,
        'limitations': [
            'Performance относится к этой Windows-машине и localhost. Это не Linux performance, не WAN и не раздельные sender/receiver процессы.',
            'Wall time — total sender profile после установления транспорта до завершения передачи. Подготовка fixture и проверка архива находятся вне интервала.',
            'CPU user/kernel и process cycles включают sender+receiver. Cycles не являются instructions и не переводятся в секунды; монитор процесса также работает в общем процессе.',
            'RSS — максимум выборок рабочего набора общего процесса во время передачи, интервал по умолчанию 50 ms; краткие пики могут быть пропущены. Это не число живых allocator bytes.',
            'first_bundle_ms — готовность первой materialized/streamed bundle-задачи, до compression, доставки payload и acknowledgement.',
            'Кэш и фоновые процессы не контролируются. Content hashing до/после серии и archive verification могут прогреть последующие прогоны. Все первые наблюдения и выбросы сохранены.',
            'Среднее MiB/s — среднее отдельных скоростей прогонов; оно не равно logical bytes / среднее время. Основной критерий времени анализируется отдельно.',
            'Strict gate: CI95 lower эффекта должен превышать соответствующий A/A floor; для регрессии CI95 upper должен быть ниже отрицательного floor. Это описательный критерий без multiplicity correction.',
            'Нет отдельного профиля terminal skips/resume. Обычные static fixtures не проверяют специализированный выигрыш счётчика на каталогах пропусков.',
            'Stage durations могут перекрываться и не складываются в total. compressed_payload_bytes не объявляется точным размером archive без отдельного подтверждения.',
        ] + reviewed.get('limitations', [])}
    save(output / 'tests/summary.json', {'commits': COMMITS, 'checks': build_checks, 'archive_validation': counts})
    save(output / 'comparison.json', report)
    (output / 'comparison.html').write_text(render(report), encoding='utf-8')
    print(json.dumps({'output': str(output), 'counts': counts, 'sha256': driver.sha(output / 'comparison.json')}, indent=2))


if __name__ == '__main__':
    main()
