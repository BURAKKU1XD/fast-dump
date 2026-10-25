"""Sequential incremental adoption measurements; never builds or changes sources."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n', encoding='utf-8')


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def resolved(value):
    path = Path(value)
    return path.resolve() if path.is_absolute() else (RUN / path).resolve()


def source_provenance(spec):
    source = resolved(spec['source'])
    manifest_path = resolved(spec['manifest'])
    manifest = load(manifest_path)
    if spec.get('manifest_key'):
        manifest = manifest[spec['manifest_key']]
    actual = {relative: sha(source / relative) for relative in manifest['files']}
    source_hash = hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()
    if actual != manifest['files'] or source_hash != manifest['source_sha256']:
        raise ValueError('source no longer matches manifest: ' + str(source))
    # Проверяем и новые build inputs, отсутствующие в зафиксированном manifest.
    build_paths = [path for directory in ('src', 'benches', 'examples', '.cargo')
                   for path in (source / directory).rglob('*') if path.is_file()]
    build_paths += [path for path in source.glob('*')
                    if path.name in ('Cargo.toml', 'Cargo.lock', 'build.rs') and path.is_file()]
    unexpected = sorted(path.relative_to(source).as_posix() for path in build_paths
                        if path.relative_to(source).as_posix() not in actual)
    if unexpected:
        raise ValueError('unrecorded build inputs: ' + repr(unexpected))
    return {'source': str(source), 'manifest': str(manifest_path), 'manifest_sha256': sha(manifest_path),
            'manifest_key': spec.get('manifest_key'), 'source_sha256': source_hash,
            'files': actual, 'reuse_note': spec.get('reuse_note')}


def artifact_provenance(directory, names):
    directory = resolved(directory)
    recorded = load(directory / 'hashes.json')
    result = {}
    for name in names:
        path = directory / (name + '.exe')
        actual = sha(path)
        if actual != recorded[name]:
            raise ValueError('binary differs from recorded artifact hash: ' + str(path))
        result[name] = {'path': str(path), 'sha256': actual}
    return result


def series(spec, phase):
    result = []
    for group in spec['groups']:
        if group['phase'] != phase:
            continue
        for kind, pairs in (('aa', group.get('aa_pairs', 5)), ('ab', group.get('ab_pairs', 10)),
                            ('alloc', group.get('allocation_pairs', 0))):
            if pairs:
                result.append({**group, 'label': spec['candidate'] + '-' + group['id'] + '-' + kind,
                    'repeats': pairs, 'calibration': kind == 'aa', 'allocations': kind == 'alloc'})
    return result


def preflight(comparison, spec, phase):
    if spec.get('enabled') is not True or not re.fullmatch(r'[0-9a-f]{40}', spec.get('accepted_head', '')):
        raise ValueError('comparison needs enabled=true and full accepted_head: ' + comparison)
    selected = series(spec, phase)
    if not selected:
        raise ValueError('phase has no configured series')
    compiler = subprocess.check_output(['rustc', '-vV'], text=True)
    sysroot = Path(subprocess.check_output(['rustc', '--print', 'sysroot'], text=True).strip())
    sources = {side: source_provenance(spec[side]) for side in ('before', 'after')}
    artifacts = {}
    for side in ('before', 'after'):
        artifacts[side] = {}
        for diagnostic, mode in ((False, 'timing'), (True, 'allocation')):
            names = sorted({item['executable'] for item in selected if item['allocations'] == diagnostic})
            if names:
                key = 'allocation_artifacts' if diagnostic else 'artifacts'
                artifacts[side][mode] = artifact_provenance(spec[side][key], names)
        recorded = load(resolved(spec[side]['compiler_manifest']))
        if recorded['compiler'] != compiler:
            raise ValueError('artifact compiler identity differs: ' + side)
    pin_spec = {key: value for key, value in spec.items() if key != 'groups'}
    pin_spec['groups'] = [group for group in spec['groups'] if group['phase'] == phase]
    return {'comparison_id': comparison, 'phase': phase, 'accepted_head': spec['accepted_head'],
        'spec': pin_spec, 'series': selected, 'sources': sources,
        'source_hashes': {side: item['source_sha256'] for side, item in sources.items()}, 'artifacts': artifacts,
        'compiler': compiler, 'compiler_binary': {'path': str(sysroot / 'bin/rustc.exe'),
            'sha256': sha(sysroot / 'bin/rustc.exe'), 'capture_scope': 'observed at preflight, not at compilation'},
        'scripts': {name: sha(REPO / 'scripts' / name) for name in ('bench-files-paired.py', 'bench-paired.py', 'bench_output.py')},
        'driver_sha256': sha(Path(__file__)), 'fixtures_manifest_sha256': sha(RUN / 'fixtures.json')}


def tagged_case(output, binaries, hashes, item):
    output.mkdir(parents=True)
    common = list(item.get('arguments', []))
    if item['allocations']:
        common = list(item.get('allocation_arguments', common)) + ['--allocations']
    manifest = {'complete': False, 'comparison_kind': 'A/A' if item['calibration'] else 'A/B',
        'binary_sha256': hashes, 'repeats': item['repeats'], 'allocations_diagnostic': item['allocations'],
        'harness': item['executable'], 'common_arguments': common,
        'policy': 'Alternating AB/BA; every observation retained; no dropped warmup or outliers.', 'invocations': []}
    rows, identity = [], None
    for pair in range(1, item['repeats'] + 1):
        for side in (('before', 'after') if pair % 2 else ('after', 'before')):
            child = output / 'runs' / f'{pair:02}-{side}'
            child.mkdir(parents=True)
            command = [str(binaries[side]), *common]
            record = {'pair': pair, 'side': side, 'command': command, 'log': str(child / 'run.log')}
            manifest['invocations'].append(record)
            save(output / 'paired-run.json', manifest)
            with (child / 'run.log').open('xb') as log:
                code = subprocess.run(command, cwd=REPO, stdout=log, stderr=subprocess.STDOUT).returncode
            record['exit_code'] = code
            if code:
                save(output / 'paired-run.json', manifest)
                return [str(binaries['before']), *common], code
            lines = [line for line in (child / 'run.log').read_text(encoding='utf-8').splitlines()
                     if line.startswith('[' + item['tag'] + '] ')]
            if len(lines) != 1:
                raise ValueError('expected one tagged measurement: ' + str(child))
            measured = dict(re.findall(r'([a-z_0-9]+)=([^\s]+)', lines[0]))
            record['measurement'] = measured
            save(output / 'paired-run.json', manifest)
            if str(measured.get('verified')).lower() != 'true':
                raise ValueError('measurement was not verified')
            for key in ('allocator_instrumented', 'allocations_enabled'):
                if str(measured.get(key)).lower() != str(item['allocations']).lower():
                    raise ValueError('allocator instrumentation differs: ' + key)
            expected = item.get('allocation_expected', item.get('expected', {})) if item['allocations'] else item.get('expected', {})
            for key, value in expected.items():
                if measured.get(key) != str(value):
                    raise ValueError('unexpected measurement contract: ' + key)
            keys = ('stage', 'case', 'iterations', 'entries', 'files', 'bytes', 'fingerprint', 'packet_bytes', 'body_bytes',
                    'scope', 'interval', 'cleanup', 'readers', 'threads', 'level', 'frame_target_mb', 'observer_hz',
                    'allocator_instrumented', 'allocations_enabled', 'counters_schema', 'cycle_source', 'io_source',
                    'terminal_skips', 'outgoing_skips', 'raw_read_bytes', 'skipped_files', 'frame_aborts', 'frame_rebuilds')
            contract = tuple(measured.get(key) for key in keys)
            if identity is None:
                identity = contract
            if identity != contract:
                raise ValueError('fixture/case/verification/counter identity changed within series')
            record.update(complete=True)
            rows.append({'pair': pair, 'side': side, **measured, 'log': str(child / 'run.log')})
            save(output / 'paired-run.json', manifest)
    with (output / 'samples.csv').open('x', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest['complete'] = True
    save(output / 'paired-run.json', manifest)
    return [str(binaries['before']), *common], 0


def run_case(comparison, phase, spec, provenance, item):
    label = item['label']
    output, logs = RUN / 'measurements' / label, RUN / 'driver-logs'
    if output.exists() or (logs / (label + '.json')).exists() or (logs / (label + '.log')).exists():
        raise ValueError('refusing overwrite: ' + label)
    logs.mkdir(exist_ok=True)
    mode = 'allocation' if item['allocations'] else 'timing'
    artifacts = {side: provenance['artifacts']['before' if side == 'before' or item['calibration'] else 'after'][mode][item['executable']]
                 for side in ('before', 'after')}
    binaries = {side: Path(value['path']) for side, value in artifacts.items()}
    hashes = {side: value['sha256'] for side, value in artifacts.items()}
    if (hashes['before'] == hashes['after']) != item['calibration']:
        raise ValueError('unexpected identical/different binaries')
    if hashes != {side: sha(path) for side, path in binaries.items()}:
        raise ValueError('artifact changed since preflight')
    source_hashes = dict(provenance['source_hashes'])
    if item['calibration']:
        source_hashes['after'] = source_hashes['before']
    metadata = {'label': label, 'comparison_id': comparison, 'phase': phase, 'group_id': item['id'],
        'candidate': spec.get('baseline_name', 'baseline') if item['calibration'] else spec['candidate'],
        'accepted_head': spec['accepted_head'], 'fixture': item['fixture'], 'stage': item['stage'],
        'adapter': item['adapter'], 'allocations': item['allocations'], 'binary_hashes': hashes,
        'source_hashes': source_hashes, 'scope': item['scope'], 'group_spec': item,
        'provenance_file': f'{comparison}-{phase}.json',
        'provenance_sha256': sha(RUN / 'provenance' / f'{comparison}-{phase}.json'),
        'started_utc': datetime.now(timezone.utc).isoformat(), 'command': [], 'exit_code': None,
        'cache': 'uncontrolled; first samples retained; preparation/verification can warm later samples'}
    save(logs / (label + '.json'), metadata)
    started = time.monotonic()
    print('START', label, flush=True)
    try:
        if item['adapter'] == 'tagged':
            command, code = tagged_case(output, binaries, hashes, item)
        else:
            script = 'bench-paired.py' if item['adapter'] == 'e2e' else 'bench-files-paired.py'
            command = [sys.executable, '-B', str(REPO / 'scripts' / script), '--before', str(binaries['before']),
                '--after', str(binaries['after']), '--output', str(output), '--source',
                load(RUN / 'fixtures.json')['paths'][item['fixture']], '--repeats', str(item['repeats']),
                *item.get('arguments', [])]
            if item['adapter'] == 'file':
                command += ['--stages', item['stage']]
            if item['allocations']:
                command += ['--allocations']
            if item['calibration']:
                command += ['--allow-identical-binaries']
            metadata['command'] = command
            save(logs / (label + '.json'), metadata)
            env = dict(os.environ, FAST_DUMP_BENCH_TEMP_DIR=str(RUN / 'scratch'))
            with (logs / (label + '.log')).open('xb') as log:
                code = subprocess.run(command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        metadata.update(command=command, exit_code=code)
        if hashes != {side: sha(path) for side, path in binaries.items()}:
            raise ValueError('binary changed during measurement')
        if code:
            raise RuntimeError('paired driver failed: ' + str(code))
    except BaseException as error:
        metadata.update(error=repr(error), exit_code=metadata['exit_code'] or -1)
        raise
    finally:
        metadata.update(elapsed_s=time.monotonic() - started, completed_utc=datetime.now(timezone.utc).isoformat())
        save(logs / (label + '.json'), metadata)
    print('END', label, f'{metadata["elapsed_s"]:.1f}s', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('comparison')
    parser.add_argument('--phase', choices=('screen', 'e2e'), default='screen')
    parser.add_argument('--group')
    parser.add_argument('--preflight-only', action='store_true')
    parser.add_argument('--plan', action='store_true')
    args = parser.parse_args()
    spec = load(RUN / 'comparisons.json')[args.comparison]
    selected = [item for item in series(spec, args.phase) if not args.group or item['id'] == args.group]
    if not selected:
        raise ValueError('no selected series')
    if args.plan:
        print(json.dumps(selected, indent=2))
        return
    if args.phase == 'e2e':
        decision = load(RUN / 'phase-decisions.json').get(args.comparison, {})
        if decision.get('screen_pass') is not True or not decision.get('reason'):
            raise ValueError('E2E requires recorded human screening decision')
    provenance = preflight(args.comparison, spec, args.phase)
    path = RUN / 'provenance' / f'{args.comparison}-{args.phase}.json'
    if path.exists() and load(path) != provenance:
        raise ValueError('pinned provenance changed; do not mix source/artifacts/config')
    if not path.exists():
        save(path, provenance)
    if args.preflight_only:
        print(json.dumps({'comparison': args.comparison, 'phase': args.phase,
            'source_hashes': provenance['source_hashes'], 'series': len(selected),
            'observations': sum(2 * item['repeats'] for item in selected)}, indent=2))
        return
    if any((RUN / 'measurements' / item['label']).exists() or (RUN / 'driver-logs' / (item['label'] + '.json')).exists() for item in selected):
        raise ValueError('selected series already contains observations')
    lock = RUN / 'measurement.lock'
    with lock.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps({'pid': os.getpid(), 'comparison': args.comparison, 'phase': args.phase}))
    try:
        for item in selected:
            run_case(args.comparison, args.phase, spec, provenance, item)
    finally:
        lock.unlink()


if __name__ == '__main__':
    main()


