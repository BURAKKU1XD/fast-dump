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


def preflight(comparison, spec):
    if spec.get('enabled') is not True:
        raise ValueError('comparison disabled until its accepted baseline is recorded: ' + comparison)
    if not re.fullmatch(r'[0-9a-f]{40}', spec.get('accepted_head', '')):
        raise ValueError('accepted_head must pin the full committed baseline hash')
    compiler = subprocess.check_output(['rustc', '-vV'], text=True)
    experiment = load(RUN / 'experiment.json')
    if compiler != experiment['compiler']:
        raise ValueError('current compiler differs from experiment compiler')
    sysroot = Path(subprocess.check_output(['rustc', '--print', 'sysroot'], text=True).strip())
    compiler_binary = sysroot / 'bin/rustc.exe'
    sources = {side: source_provenance(spec[side]) for side in ('before', 'after')}
    artifacts = {}
    for side in ('before', 'after'):
        names = ('file_pipeline', 'loopback_send_dump') if comparison == 'm2' else ('control_messages',)
        artifacts[side] = {'timing': artifact_provenance(spec[side]['artifacts'], names)}
        if comparison == 'm2':
            artifacts[side]['allocation'] = artifact_provenance(spec[side]['allocation_artifacts'], ('file_pipeline',))
        original_experiment = load(resolved(spec[side].get('compiler_manifest', 'experiment.json')))
        if original_experiment['compiler'] != compiler:
            raise ValueError('artifact source experiment compiler differs: ' + side)
    scripts = {name: sha(REPO / 'scripts' / name) for name in ('bench-files-paired.py', 'bench-paired.py', 'bench_output.py')}
    return {'comparison_id': comparison, 'accepted_head': spec['accepted_head'], 'spec': spec,
            'source_hashes': {side: item['source_sha256'] for side, item in sources.items()},
            'sources': sources, 'artifacts': artifacts, 'compiler': compiler,
            'compiler_binary': {'path': str(compiler_binary), 'sha256': sha(compiler_binary),
                'capture_scope': 'observed at preflight; build compiler version is recorded separately'},
            'scripts': scripts, 'driver_sha256': sha(Path(__file__)),
            'fixtures_manifest_sha256': sha(RUN / 'fixtures.json')}


def cases(suite):
    result = []
    if suite in ('m2', 'm2-scan'):
        for fixture in ('manySmallFiles', 'wide-tree'):
            result += [('m2-aa-scan-' + fixture, fixture, 'scan', 5, False, True),
                       ('m2-ab-scan-' + fixture, fixture, 'scan', 10, False, False),
                       ('m2-alloc-scan-' + fixture, fixture, 'scan', 5, True, False)]
    if suite in ('m2', 'm2-e2e'):
        for fixture in ('manySmallFiles', 'mixed', 'bigFiles'):
            result += [('m2-aa-e2e-' + fixture, fixture, 'e2e', 5, False, True),
                       ('m2-ab-e2e-' + fixture, fixture, 'e2e', 5, False, False)]
    if suite == 'm3-control':
        result += [('m3-aa-control-plan-short-4096', 'plan-short-4096', 'control', 5, False, True),
                   ('m3-ab-control-plan-short-4096', 'plan-short-4096', 'control', 10, False, False)]
    return result


def control_case(output, binaries, hashes, scenario, repeats):
    output.mkdir(parents=True)
    common = ['--case', scenario, '--iterations', '1024', '--repeats', '1']
    manifest = {'complete': False, 'comparison_kind': 'A/A' if hashes['before'] == hashes['after'] else 'A/B',
        'binary_sha256': hashes, 'repeats': repeats, 'allocations_diagnostic': False,
        'harness': 'control_messages', 'common_arguments': common,
        'policy': 'Alternating AB/BA; all observations retained; preparation/verification outside timing.',
        'invocations': []}
    rows, identity = [], None
    for pair in range(1, repeats + 1):
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
                     if line.startswith('[control-perf] ')]
            if len(lines) != 1:
                raise ValueError('expected exactly one control result: ' + str(child))
            measured = dict(re.findall(r'([a-z_0-9]+)=([^\s]+)', lines[0]))
            contract = tuple(measured.get(key) for key in ('case', 'iterations', 'entries', 'bytes', 'fingerprint',
                'packet_bytes', 'body_bytes', 'allocator_instrumented', 'allocations_enabled',
                'counters_schema', 'cycle_source', 'io_source', 'scope', 'interval'))
            if identity is None:
                identity = contract
            if identity != contract:
                raise ValueError('control packet/config/counter contract differs across samples')
            if measured.get('verified') != 'true' or measured.get('case') != scenario or measured.get('iterations') != '1024':
                raise ValueError('unverified or unexpected control case')
            if any(measured.get(key) != 'false' for key in ('allocator_instrumented', 'allocations_enabled')):
                raise ValueError('timing binary has allocator instrumentation')
            measured.update(stage='control', readers='0')
            record.update(complete=True, measurement=measured)
            rows.append({'pair': pair, 'side': side, **measured, 'log': str(child / 'run.log')})
            save(output / 'paired-run.json', manifest)
    with (output / 'samples.csv').open('x', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest['complete'] = True
    save(output / 'paired-run.json', manifest)
    return [str(binaries['before']), *common], 0


def run_case(comparison, spec, provenance, item):
    label, fixture, stage, repeats, allocations, calibration = item
    output, logs = RUN / 'measurements' / label, RUN / 'driver-logs'
    if output.exists() or (logs / (label + '.json')).exists() or (logs / (label + '.log')).exists():
        raise ValueError('refusing to overwrite existing observation: ' + label)
    logs.mkdir(exist_ok=True)
    executable = {'scan': 'file_pipeline', 'e2e': 'loopback_send_dump', 'control': 'control_messages'}[stage]
    mode = 'allocation' if allocations else 'timing'
    artifacts = {side: provenance['artifacts']['before' if side == 'before' or calibration else 'after'][mode][executable]
                 for side in ('before', 'after')}
    binaries = {side: Path(item['path']) for side, item in artifacts.items()}
    hashes = {side: item['sha256'] for side, item in artifacts.items()}
    if (hashes['before'] == hashes['after']) != calibration:
        raise ValueError('binary identity does not match A/A or A/B design')
    for side, path in binaries.items():
        if sha(path) != hashes[side]:
            raise ValueError('artifact changed since preflight: ' + str(path))
    source_hashes = dict(provenance['source_hashes'])
    if calibration:
        source_hashes['after'] = source_hashes['before']
    metadata = {'label': label, 'comparison_id': comparison,
        'candidate': spec['baseline_name'] if calibration else spec['candidate'],
        'accepted_head': spec['accepted_head'], 'fixture': fixture, 'stage': stage,
        'allocations': allocations, 'binary_hashes': hashes, 'source_hashes': source_hashes,
        'provenance_sha256': sha(RUN / 'provenance' / (comparison + '.json')),
        'scope': 'sender_receiver_process' if stage == 'e2e' else 'isolated_sender_stage',
        'cache': 'uncontrolled; every first sample retained; verification can warm cache',
        'started_utc': datetime.now(timezone.utc).isoformat(), 'command': [], 'exit_code': None}
    save(logs / (label + '.json'), metadata)
    started = time.monotonic()
    print('START', label, flush=True)
    try:
        if stage == 'control':
            command, code = control_case(output, binaries, hashes, fixture, repeats)
        else:
            script = 'bench-paired.py' if stage == 'e2e' else 'bench-files-paired.py'
            command = [sys.executable, '-B', str(REPO / 'scripts' / script), '--before', str(binaries['before']),
                '--after', str(binaries['after']), '--output', str(output), '--source',
                load(RUN / 'fixtures.json')['paths'][fixture], '--repeats', str(repeats), '--readers', '8']
            command += (['--threads', '4', '--frames', '64', '--buffers', '1024', '--executor', 'default',
                         '--network-profile', 'loopback-localhost'] if stage == 'e2e' else ['--stages', stage])
            if allocations:
                command.append('--allocations')
            if calibration:
                command.append('--allow-identical-binaries')
            metadata['command'] = command
            save(logs / (label + '.json'), metadata)
            env = os.environ.copy()
            env['FAST_DUMP_BENCH_TEMP_DIR'] = str(RUN / 'scratch')
            with (logs / (label + '.log')).open('xb') as log:
                code = subprocess.run(command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        metadata.update(command=command, exit_code=code)
        if hashes != {side: sha(path) for side, path in binaries.items()}:
            raise ValueError('binary changed during measurement')
        if code:
            raise RuntimeError('paired driver failed with exit code ' + str(code))
    except BaseException as error:
        metadata['error'] = repr(error)
        metadata['exit_code'] = metadata['exit_code'] or -1
        raise
    finally:
        metadata.update(elapsed_s=time.monotonic() - started, completed_utc=datetime.now(timezone.utc).isoformat())
        save(logs / (label + '.json'), metadata)
    print('END', label, f'{metadata["elapsed_s"]:.1f}s', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('suite', choices=('m2', 'm2-scan', 'm2-e2e', 'm3-control'))
    parser.add_argument('--preflight-only', action='store_true')
    args = parser.parse_args()
    comparison = 'm3-control' if args.suite == 'm3-control' else 'm2'
    spec = load(RUN / 'comparison-specs.json')[comparison]
    provenance = preflight(comparison, spec)
    path = RUN / 'provenance' / (comparison + '.json')
    if path.exists() and load(path) != provenance:
        raise ValueError('comparison provenance changed after pinning; do not mix sources/heads')
    if not path.exists():
        save(path, provenance)
    selected = cases(args.suite)
    if args.preflight_only:
        print(json.dumps({'comparison': comparison, 'accepted_head': spec['accepted_head'],
            'source_hashes': provenance['source_hashes'], 'series': len(selected),
            'observations': sum(2 * item[3] for item in selected)}, indent=2))
        return
    conflicts = [item[0] for item in selected if (RUN / 'measurements' / item[0]).exists()
                 or (RUN / 'driver-logs' / (item[0] + '.json')).exists()]
    if conflicts:
        raise ValueError('suite already contains observations; choose a not-yet-run sub-suite: ' + repr(conflicts))
    # Один lock охватывает обе фазы и исключает параллельные запуски этого driver.
    lock = RUN / 'measurement.lock'
    with lock.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps({'pid': os.getpid(), 'suite': args.suite}))
    try:
        for item in selected:
            run_case(comparison, spec, provenance, item)
    finally:
        lock.unlink()


if __name__ == '__main__':
    main()
