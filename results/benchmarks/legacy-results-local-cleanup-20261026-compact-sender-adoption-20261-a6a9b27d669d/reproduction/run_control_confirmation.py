"""One predeclared longer-loop M3 confirmation; original screening remains immutable."""
import argparse
import csv
import json
from pathlib import Path
import re
import subprocess
import os

import run_measurements as driver

RUN = Path(__file__).resolve().parent
COMPARISON = 'm3-control-confirm'
ITERATIONS = 16384


def cases():
    return [('m3-confirm-aa-control-plan-short-4096', 'plan-short-4096', 'control', 5, False, True),
            ('m3-confirm-ab-control-plan-short-4096', 'plan-short-4096', 'control', 10, False, False)]


def preflight(spec):
    result = driver.preflight(COMPARISON, spec)
    result['confirmation_driver_sha256'] = driver.sha(Path(__file__))
    result['iterations'] = ITERATIONS
    return result


def control_case(output, binaries, hashes, scenario, repeats):
    output.mkdir(parents=True)
    common = ['--case', scenario, '--iterations', str(ITERATIONS), '--repeats', '1']
    manifest = {'complete': False, 'comparison_kind': 'A/A' if hashes['before'] == hashes['after'] else 'A/B',
        'binary_sha256': hashes, 'repeats': repeats, 'allocations_diagnostic': False,
        'harness': 'control_messages', 'common_arguments': common,
        'policy': 'Fixed focused confirmation selected after noisy 1024-iteration screen. Alternating AB/BA; all samples retained; no warmup dropping.',
        'invocations': []}
    rows, identity = [], None
    for pair in range(1, repeats + 1):
        for side in (('before', 'after') if pair % 2 else ('after', 'before')):
            child = output / 'runs' / f'{pair:02}-{side}'
            child.mkdir(parents=True)
            command = [str(binaries[side]), *common]
            record = {'pair': pair, 'side': side, 'command': command, 'log': str(child / 'run.log')}
            manifest['invocations'].append(record)
            driver.save(output / 'paired-run.json', manifest)
            with (child / 'run.log').open('xb') as log:
                code = subprocess.run(command, cwd=driver.REPO, stdout=log, stderr=subprocess.STDOUT).returncode
            record['exit_code'] = code
            if code:
                driver.save(output / 'paired-run.json', manifest)
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
            if measured.get('verified') != 'true' or measured.get('case') != scenario or measured.get('iterations') != str(ITERATIONS):
                raise ValueError('unverified or unexpected control case')
            if any(measured.get(key) != 'false' for key in ('allocator_instrumented', 'allocations_enabled')):
                raise ValueError('timing binary has allocator instrumentation')
            measured.update(stage='control', readers='0')
            record.update(complete=True, measurement=measured)
            rows.append({'pair': pair, 'side': side, **measured, 'log': str(child / 'run.log')})
            driver.save(output / 'paired-run.json', manifest)
    with (output / 'samples.csv').open('x', newline='', encoding='utf-8') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    manifest['complete'] = True
    driver.save(output / 'paired-run.json', manifest)
    return [str(binaries['before']), *common], 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preflight-only', action='store_true')
    args = parser.parse_args()
    spec = driver.load(RUN / 'control-confirmation-spec.json')
    provenance = preflight(spec)
    path = RUN / 'provenance' / (COMPARISON + '.json')
    if path.exists() and driver.load(path) != provenance:
        raise ValueError('confirmation provenance changed after pinning')
    if not path.exists():
        driver.save(path, provenance)
    if args.preflight_only:
        print(json.dumps({'comparison': COMPARISON, 'accepted_head': spec['accepted_head'],
            'iterations': ITERATIONS, 'series': 2, 'observations': 30}, indent=2))
        return
    if any((RUN / 'measurements' / item[0]).exists() or (RUN / 'driver-logs' / (item[0] + '.json')).exists()
           for item in cases()):
        raise ValueError('confirmation has prior observations; refusing overwrite')
    # Используем неизменённые запись метаданных, checksums и последовательный запуск.
    driver.control_case = control_case
    lock = RUN / 'measurement.lock'
    with lock.open('x', encoding='utf-8') as stream:
        stream.write(json.dumps({'pid': os.getpid(), 'suite': COMPARISON}))
    try:
        for item in cases():
            driver.run_case(COMPARISON, spec, provenance, item)
    finally:
        lock.unlink()


if __name__ == '__main__':
    main()
