"""Run fixed-config independent trials; never overlap measurements with builds."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]


def run_case(label, candidate, fixture, stage='e2e', repeats=5, identical=False):
    output = RUN / 'measurements' / label
    if output.exists():
        raise RuntimeError(f'refusing to overwrite {output}')
    executable = 'loopback_send_dump.exe' if stage == 'e2e' else 'file_pipeline.exe'
    before = RUN / 'artifacts-isolated/baseline' / executable
    after = RUN / 'artifacts-isolated' / candidate / executable
    script = 'bench-paired.py' if stage == 'e2e' else 'bench-files-paired.py'
    command = [sys.executable, '-B', str(REPO / 'scripts' / script),
               '--before', str(before), '--after', str(after), '--output', str(output),
               '--source', str(RUN / 'fixtures' / fixture), '--repeats', str(repeats),
               '--readers', '8']
    if stage == 'e2e':
        command += ['--threads', '4', '--frames', '64', '--buffers', '1024',
                    '--executor', 'default', '--network-profile', 'loopback-localhost']
    else:
        command += ['--stages', stage]
    if identical:
        command += ['--allow-identical-binaries']
    environment = os.environ.copy()
    environment['FAST_DUMP_BENCH_DATASET_DIR'] = str(RUN / 'fixtures')
    environment['FAST_DUMP_BENCH_TEMP_DIR'] = str(RUN / 'scratch')
    logs = RUN / 'driver-logs'
    logs.mkdir(exist_ok=True)
    started = time.time()
    print(f'START {label}', flush=True)
    with (logs / (label + '.log')).open('wb') as log:
        completed = subprocess.run(command, cwd=REPO, env=environment,
                                   stdout=log, stderr=subprocess.STDOUT)
    metadata = {'label': label, 'candidate': candidate, 'fixture': fixture,
                'stage': stage, 'command': command, 'exit_code': completed.returncode,
                'elapsed_s': time.time() - started,
                'binary_hashes': {name: hashlib.sha256(path.read_bytes()).hexdigest()
                                  for name, path in [('before', before), ('after', after)]},
                'scope': 'sender_receiver_process' if stage == 'e2e' else 'isolated_sender_stage',
                'cache': 'uncontrolled; all first samples retained; verification can warm cache'}
    (logs / (label + '.json')).write_text(json.dumps(metadata, indent=2) + '\n')
    if completed.returncode:
        print((logs / (label + '.log')).read_text(errors='replace')[-6000:], flush=True)
        raise RuntimeError(label)
    print(f'END {label} {metadata["elapsed_s"]:.1f}s', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=['calibration', 'primary', 'case'])
    parser.add_argument('--label')
    parser.add_argument('--candidate')
    parser.add_argument('--fixture')
    parser.add_argument('--stage', default='e2e')
    parser.add_argument('--repeats', type=int, default=5)
    args = parser.parse_args()
    if args.suite == 'calibration':
        for fixture in ['manySmallFiles', 'startup-tree', 'bigFiles', 'incompressible', 'medium-prefix']:
            run_case('aa-' + fixture, 'baseline', fixture, identical=True)
        for fixture in ['manySmallFiles', 'medium-prefix']:
            run_case('aa-read-' + fixture, 'baseline', fixture, 'read', identical=True)
    elif args.suite == 'primary':
        for label, candidate, fixture, stage in [
            ('exp2-small', 'exp2-metadata', 'manySmallFiles', 'e2e'),
            ('exp2-read-small', 'exp2-metadata', 'manySmallFiles', 'read'),
            ('exp3-small', 'exp3-refill', 'manySmallFiles', 'e2e'),
            ('exp3-startup', 'exp3-refill', 'startup-tree', 'e2e'),
            ('exp4-big', 'exp4-double-buffer', 'bigFiles', 'e2e'),
            ('exp4-random', 'exp4-double-buffer', 'incompressible', 'e2e'),
            ('exp5-medium', 'exp5-prefix', 'medium-prefix', 'e2e'),
            ('exp5-read-medium', 'exp5-prefix', 'medium-prefix', 'read'),
        ]:
            run_case(label, candidate, fixture, stage)
    else:
        run_case(args.label, args.candidate, args.fixture, args.stage, args.repeats)
