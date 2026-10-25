import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]

def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')

def hashes(source):
    return {p.relative_to(source).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(source.rglob('*')) if p.is_file()
            and not {'.git', 'target', 'results', '__pycache__'}.intersection(p.relative_to(source).parts)}

def snapshot(name, origin=None):
    dest = RUN / 'sources' / name
    if dest.exists():
        raise RuntimeError('snapshot already exists: ' + str(dest))
    if origin:
        shutil.copytree(origin, dest)
    else:
        paths = subprocess.check_output(['git', 'ls-files', '-z'], cwd=REPO).decode().split('\0')
        for relative in paths:
            if not relative or relative.startswith(('results/', '.git/')):
                continue
            src = REPO / relative
            if src.is_file():
                out = dest / relative
                out.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, out)
    record_source(name)

def record_source(name):
    files = hashes(RUN / 'sources' / name)
    save(RUN / (name + '-source.json'), {'files': files,
        'source_sha256': hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()})

def ignores(source):
    names = {'fast-dump', 'fast_dump', 'libfast_dump', 'control_messages', 'progress_micro', 'skip_micro'}
    for directory in ('tests', 'benches', 'examples', 'src/bin'):
        names.update(p.stem for p in (source / directory).glob('*.rs'))
    names.update(re.findall(r'^name\s*=\s*"([^"]+)"', (source / 'Cargo.toml').read_text(), re.M))
    patterns = tuple(n + '*' for n in names)
    return lambda _path, entries: [n for n in entries if n == 'incremental' or n.endswith('.lock')
                                  or any(fnmatch.fnmatchcase(n, p) for p in patterns)]

def run(name, mode, benches):
    source = RUN / 'sources' / name
    before = hashes(source)
    profile = 'release' if mode in ('release', 'alloc') else 'debug'
    target = REPO / 'target/sender-micro2-20261025' / (name + ('-alloc' if mode == 'alloc' else ''))
    if not (target / profile).exists():
        seed = REPO / 'target/sender-adoption-20261025' / ('m2-bench' if profile == 'release' else 'm2')
        for directory in ('.fingerprint', 'build', 'deps'):
            if (seed / profile / directory).is_dir():
                shutil.copytree(seed / profile / directory, target / profile / directory, ignore=ignores(source))
    output_root = RUN / 'checks/windows' / name / mode
    attempt = 1
    while (output_root / f'attempt-{attempt:02}').exists():
        attempt += 1
    output = output_root / f'attempt-{attempt:02}'
    output.mkdir(parents=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_NET_OFFLINE='true')
    if mode == 'checks':
        commands = [('fmt', ['cargo', 'fmt', '--all', '--check']),
          ('default', ['cargo', 'test', '--locked', '--quiet']),
          ('all-features', ['cargo', 'test', '--locked', '--all-features', '--quiet']),
          ('clippy', ['cargo', 'clippy', '--locked', '--all-targets', '--all-features', '--', '-D', 'warnings']),
          ('benches', ['cargo', 'check', '--locked', '--benches', '--all-features']),
          ('python', ['python', '-B', '-m', 'unittest', 'discover', '-s', 'scripts', '-p', 'test_*.py'])]
    elif mode == 'screen':
        commands = [('fmt', ['cargo', 'fmt', '--all', '--check']),
                    ('targeted', ['cargo', 'test', '--locked', '--lib', benches, '--quiet'])]
    else:
        command = ['cargo', 'build', '--locked', '--release', '--features',
                   'bench-allocations' if mode == 'alloc' else 'bench-internals']
        for name_ in benches.split(','):
            command += ['--bench', name_]
        commands = [('build', command)]
    save(output / 'environment.json', {'source': str(source), 'target': str(target), 'source_files': before,
        'compiler': subprocess.check_output(['rustc', '-vV'], text=True), 'mode': mode})
    records = []
    for label, command in commands:
        print('START', name, mode, label, flush=True)
        started = time.monotonic()
        with (output / (label + '.log')).open('xb') as log:
            code = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        records.append({'check': label, 'command': command, 'exit_code': code, 'seconds': time.monotonic()-started})
        save(output / 'checks.json', records)
        print('END', name, mode, label, code, flush=True)
        if code:
            print((output / (label + '.log')).read_text(errors='replace')[-7000:], flush=True)
            return code
    assert hashes(source) == before, 'source mutated during verification'
    if mode in ('release', 'alloc'):
        dest = RUN / 'artifacts' / (name + ('-alloc' if mode == 'alloc' else ''))
        dest.mkdir(parents=True, exist_ok=True)
        recorded = json.loads((dest / 'hashes.json').read_text()) if (dest / 'hashes.json').exists() else {}
        for name_ in benches.split(','):
            choices = list((target / 'release/deps').glob(name_ + '-*.exe'))
            assert len(choices) == 1, choices
            final = dest / (name_ + '.exe')
            if final.exists():
                raise RuntimeError('artifact already exists: ' + str(final))
            shutil.copy2(choices[0], final)
            recorded[name_] = hashlib.sha256(final.read_bytes()).hexdigest()
        save(dest / 'hashes.json', recorded)
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('snapshot', 'record', 'screen', 'checks', 'release', 'alloc'))
    parser.add_argument('name')
    parser.add_argument('--origin')
    parser.add_argument('--benches', default='file_pipeline,loopback_send_dump,control_messages')
    args = parser.parse_args()
    if args.action == 'snapshot':
        snapshot(args.name, args.origin)
    elif args.action == 'record':
        record_source(args.name)
    else:
        raise SystemExit(run(args.name, args.action, args.benches))
