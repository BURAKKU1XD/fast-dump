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

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[3]


def ignores(source):
    names = {'fast-dump', 'fast_dump', 'libfast_dump'}
    for directory in ('tests', 'benches', 'examples', 'src/bin'):
        names.update(path.stem for path in (source / directory).glob('*.rs'))
    names.update(re.findall(r'^name\s*=\s*"([^"]+)"', (source / 'Cargo.toml').read_text(), re.M))
    patterns = tuple(name + '*' for name in names)
    return lambda _path, entries: [name for name in entries if name == 'incremental'
        or name.endswith('.lock') or any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)]


def target_for(candidate, source, profile):
    target = REPO / 'target/sender-adoption-20261025' / candidate
    if not target.exists():
        target.mkdir(parents=True)
        seed = REPO / 'target/sender-micro-20261025/m1-scan-tail'
        for directory in ('.fingerprint', 'build', 'deps'):
            origin = seed / profile / directory
            if origin.exists():
                shutil.copytree(origin, target / profile / directory, ignore=ignores(source))
    return target


def run(candidate, mode):
    source = ROOT / 'sources' / candidate
    target = target_for(candidate + ('-alloc' if mode == 'alloc' else ''), source,
                        'debug' if mode == 'checks' else 'release')
    output = ROOT / 'checks/windows' / candidate / mode
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update(CARGO_TARGET_DIR=str(target), CARGO_NET_OFFLINE='true')
    commands = [
        ('fmt', ['cargo', 'fmt', '--all', '--check']),
        ('scan', ['cargo', 'test', '--locked', '--lib', 'sender::scan::', '--quiet']),
        ('default', ['cargo', 'test', '--locked', '--quiet']),
        ('all-features', ['cargo', 'test', '--locked', '--all-features', '--quiet']),
        ('clippy', ['cargo', 'clippy', '--locked', '--all-targets', '--all-features', '--', '-D', 'warnings']),
        ('benches', ['cargo', 'check', '--locked', '--benches', '--all-features']),
        ('python', ['python', '-B', '-m', 'unittest', 'discover', '-s', 'scripts', '-p', 'test_*.py']),
    ] if mode == 'checks' else [
        ('release', ['cargo', 'build', '--locked', '--release', '--bin', 'fast-dump', '--benches',
                     '--features', 'bench-allocations' if mode == 'alloc' else 'bench-internals'])]
    metadata = {'source': str(source), 'target': str(target), 'compiler':
        subprocess.check_output(['rustc', '-vV'], text=True), 'mode': mode, 'candidate': candidate}
    (output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    records = []
    for label, command in commands:
        print('START', candidate, mode, label, flush=True)
        start = time.monotonic()
        with (output / (label + '.log')).open('xb') as log:
            code = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
        records.append({'check': label, 'command': command, 'exit_code': code,
                        'seconds': time.monotonic() - start})
        (output / 'checks.json').write_text(json.dumps(records, indent=2) + '\n')
        print('END', candidate, mode, label, code, flush=True)
        if code:
            print((output / (label + '.log')).read_text(errors='replace')[-10000:], flush=True)
            return code
    if mode != 'checks':
        destination = ROOT / 'artifacts' / (candidate + ('-alloc' if mode == 'alloc' else ''))
        destination.mkdir(parents=True, exist_ok=False)
        hashes = {}
        for name in ('fast-dump', 'file_pipeline', 'loopback_send_dump', 'control_messages'):
            paths = [target / 'release/fast-dump.exe'] if name == 'fast-dump' else list((target / 'release/deps').glob(name + '-*.exe'))
            if len(paths) != 1:
                raise RuntimeError((name, paths))
            dest = destination / (name + '.exe')
            shutil.copy2(paths[0], dest)
            hashes[name] = hashlib.sha256(dest.read_bytes()).hexdigest()
        (destination / 'hashes.json').write_text(json.dumps(hashes, indent=2) + '\n')
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('candidate')
    parser.add_argument('mode', choices=('checks', 'release', 'alloc'))
    args = parser.parse_args()
    raise SystemExit(run(args.candidate, args.mode))
