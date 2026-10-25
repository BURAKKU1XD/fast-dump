import argparse
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


RUN = Path(__file__).resolve().parent
CANDIDATES = ('arc', 'progress', 'counter', 'commit')
TARGET_ROOT = Path('/tmp/fast-dump-sender-micro2-20261025')
OLD_SEEDS = (Path('/tmp/fast-dump-sender-micro-20261025/baseline'),
             Path('/tmp/fast-dump-sender-20261025-isolated/baseline'))
CARGO = Path('/home/test/.cargo/bin/cargo')


def seed_ignores(source):
    # Копируем только зависимости: ни fingerprint пакета, ни его тесты и benches
    # не должны попасть в target другого варианта.
    names = {'fast-dump', 'fast_dump', 'libfast_dump', 'control_messages'}
    for directory in ('tests', 'benches', 'examples', 'src/bin'):
        names.update(path.stem for path in (source / directory).glob('*.rs'))
    manifest = (source / 'Cargo.toml').read_text(encoding='utf-8')
    names.update(re.findall(r'^name\s*=\s*"([^"]+)"', manifest, re.MULTILINE))
    patterns = tuple(name + '*' for name in names)

    def ignore(_directory, entries):
        return [name for name in entries if name == 'incremental'
                or name.endswith('.lock')
                or any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)]

    return ignore


def prepare_target(candidate, source):
    target = TARGET_ROOT / candidate
    if target.exists():
        return target, {'reused_own_target': True}
    target.mkdir(parents=True)
    seeds = ((TARGET_ROOT / 'm1',) if candidate == 'm2' else ()) + OLD_SEEDS
    seed = next((path for path in seeds if (path / 'debug/deps').is_dir()), None)
    copied = []
    if seed is not None:
        # Проверки используют debug; верхние бинарники и incremental не переносим.
        for name in ('.fingerprint', 'build', 'deps'):
            origin = seed / 'debug' / name
            if origin.is_dir():
                shutil.copytree(origin, target / 'debug' / name, ignore=seed_ignores(source))
                copied.append(name)
    details = {'reused_own_target': False, 'dependency_seed': str(seed) if copied else None,
               'copied_debug_directories': copied}
    (target / 'dependency-seed.json').write_text(json.dumps(details, indent=2) + '\n')
    return target, details


def new_attempt(candidate):
    root = RUN / 'checks/linux' / candidate
    root.mkdir(parents=True, exist_ok=True)
    number = 1
    while True:
        attempt = root / f'attempt-{number:02}'
        try:
            attempt.mkdir()
            return attempt
        except FileExistsError:
            number += 1


def source_hashes(source):
    # Снимок остаётся неизменным весь прогон; внешние target/logs в него не входят.
    ignored = {'.git', 'target', 'results', '__pycache__'}
    hashes = {}
    for path in sorted(source.rglob('*')):
        relative = path.relative_to(source)
        if path.is_file() and not ignored.intersection(relative.parts):
            hashes[relative.as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def verify(candidate):
    source = RUN / 'sources' / candidate
    if not (source / 'Cargo.toml').is_file():
        raise RuntimeError(f'Immutable source snapshot is not ready: {source}')
    output = new_attempt(candidate)
    print('PREPARE', candidate, str(output), flush=True)
    before = source_hashes(source)
    (output / 'source-hashes.json').write_text(json.dumps(before, indent=2) + '\n')
    target, seed = prepare_target(candidate, source)
    environment = os.environ.copy()
    environment['PATH'] = str(CARGO.parent) + os.pathsep + environment.get('PATH', '')
    environment['CARGO_TARGET_DIR'] = str(target)
    environment['CARGO_NET_OFFLINE'] = 'true'
    metadata = {'candidate': candidate, 'source': str(source), 'target': str(target),
                'seed': seed, 'python': sys.version,
                'rustc': subprocess.check_output([str(CARGO.parent / 'rustc'), '-vV'],
                                                env=environment, text=True),
                'cargo': subprocess.check_output([str(CARGO), '--version'],
                                                env=environment, text=True)}
    (output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    commands = [
        ('fmt', [str(CARGO), 'fmt', '--all', '--check']),
        ('default', [str(CARGO), 'test', '--offline', '--locked', '--quiet']),
        ('all-features', [str(CARGO), 'test', '--offline', '--locked', '--all-features', '--quiet']),
        ('clippy', [str(CARGO), 'clippy', '--offline', '--locked', '--all-targets',
                    '--all-features', '--', '-D', 'warnings']),
        ('benches', [str(CARGO), 'check', '--offline', '--locked', '--benches', '--all-features']),
        ('python', [sys.executable, '-B', '-m', 'unittest', 'discover', '-s', 'scripts',
                    '-p', 'test_*.py']),
    ]
    records = []
    for label, command in commands:
        print('START', candidate, label, flush=True)
        started = time.monotonic()
        logpath = output / (label + '.log')
        # Отдельный каталог каждой попытки и exclusive create сохраняют все ошибки.
        with logpath.open('xb') as log:
            result = subprocess.run(command, cwd=source, env=environment,
                                    stdout=log, stderr=subprocess.STDOUT)
        records.append({'candidate': candidate, 'check': label, 'command': command,
                        'exit_code': result.returncode, 'seconds': time.monotonic() - started,
                        'log': str(logpath)})
        (output / 'checks.json').write_text(json.dumps(records, indent=2) + '\n')
        print('END', candidate, label, result.returncode, flush=True)
        if result.returncode:
            print(logpath.read_text(errors='replace')[-12000:], flush=True)
            return result.returncode
    after = source_hashes(source)
    unchanged = before == after
    (output / 'snapshot-status.json').write_text(json.dumps({'unchanged': unchanged}, indent=2) + '\n')
    if not unchanged:
        (output / 'source-hashes-after.json').write_text(json.dumps(after, indent=2) + '\n')
        print('FAIL source snapshot changed during verification', candidate, flush=True)
        return 1
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('candidate', choices=CANDIDATES, help='Ready immutable source snapshot to verify')
    args = parser.parse_args()
    if sys.platform != 'linux' or not CARGO.is_file():
        parser.error('run in Ubuntu WSL with native /home/test/.cargo/bin/cargo')
    return verify(args.candidate)


if __name__ == '__main__':
    raise SystemExit(main())
