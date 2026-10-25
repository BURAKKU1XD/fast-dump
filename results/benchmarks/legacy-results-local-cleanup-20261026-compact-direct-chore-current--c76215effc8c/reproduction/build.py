"""Build two immutable exact-commit snapshots, never reusing package artifacts."""
import argparse
from datetime import datetime, timezone
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
COMMITS = {
    'before': '4c58969a1ac1d4fbf18675077cac105485f2f2ab',
    'after': 'bb5004255e8cc23c5d7543d5e6d6f4499960b27f',
}

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(data)
    return digest.hexdigest()

def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')

def source_hashes(source):
    return {path.relative_to(source).as_posix(): sha(path)
            for path in sorted(source.rglob('*')) if path.is_file()}

def prepare(name):
    source = RUN / 'sources' / name
    source.mkdir(parents=True, exist_ok=False)
    entries = subprocess.check_output(['git', 'ls-tree', '-r', '-z', COMMITS[name]], cwd=REPO)
    blobs = {}
    for item in entries.split(b'\0'):
        if not item:
            continue
        meta, relative = item.split(b'\t', 1)
        mode, kind, oid = meta.decode().split()
        relative = relative.decode('utf-8')
        if relative.startswith('results/'):
            continue
        assert kind == 'blob' and mode in ('100644', '100755'), (mode, relative)
        destination = (source / relative).resolve()
        assert source.resolve() in destination.parents
        content = subprocess.check_output(['git', 'cat-file', 'blob', oid], cwd=REPO)
        assert hashlib.sha1(b'blob ' + str(len(content)).encode() + b'\0' + content).hexdigest() == oid
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        blobs[relative] = oid
    files = source_hashes(source)
    save(RUN / (name + '-source.json'), {'commit': COMMITS[name], 'files': files,
        'source_sha256': hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest(),
        'git_blob_sha1': blobs, 'excluded': ['results/'], 'exact_git_blob_contents': True})
    print('SNAPSHOT', name, COMMITS[name], len(files), flush=True)

def dependency_ignore(source):
    names = {'fast-dump', 'fast_dump', 'libfast_dump', 'control_messages',
             'planner_skips', 'progress_guard', 'observed_e2e'}
    for directory in ('tests', 'benches', 'examples', 'src/bin'):
        names.update(path.stem for path in (source / directory).glob('*.rs'))
    names.update(re.findall(r'^name\s*=\s*"([^"]+)"', (source / 'Cargo.toml').read_text(), re.M))
    patterns = tuple(name + '*' for name in names)
    return lambda _path, entries: [name for name in entries if name == 'incremental'
        or name.endswith('.lock') or any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)]

def build(name):
    source = RUN / 'sources' / name
    manifest = json.loads((RUN / (name + '-source.json')).read_text())
    assert source_hashes(source) == manifest['files']
    target = REPO / 'target/direct-chore-current-20261025' / name
    target.mkdir(parents=True, exist_ok=False)
    seed = REPO / 'target/sender-micro2-20261025/commit-bench'
    for directory in ('.fingerprint', 'build', 'deps'):
        shutil.copytree(seed / 'release' / directory, target / 'release' / directory,
                        ignore=dependency_ignore(source))
    output = RUN / 'checks/windows' / name / 'release'
    output.mkdir(parents=True, exist_ok=False)
    compiler = subprocess.check_output(['rustc', '-vV'], text=True)
    sysroot = Path(subprocess.check_output(['rustc', '--print', 'sysroot'], text=True).strip())
    compiler_hash = sha(sysroot / 'bin/rustc.exe')
    command = ['cargo', 'build', '--offline', '--locked', '--release', '--bench',
               'loopback_send_dump', '--features', 'bench-internals']
    env = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_NET_OFFLINE='true')
    save(output / 'environment.json', {'commit': COMMITS[name], 'source': str(source),
        'source_sha256': manifest['source_sha256'], 'target': str(target), 'compiler': compiler,
        'compiler_binary_sha256': compiler_hash, 'command': command,
        'dependency_seed': str(seed), 'package_artifacts_seeded': False,
        'rustflags': env.get('RUSTFLAGS'), 'encoded_rustflags': env.get('CARGO_ENCODED_RUSTFLAGS'),
        'started_utc': datetime.now(timezone.utc).isoformat()})
    print('BUILD', name, flush=True)
    start = time.monotonic()
    with (output / 'build.log').open('xb') as log:
        code = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT).returncode
    save(output / 'checks.json', [{'check': 'release-build', 'exit_code': code,
        'command': command, 'seconds': time.monotonic() - start}])
    if code:
        print((output / 'build.log').read_text(errors='replace')[-8000:], flush=True)
        return code
    assert source_hashes(source) == manifest['files']
    assert sha(sysroot / 'bin/rustc.exe') == compiler_hash
    candidates = list((target / 'release/deps').glob('loopback_send_dump-*.exe'))
    assert len(candidates) == 1, candidates
    dest = RUN / 'artifacts' / name
    dest.mkdir(parents=True, exist_ok=False)
    shutil.copy2(candidates[0], dest / 'loopback_send_dump.exe')
    save(dest / 'hashes.json', {'loopback_send_dump': sha(dest / 'loopback_send_dump.exe')})
    save(output / 'snapshot-status.json', {'unchanged': True, 'exact_commit': COMMITS[name]})
    print('READY', name, flush=True)
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('prepare', 'build'))
    parser.add_argument('name', choices=COMMITS)
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare(args.name)
    else:
        raise SystemExit(build(args.name))
