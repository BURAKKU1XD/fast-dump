import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]

def verify(candidate):
    source = RUN / 'sources' / candidate
    root = RUN / 'checks/windows' / candidate
    root.mkdir(parents=True, exist_ok=True)
    output = root / f'attempt-{len(list(root.glob("attempt-*"))) + 1:02}'
    output.mkdir()
    target = REPO / 'target/sender-micro-20261025' / candidate
    environment = os.environ.copy()
    environment['CARGO_TARGET_DIR'] = str(target)
    commands = [
        ('fmt', ['cargo', 'fmt', '--all', '--check']),
        ('default', ['cargo', 'test', '--locked', '--quiet']),
        ('all-features', ['cargo', 'test', '--locked', '--all-features', '--quiet']),
        ('clippy', ['cargo', 'clippy', '--locked', '--all-targets', '--all-features', '--', '-D', 'warnings']),
        ('benches', ['cargo', 'check', '--locked', '--benches', '--all-features']),
        ('python', ['python', '-B', '-m', 'unittest', 'discover', '-s', 'scripts', '-p', 'test_*.py']),
        ('release', ['cargo', 'build', '--locked', '--release', '--bin', 'fast-dump', '--benches', '--features', 'bench-internals']),
    ]
    records = []
    for label, command in commands:
        print('START', candidate, label, flush=True)
        started = time.time()
        with (output / (label + '.log')).open('wb') as log:
            result = subprocess.run(command, cwd=source, env=environment, stdout=log, stderr=subprocess.STDOUT)
        records.append({'candidate': candidate, 'check': label, 'command': command,
                        'exit_code': result.returncode, 'seconds': time.time()-started})
        (output / 'checks.json').write_text(json.dumps(records, indent=2)+'\n')
        print('END', candidate, label, result.returncode, flush=True)
        if result.returncode:
            print((output / (label + '.log')).read_text(errors='replace')[-9000:], flush=True)
            return result.returncode
    destination = RUN / 'artifacts' / candidate
    destination.mkdir(parents=True, exist_ok=True)
    hashes = {}
    for name in ['fast-dump', 'loopback_send_dump', 'file_pipeline', 'control_messages']:
        choices = [target/'release/fast-dump.exe'] if name == 'fast-dump' else list((target/'release/deps').glob(name+'-*.exe'))
        if len(choices) != 1:
            raise RuntimeError((name, choices))
        saved = destination / (name + '.exe')
        shutil.copy2(choices[0], saved)
        hashes[name] = hashlib.sha256(saved.read_bytes()).hexdigest()
    (destination / 'hashes.json').write_text(json.dumps(hashes, indent=2)+'\n')
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('candidate')
    raise SystemExit(verify(parser.parse_args().candidate))
