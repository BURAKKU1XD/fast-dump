import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]
for candidate in ['baseline', 'm1-scan-tail', 'm2-subtree-move', 'm3-control-capacity']:
    benches = ['file_pipeline', 'control_messages'] if candidate == 'baseline' else [
        'control_messages' if candidate == 'm3-control-capacity' else 'file_pipeline']
    target = REPO / 'target/sender-micro-20261025' / (candidate + '-alloc')
    env = os.environ.copy()
    env['CARGO_TARGET_DIR'] = str(target)
    logs = RUN / 'checks/allocations' / candidate
    logs.mkdir(parents=True, exist_ok=False)
    command = ['cargo', 'build', '--locked', '--release', '--features', 'bench-allocations']
    for bench in benches:
        command += ['--bench', bench]
    started = time.time()
    print('START allocation build', candidate, flush=True)
    with (logs / 'build.log').open('wb') as output:
        result = subprocess.run(command, cwd=RUN/'sources'/candidate, env=env,
                                stdout=output, stderr=subprocess.STDOUT)
    (logs / 'checks.json').write_text(json.dumps({'candidate': candidate, 'command': command,
        'exit_code': result.returncode, 'seconds': time.time()-started}, indent=2)+'\n')
    if result.returncode:
        print((logs/'build.log').read_text(errors='replace')[-8000:])
        raise SystemExit(result.returncode)
    out = RUN / 'artifacts' / (candidate + '-alloc')
    out.mkdir(parents=True, exist_ok=True)
    hashes = {}
    for name in benches:
        choices = list((target/'release/deps').glob(name+'-*.exe'))
        if len(choices) != 1:
            raise RuntimeError((name, choices))
        shutil.copy2(choices[0], out/(name+'.exe'))
        hashes[name] = hashlib.sha256((out/(name+'.exe')).read_bytes()).hexdigest()
    (out/'hashes.json').write_text(json.dumps(hashes, indent=2)+'\n')
    print('END allocation build', candidate, flush=True)
