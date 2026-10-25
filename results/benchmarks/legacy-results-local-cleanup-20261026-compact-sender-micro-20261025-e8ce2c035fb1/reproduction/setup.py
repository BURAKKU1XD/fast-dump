import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]
CANDIDATES = ['baseline', 'm1-scan-tail', 'm2-subtree-move', 'm3-control-capacity']
files = [p for p in subprocess.check_output(['git', 'ls-files'], cwd=REPO, text=True).splitlines()
         if not p.startswith('results/')]
for candidate in CANDIDATES:
    destination = RUN / 'sources' / candidate
    destination.mkdir(parents=True, exist_ok=False)
    for relative in files:
        source = REPO / relative
        if source.is_file():
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    print('source', candidate, flush=True)
(RUN / 'experiment.json').write_text(json.dumps({
    'created_utc': datetime.now(timezone.utc).isoformat(),
    'baseline_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
    'compiler': subprocess.check_output(['rustc', '-vV'], cwd=REPO, text=True),
    'status': 'in_progress', 'candidates': CANDIDATES,
    'config': {'threads': 4, 'readers': 8, 'level': 3, 'frame_target_mb': 64,
               'network_profile': 'loopback-localhost'},
    'acceptance': 'Reproducible allocation/CPU reduction with simple implementation; no confirmed regression above matching A/A noise. E2E speedup is not required.',
    'adoption': 'Independent patches and report; mainline source remains unchanged.'
}, indent=2) + '\n')
