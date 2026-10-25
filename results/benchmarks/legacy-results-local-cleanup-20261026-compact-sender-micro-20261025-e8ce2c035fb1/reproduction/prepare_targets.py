from pathlib import Path
import shutil

RUN = Path(__file__).resolve().parent
REPO = RUN.parents[3]
seed = REPO / 'target/sender-experiments-isolated/baseline'
ignore = shutil.ignore_patterns('fast-dump*', 'fast_dump*', 'libfast_dump*',
    'loopback_send_dump*', 'file_pipeline*', 'control_messages*', 'scan_source*',
    'bench_matrix*', 'integration*', 'incremental', '*.lock')
for candidate in ['baseline', 'm1-scan-tail', 'm2-subtree-move', 'm3-control-capacity']:
    for suffix, profiles in [('', ['debug', 'release']), ('-alloc', ['release'])]:
        target = REPO / 'target/sender-micro-20261025' / (candidate + suffix)
        target.mkdir(parents=True, exist_ok=False)
        for profile in profiles:
            shutil.copytree(seed / profile, target / profile, ignore=ignore)
        print('prepared', target.name, flush=True)
