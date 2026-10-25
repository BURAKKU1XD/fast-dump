from concurrent.futures import ThreadPoolExecutor
from verify import verify

with ThreadPoolExecutor(max_workers=2) as executor:
    results = list(executor.map(verify, ['baseline', 'm1-scan-tail', 'm2-subtree-move', 'm3-control-capacity']))
raise SystemExit(any(results))
