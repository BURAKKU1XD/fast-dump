"""Independent raw CSV/log audit and arithmetic check; no bootstrap or timing."""
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import statistics

ROOT = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    analysis = json.loads((ROOT / 'analysis.json').read_text(encoding='utf-8'))
    records = []
    runs = analysis['measurement_runs']
    assert len(runs) == 6
    for run in runs:
        assert run['valid_evidence'] and run['complete'] and not run['errors']
        expected_pairs = 5 if run['comparison_kind'] == 'A/A' else 10
        observations = run['observations']
        assert len(observations) == expected_pairs * 2
        assert {(item['pair'], item['side']) for item in observations} == {
            (pair, side) for pair in range(1, expected_pairs + 1) for side in ('before', 'after')}
        identities = set()
        for item in observations:
            assert not item['errors']
            log_path = Path(item['log']); csv_path = log_path.with_name('results_raw.csv')
            with csv_path.open(newline='', encoding='utf-8-sig') as stream:
                rows = list(csv.DictReader(stream))
            assert len(rows) == 1 and rows[0] == item['raw']
            raw, metrics = rows[0], item['metrics']
            log = log_path.read_text(encoding='utf-8', errors='replace')
            verified = re.findall(r'^\[bench-verify\] verified (\d+) entries, (\d+) regular-file bytes', log, re.M)
            assert len(verified) == 1 and tuple(map(int, verified[0])) == (int(raw['files_count']), int(raw['raw_read_bytes']))
            assert re.findall(r'^\[bench-readers\] requested=(\d+)$', log, re.M) == ['8']
            assert all(int(raw[key]) == 0 for key in ('skipped_files', 'frame_aborts', 'frame_rebuilds'))
            assert (raw['threads'], raw['frame_target_mb'], raw['read_buffer_kb'], raw['protocol_runtime'], raw['network_profile']) == ('4', '64', '1024', 'v4', 'loopback-localhost')
            work = item['resources']['bench-work']; link = item['resources']['bench-link']
            assert work['scope'] == item['contract']['cpu_scope'] == 'sender_receiver_process'
            assert metrics['cpu_ms'] == (int(work['user_ns']) + int(work['kernel_ns'])) / 1e6
            assert metrics['cpu_cycles'] == int(work['cpu_cycles'])
            assert math.isclose(metrics['total_mib_s'], int(link['logical_bytes']) / 2**20 / (metrics['elapsed_ms'] / 1000), rel_tol=1e-8, abs_tol=1e-5)
            identities.add(tuple(raw[key] for key in ('files_count', 'raw_read_bytes', 'compressed_payload_bytes', 'sent_frames')))
            records.append({'label': run['label'], 'pair': item['pair'], 'side': item['side'],
                            'csv_sha256': sha(csv_path), 'log_sha256': sha(log_path), 'archive_verified': True})
        assert len(identities) == 1
    assert len(records) == 90
    for group in analysis['groups']:
        selected = [item for run in runs if run['label'] in group['labels'] for item in run['observations']]
        for name in ('elapsed_ms', 'total_mib_s', 'cpu_ms', 'cpu_cycles', 'rss_mib', 'first_bundle_ms'):
            summary = group['metrics'][name]
            for side in ('before', 'after'):
                values = [item['metrics'][name] for item in selected if item['side'] == side]
                assert math.isclose(summary[side + '_mean'], statistics.mean(values), rel_tol=1e-12)
                assert summary[side + '_median'] == statistics.median(values)
            expected = 100 * (1 - summary['after_mean'] / summary['before_mean'])
            if name == 'total_mib_s':
                expected *= -1
            assert math.isclose(summary['improvement_pct'], expected, abs_tol=1e-10)
    output = ROOT / 'independent-audit.json'
    assert not output.exists(), 'refusing to overwrite audit'
    result = {'status': 'pass', 'generated_utc': datetime.now(timezone.utc).isoformat(),
        'analysis_sha256': sha(ROOT / 'analysis.json'), 'series': 6, 'observations': 90,
        'verified_archives': 90, 'mean_median_and_effect_arithmetic_checked': True,
        'runtime_settings_checked': {'readers': 8, 'threads': 4, 'frame_mib': 64, 'buffer_kib': 1024},
        'observations_evidence': records}
    output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print('PASS 6 series / 90 observations / 90 verified archives; raw CSV/log and summary arithmetic agree')


if __name__ == '__main__':
    main()
