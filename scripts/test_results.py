"""Regression tests for durable measurement history; no benchmark binaries required."""

import argparse
import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location('results', Path(__file__).with_name('results.py'))
RESULTS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RESULTS)


def matrix_row(**changes):
    row = {'dataset': 'mixed', 'protocol_runtime': 'v4', 'network_profile': 'loopback-localhost',
           'executor': 'default', 'threads': '4', 'frame_target_mb': '64', 'read_buffer_kb': '1024',
           'repeat_index': '1', 'files_count': '5', 'raw_read_bytes': '1048576',
           'skipped_files': '0', 'frame_aborts': '0', 'frame_rebuilds': '0',
           'total_duration_ms': '100', 'total_mib_s': '10', 'avg_cpu_pct': '0', 'max_rss_mib': '0'}
    row.update(changes)
    return row


def file_row(**changes):
    row = {'stage': 'read', 'readers': '2', 'side': 'before', 'entries': '4', 'bytes': '2048',
           'fingerprint': 'abc123', 'elapsed_ns': '1000000', 'cpu_ns': '500000',
           'counters_schema': '2', 'cycle_source': 'windows_process_cycles', 'io_source': 'windows_process_io',
           'verified': 'true', 'allocator_instrumented': 'false'}
    row.update(changes)
    return row


def write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(RESULTS.csv_bytes(rows))


class ResultAdaptersTests(unittest.TestCase):
    def test_harness_binding_requires_this_repository_cargo_bench(self):
        self.assertTrue(RESULTS.cargo_bench_from_repository(['cargo', '+stable', 'bench'], ROOT_DIR))
        self.assertTrue(RESULTS.cargo_bench_from_repository(
            ['cargo', 'bench', '--manifest-path', str(ROOT_DIR / 'Cargo.toml')], ROOT_DIR))
        self.assertFalse(RESULTS.cargo_bench_from_repository(['cargo', 'run', '--', 'bench'], ROOT_DIR))
        self.assertFalse(RESULTS.cargo_bench_from_repository(
            ['cargo', 'bench', '--manifest-path', 'target/other/Cargo.toml'], ROOT_DIR))

    def test_matrix_missing_protocol_stays_unknown_and_zero_resources_unmeasured(self):
        row = matrix_row()
        del row['protocol_runtime']
        entry = RESULTS.matrix_entries([row], {})[0]
        self.assertIsNone(entry['scenario']['protocol_runtime'])
        self.assertIsNone(entry['metrics']['cpu_pct']['mean'])
        self.assertIsNone(entry['metrics']['rss_mib']['mean'])
        self.assertEqual(entry['sample_count'], 1)

    def test_summary_only_does_not_invent_observations_or_distribution(self):
        entry = RESULTS.matrix_entries([matrix_row(completed_repeats='7')], {}, summary_only=True)[0]
        self.assertIsNone(entry['sample_count'])
        self.assertIsNone(entry['metrics']['elapsed_ms']['p95'])
        self.assertEqual(entry['reported_repeats'], '7')

    def test_identical_sample_values_are_kept_as_distinct_observations(self):
        entry = RESULTS.file_entries([file_row(), file_row()], {})[0]
        self.assertEqual(entry['sample_count'], 2)
        self.assertEqual(entry['metrics']['elapsed_ms']['n'], 2)

    def test_missing_file_metrics_are_null(self):
        entry = RESULTS.file_entries([file_row()], {})[0]
        self.assertEqual(entry['metrics']['elapsed_ms']['mean'], 1)
        self.assertIsNone(entry['metrics']['cpu_cycles']['mean'])

    def test_criterion_divides_iteration_time_and_preserves_mode(self):
        entry = RESULTS.criterion_entry({'iters': [2, 4], 'times': [20, 60], 'sampling_mode': 'Linear'},
                                       {'full_id': 'scan/tree'}, {})
        self.assertEqual(entry['metrics']['time_per_iteration_ns']['mean'], 12.5)
        self.assertEqual(entry['sample_count'], 2)
        with self.assertRaises(ValueError):
            RESULTS.criterion_entry({'iters': [0], 'times': [4]}, {}, {})

    def test_comparison_rejects_unknown_hosts_and_failed_or_different_runs(self):
        entry = RESULTS.file_entries([file_row()], {})[0]
        run = {'id': 'one', 'status': 'completed', 'source': None}
        other = {'id': 'two', 'status': 'completed', 'source': None}
        self.assertIn('same host not established', RESULTS.compare_entries(entry, entry, run, other, 'elapsed_ms')['reasons'])
        run['source'] = other['source'] = {'host': {'system': 'Windows', 'node': 'test'}}
        self.assertEqual(RESULTS.compare_entries(entry, entry, run, other, 'elapsed_ms')['ratio'], 1)
        other['status'] = 'failed'
        self.assertIsNone(RESULTS.compare_entries(entry, entry, run, other, 'elapsed_ms')['ratio'])

    def test_paired_comparison_still_checks_instrumentation_and_readers(self):
        first = RESULTS.file_entries([file_row()], {})[0]
        second = RESULTS.file_entries([file_row(side='after', elapsed_ns='500000')], {})[0]
        first.update(pair_context='pair', experiment='pair')
        second.update(pair_context='pair', experiment='pair')
        run = {'id': 'paired', 'status': 'imported'}
        self.assertEqual(RESULTS.compare_entries(first, second, run, run, 'elapsed_ms')['ratio'], .5)
        second['scenario']['allocations'] = True
        self.assertIsNone(RESULTS.compare_entries(first, second, run, run, 'elapsed_ms')['ratio'])

    def test_same_run_different_experiments_are_not_a_pair(self):
        first = RESULTS.file_entries([file_row()], {})[0]
        second = RESULTS.file_entries([file_row(side='after')], {})[0]
        first.update(pair_context='one', experiment='one')
        second.update(pair_context='two', experiment='two')
        first['fixture']['fingerprint'] = second['fixture']['fingerprint'] = None
        run = {'id': 'multi', 'status': 'completed'}
        self.assertIsNone(RESULTS.compare_entries(first, second, run, run, 'elapsed_ms')['ratio'])

    def test_incomplete_metric_coverage_cannot_report_ratio(self):
        first = RESULTS.file_entries([file_row(cpu_cycles='100'), file_row()], {})[0]
        second = RESULTS.file_entries([file_row(side='after', cpu_cycles='50'), file_row(side='after')], {})[0]
        first.update(pair_context='pair', experiment='pair')
        second.update(pair_context='pair', experiment='pair')
        run = {'id': 'pair', 'status': 'completed'}
        result = RESULTS.compare_entries(first, second, run, run, 'cpu_cycles')
        self.assertIsNone(result['ratio'])
        self.assertIn('incomplete metric coverage', result['reasons'])

    def test_matrix_resources_require_exact_csv_log_matching(self):
        text = '[bench-work] cpu_cycles=123 user_ns=10 kernel_ns=20 scope=sender_receiver_process\n'
        text += '[bench-link] profile=loopback-localhost tx_bytes=80 rx_bytes=20 elapsed_ms=100 raw_read_bytes=1048576\n'
        samples = RESULTS.matrix_log_samples(text, [matrix_row(wire_bytes='100')])
        self.assertEqual(samples[0]['cpu_cycles'], '123')
        self.assertIsNone(RESULTS.matrix_log_samples(text, [matrix_row(total_duration_ms='99')]))


class ResultImportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / 'old'
        self.output = self.root / 'results'
        self.source.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def test_import_is_idempotent_and_preserves_original_bytes(self):
        raw = self.source / 'experiment' / 'results_raw.csv'
        write_rows(raw, [matrix_row(), matrix_row(repeat_index='2')])
        before = raw.read_bytes()
        first = RESULTS.import_results(self.source, self.output)
        second = RESULTS.import_results(self.source, self.output)
        self.assertEqual(first['imported'], 1)
        self.assertEqual(second['existing'], 1)
        self.assertEqual(raw.read_bytes(), before)
        saved = next((self.output / 'benchmarks').glob('*/raw/results_raw.csv'))
        self.assertEqual(saved.read_bytes(), before)

    def test_importing_child_after_parent_keeps_the_existing_snapshot(self):
        parent = self.source / 'experiment'
        write_rows(parent / 'results_raw.csv', [matrix_row()])
        self.assertEqual(RESULTS.import_results(self.source, self.output)['imported'], 1)
        result = RESULTS.import_results(parent, self.output)
        self.assertEqual((result['imported'], result['existing']), (0, 1))

    def test_paired_aggregates_do_not_duplicate_children(self):
        parent = self.source / 'paired'
        parent.mkdir()
        RESULTS.atomic_json(parent / 'paired-run.json', {'complete': True, 'requested_readers': {'before': 2, 'after': 2}})
        for side in ('before', 'after'):
            write_rows(parent / side / 'results_raw.csv', [matrix_row()])
            write_rows(parent / 'runs' / ('01-' + side) / 'results_raw.csv', [matrix_row()])
        self.assertEqual(RESULTS.discover(self.source), [parent])
        report = RESULTS.import_results(self.source, self.output)
        self.assertEqual(report['imported'], 1)
        summary = RESULTS.read_json(next((self.output / 'benchmarks').glob('*/summary.json')))
        self.assertEqual(sum(row['sample_count'] for row in summary['entries']), 2)

    def test_partial_paired_matrix_keeps_completed_samples(self):
        parent = self.source / 'partial'
        RESULTS.atomic_json(parent / 'paired-run.json', {'complete': False})
        write_rows(parent / 'before' / 'results_raw.csv', [matrix_row()])
        write_rows(parent / 'runs' / '01-before' / 'results_raw.csv', [matrix_row()])
        write_rows(parent / 'runs' / '01-after' / 'results_raw.csv', [matrix_row()])
        self.assertEqual(RESULTS.discover(self.source), [parent])
        result = RESULTS.import_results(self.source, self.output)
        self.assertEqual(result['imported'], 1)
        run = RESULTS.read_json(next((self.output / 'benchmarks').glob('*/run.json')))
        summary = RESULTS.read_json(next((self.output / 'benchmarks').glob('*/summary.json')))
        self.assertEqual(run['status'], 'incomplete')
        self.assertEqual(sum(row['sample_count'] for row in summary['entries']), 2)

    def test_partial_file_manifest_measurements_are_not_duplicated_by_logs(self):
        parent = self.source / 'partial-files'
        log = parent / 'runs' / 'read-r2' / '01-before' / 'run.log'
        log.parent.mkdir(parents=True)
        log.write_text('[file-perf] stage=read readers=2 elapsed_ns=1000 entries=4 bytes=2048 fingerprint=abc verified=true\n', encoding='utf-8')
        RESULTS.atomic_json(parent / 'paired-run.json', {'complete': False, 'invocations': [
            {'side': 'before', 'pair': 1, 'log': '/old/root/runs/read-r2/01-before/run.log', 'measurement': file_row()}
        ]})
        result = RESULTS.import_results(self.source, self.output)
        self.assertEqual(result['imported'], 1)
        summary = RESULTS.read_json(next((self.output / 'benchmarks').glob('*/summary.json')))
        self.assertEqual(sum(entry['sample_count'] for entry in summary['entries']), 1)

    def test_standalone_file_log_is_discovered(self):
        path = self.source / 'standalone' / 'run.log'
        path.parent.mkdir()
        path.write_text('[file-perf] stage=scan readers=1 elapsed_ns=1000 entries=4 bytes=2048 fingerprint=abc verified=true\n', encoding='utf-8')
        result = RESULTS.import_results(self.source, self.output)
        self.assertEqual(result['imported'], 1)
        self.assertTrue(list((self.output / 'benchmarks').glob('*/raw/samples.csv')))

    def test_changed_manifest_creates_new_snapshot_and_supersedes_old_view(self):
        parent = self.source / 'pair'
        write_rows(parent / 'samples.csv', [file_row(), file_row(side='after')])
        RESULTS.atomic_json(parent / 'paired-run.json', {'complete': True})
        RESULTS.import_results(self.source, self.output)
        RESULTS.atomic_json(parent / 'paired-run.json', {'complete': True, 'valid_for_comparison': False})
        result = RESULTS.import_results(self.source, self.output)
        self.assertEqual(result['imported'], 1)
        manifests = [RESULTS.read_json(path) for path in (self.output / 'benchmarks').glob('*/run.json')]
        self.assertEqual({run['status'] for run in manifests}, {'imported', 'invalid'})
        page = (self.output / 'index.html').read_text(encoding='utf-8')
        data = json.loads(page.split('<script id="results-data" type="application/json">')[1].split('</script>')[0])
        self.assertEqual({run['status'] for run in data}, {'superseded', 'invalid'})

    def test_explicit_ancestor_exclusion_is_preserved(self):
        parent = self.source / 'excluded-trial'
        write_rows(parent / 'results_raw.csv', [matrix_row()])
        RESULTS.atomic_json(self.source / 'analysis-summary.json', {'method': {'excluded_directories': ['excluded-trial']}})
        RESULTS.import_results(self.source, self.output)
        run = RESULTS.read_json(next((self.output / 'benchmarks').glob('*/run.json')))
        self.assertEqual(run['status'], 'invalid')
        self.assertEqual(run['context_evidence'][0]['source'], 'analysis-summary.json')

    def test_importing_child_preserves_ancestor_exclusion(self):
        parent = self.source / 'excluded-trial'
        write_rows(parent / 'results_raw.csv', [matrix_row()])
        RESULTS.atomic_json(self.source / 'analysis-summary.json', {'method': {'excluded_directories': ['excluded-trial']}})
        RESULTS.import_results(parent, self.output)
        run = RESULTS.read_json(next((self.output / 'benchmarks').glob('*/run.json')))
        self.assertEqual(run['status'], 'invalid')

    def test_historical_checks_have_their_own_test_history_without_packages(self):
        RESULTS.atomic_json(self.source / 'check' / 'summary.json', {
            'commit': 'old-commit', 'checks': {'linux': [{'check': 'tests', 'exit_code': 0, 'passed_tests': 4}]},
            'python_tests': {'passed': 2}, 'packages': [{'binary': 'must-not-copy'}],
        })
        RESULTS.atomic_json(self.source / 'validation' / 'exit-codes.json', {'test': 0, 'clippy': 0})
        result = RESULTS.import_results(self.source, self.output)
        self.assertEqual(result['imported'], 2)
        self.assertEqual(len(list((self.output / 'tests').glob('*/run.json'))), 2)
        self.assertFalse(list((self.output / 'benchmarks').glob('*/run.json')))
        manifests = [RESULTS.read_json(path) for path in (self.output / 'tests').glob('*/run.json')]
        self.assertIn('old-commit', [run['parameters'].get('commit') for run in manifests])
        extracted = next((self.output / 'tests').glob('*/raw/checks.json')).read_text(encoding='utf-8')
        self.assertNotIn('must-not-copy', extracted)
        self.assertIn('passed_tests', extracted)

    def test_criterion_new_and_base_are_not_double_counted(self):
        parent = self.source / 'criterion' / 'case'
        for name in ('new', 'base'):
            RESULTS.atomic_json(parent / name / 'sample.json', {'iters': [1], 'times': [5], 'sampling_mode': 'Flat'})
            RESULTS.atomic_json(parent / name / 'benchmark.json', {'full_id': 'scan/case'})
        self.assertEqual(RESULTS.discover(self.source), [parent])
        RESULTS.atomic_json(parent / 'base' / 'sample.json', {'iters': [1], 'times': [8]})
        self.assertEqual(RESULTS.discover(self.source), [parent, parent / 'base'])

    def test_unknown_schema_preserves_evidence_without_fabricated_samples(self):
        path = self.source / 'old-comparison'
        write_rows(path / 'comparison.csv', [{'baseline_something': '42', 'current_something': '41'}])
        result = RESULTS.import_results(self.source, self.output)
        self.assertEqual(result['imported'], 1)
        summary = RESULTS.read_json(next((self.output / 'benchmarks').glob('*/summary.json')))
        self.assertFalse(summary['entries'])
        self.assertTrue(summary['warnings'])

    def test_comparison_json_embedded_samples_are_extracted_once(self):
        path = self.source / 'embedded'
        RESULTS.atomic_json(path / 'comparison.json', {'method': {'complete': True}, 'samples': [file_row(), file_row(side='after')]})
        RESULTS.import_results(self.source, self.output)
        self.assertEqual(len(list((self.output / 'benchmarks').glob('*/raw/samples.csv'))), 1)
        self.assertFalse(list((self.output / 'benchmarks').glob('*/raw/comparison.json')))

    def test_bad_csv_is_reported_without_mutating_source(self):
        path = self.source / 'broken' / 'results_raw.csv'
        path.parent.mkdir()
        path.write_text('dataset,total_duration_ms\nmixed\n', encoding='utf-8')
        result = RESULTS.import_results(self.source, self.output)
        self.assertEqual(len(result['errors']), 1)
        self.assertEqual(result['imported'], 0)
        self.assertTrue(path.exists())

    def test_html_embeds_escaped_data_and_does_not_fetch_local_json(self):
        label = '</script><script>alert(1)</script>'
        destination = self.output / 'benchmarks' / 'safe'
        RESULTS.atomic_json(destination / 'run.json', {'id': 'safe', 'kind': 'benchmark', 'label': label, 'status': 'completed'})
        RESULTS.atomic_json(destination / 'summary.json', {'entries': []})
        RESULTS.build_index(self.output)
        page = (self.output / 'index.html').read_text(encoding='utf-8')
        self.assertNotIn(label, page)
        self.assertIn('\\u003c/script', page)
        self.assertNotIn('fetch(', page)
        self.assertIn('textContent', page)


class ResultRecorderTests(unittest.TestCase):
    def record(self, root, script, kind='test'):
        args = argparse.Namespace(results_root=root, kind=kind, label='test recorder', suite=None,
                                  command=[sys.executable, '-c', script])
        with patch.object(RESULTS, 'provenance', return_value={'commit': 'test', 'host': {'system': 'test'}}), contextlib.redirect_stdout(io.StringIO()):
            return RESULTS.record(args, ROOT_DIR)

    def test_failed_checks_keep_exit_code_and_local_log(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(self.record(root, "print('failure'); raise SystemExit(7)"), 7)
            path = next(root.glob('tests/*/run.json'))
            self.assertEqual(RESULTS.read_json(path)['status'], 'failed')
            self.assertEqual(len(list(root.glob('local/tests/*/output.log'))), 1)
            self.assertFalse(list(root.glob('tests/*/*.log')))

    def test_repeat_label_creates_unique_runs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.record(root, "print('ok')")
            self.record(root, "print('ok')")
            self.assertEqual(len(list(root.glob('tests/*/run.json'))), 2)

    def test_stdout_file_measurements_survive_and_fixture_is_not_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            text = '[file-perf] stage=scan repeat=1 readers=1 entries=4 bytes=2048 fingerprint=abc elapsed_ns=1000000 verified=true'
            self.record(root, 'print(' + repr(text) + ')', kind='benchmark')
            summary = RESULTS.read_json(next(root.glob('benchmarks/*/summary.json')))
            self.assertEqual(summary['entries'][0]['metrics']['elapsed_ms']['mean'], 1)

    def test_collection_failure_finishes_manifest_and_preserves_child_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(RESULTS, 'publish_experiments', side_effect=OSError('disk unavailable')):
                self.assertEqual(self.record(root, "print('child passed')", kind='benchmark'), 2)
            run = RESULTS.read_json(next(root.glob('benchmarks/*/run.json')))
            self.assertEqual(run['status'], 'incomplete')
            self.assertEqual(run['exit_code'], 0)
            self.assertIn('disk unavailable', run['collection_errors'][0])

    def test_explicit_output_environment_is_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            explicit = {'FAST_DUMP_BENCH_MATRIX_DIR': 'custom-matrix', 'CRITERION_HOME': 'custom-criterion',
                        'FAST_DUMP_NETEM_DIR': 'custom-netem'}
            with patch.dict(os.environ, explicit):
                self.record(root, "print('ok')", kind='benchmark')
            run = RESULTS.read_json(next(root.glob('benchmarks/*/run.json')))
            for name, value in explicit.items():
                self.assertEqual(run['output_environment'][name], value)
            self.assertIsNone(run['source']['benchmark_harness_sha256'])

    def test_successful_command_cannot_override_invalid_experiment_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            script = "import json, os; from pathlib import Path; p=Path(os.environ['FAST_DUMP_RESULTS_RUN_DIR']); "
            script += "(p/'paired-run.json').write_text(json.dumps({'complete': True, 'valid_for_comparison': False})); "
            script += "print('[file-perf] stage=scan readers=1 entries=1 bytes=1 fingerprint=abc elapsed_ns=100 verified=true')"
            self.assertEqual(self.record(root, script, kind='benchmark'), 0)
            run = RESULTS.read_json(next(root.glob('benchmarks/*/run.json')))
            self.assertEqual(run['status'], 'invalid')


ROOT_DIR = Path(__file__).resolve().parent.parent


if __name__ == '__main__':
    unittest.main()
