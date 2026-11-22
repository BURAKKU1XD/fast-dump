#!/usr/bin/env python3
"""Lightweight parser/report regressions; no benchmark binary or fixture required."""

import contextlib
import csv
import importlib.util
import io
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location('bench_files_paired', Path(__file__).with_name('bench-files-paired.py'))
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)
PIPELINE_SPEC = importlib.util.spec_from_file_location('bench_paired', Path(__file__).with_name('bench-paired.py'))
PIPELINE = importlib.util.module_from_spec(PIPELINE_SPEC)
PIPELINE_SPEC.loader.exec_module(PIPELINE)


def measurement(**overrides):
    result = {
        'stage': 'read', 'repeat': 1, 'entries': 4, 'files': 3, 'bytes': 2 * BENCH.MIB,
        'elapsed_ns': 100, 'work_ns': 90, 'drop_ns': 10,
        'cpu_ns': 60, 'user_ns': 40, 'kernel_ns': 20, 'readers': 4,
        'fingerprint': '00abcdef12345678', 'allocs': None, 'alloc_bytes': None,
        'reallocs': None, 'deallocs': None, 'voluntary_switches': None,
        'involuntary_switches': None, 'minor_faults': None, 'major_faults': None,
        'process_peak_rss_bytes': 2048, 'allocator_instrumented': False,
        'allocations_enabled': False, 'verified': True,
    }
    result.update(overrides)
    return result


def write_measurement(path, row):
    def formatted(value):
        return 'unmeasured' if value is None else str(value).lower() if isinstance(value, bool) else str(value)
    path.write_text('[file-perf] ' + ' '.join(f'{key}={formatted(value)}' for key, value in row.items()) + '\n', encoding='utf-8')


class FileComparisonTests(unittest.TestCase):
    def parse(self, row):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'run.log'
            write_measurement(log, row)
            return BENCH.parse_measurement(log)

    def test_legacy_optional_counters_are_unmeasured(self):
        row = self.parse(measurement())
        self.assertEqual(BENCH.measurement_contract(row), (1, 'legacy_unreported', 'legacy_unreported'))
        self.assertIsNone(row['cpu_cycles'])
        self.assertIsNone(row['io_read_ops'])
        self.assertIsNone(row['alloc_successes'])
        self.assertEqual(row['fingerprint'], '00abcdef12345678')

    def test_source_labels_and_measured_zero_survive_parsing(self):
        row = self.parse(measurement(counters_schema=2, cycle_source='windows_process_cycle_time',
                                     io_source='windows_process_io_accounting', io_read_ops=0,
                                     cpu_cycles=12_345_678_901_234_567))
        self.assertEqual(BENCH.measurement_contract(row),
                         (2, 'windows_process_cycle_time', 'windows_process_io_accounting'))
        self.assertEqual(row['io_read_ops'], 0)
        self.assertEqual(row['cpu_cycles'], 12_345_678_901_234_567)
        self.assertIsInstance(row['cpu_cycles'], int)

    def test_normalization_uses_entries_and_binary_mib(self):
        row = BENCH.normalized_measurement(measurement(cpu_cycles=120, io_read_ops=8, io_read_bytes=4 * BENCH.MIB))
        self.assertEqual(row['cpu_cycles_per_entry'], 30)
        self.assertEqual(row['cpu_cycles_per_mib'], 60)
        self.assertEqual(row['io_read_ops_per_entry'], 2)
        self.assertEqual(row['io_read_bytes_per_mib'], 2 * BENCH.MIB)
        self.assertIsNone(row['storage_read_bytes_per_mib'])
        self.assertNotIn('process_peak_rss_bytes_per_entry', row)

    def test_io_observer_correction_keeps_auditable_counts(self):
        row = self.parse(measurement(io_read_ops=40, io_read_ops_raw=42, io_observer_read_ops=2,
                                     io_read_bytes=1000, io_read_bytes_raw=1200, io_observer_read_bytes=200))
        self.assertEqual(row['io_read_ops'], 40)
        self.assertEqual(row['io_read_ops_raw'], 42)
        with self.assertRaisesRegex(ValueError, 'I/O observer correction mismatch'):
            self.parse(measurement(io_read_ops=40, io_read_ops_raw=41, io_observer_read_ops=2))

    def test_schema_two_requires_explicit_counter_sources(self):
        with self.assertRaisesRegex(ValueError, 'Counter source labels missing'):
            self.parse(measurement(counters_schema=2))

    def test_zero_denominators_do_not_invent_resource_values(self):
        row = BENCH.normalized_measurement(measurement(entries=0, bytes=0, io_read_ops=0))
        self.assertIsNone(row['io_read_ops_per_entry'])
        self.assertIsNone(row['io_read_ops_per_mib'])
        row = BENCH.normalized_measurement(measurement(bytes=0, io_read_ops=0))
        self.assertEqual(row['io_read_ops_per_entry'], 0)
        self.assertIsNone(row['io_read_ops_per_mib'])

    def test_unsupported_counters_and_zero_baselines_remain_distinct(self):
        selected = [{'side': side, 'pair': 1, 'cpu_cycles': None, 'io_read_ops': 0} for side in ('before', 'after')]
        unsupported = BENCH.compare_metric(selected, 'cpu_cycles', 1)
        self.assertIsNone(unsupported['before_mean'])
        self.assertIsNone(unsupported['ratio_after_before'])
        self.assertIsNone(unsupported['after_wins'])
        self.assertEqual(unsupported['paired_n'], 0)
        measured_zero = BENCH.compare_metric(selected, 'io_read_ops', 1)
        self.assertEqual(measured_zero['before_mean'], 0)
        self.assertIsNone(measured_zero['ratio_after_before'])
        self.assertEqual(measured_zero['after_wins'], 0)
        self.assertEqual(measured_zero['paired_n'], 1)

    def test_partial_coverage_does_not_create_unpaired_ratio(self):
        selected = [
            {'side': 'before', 'pair': 1, 'cpu_cycles': 100},
            {'side': 'after', 'pair': 1, 'cpu_cycles': 75},
            {'side': 'before', 'pair': 2, 'cpu_cycles': None},
            {'side': 'after', 'pair': 2, 'cpu_cycles': 50},
        ]
        compared = BENCH.compare_metric(selected, 'cpu_cycles', 2)
        self.assertEqual(compared['before_n'], 1)
        self.assertFalse(compared['before_complete'])
        self.assertIsNone(compared['ratio_after_before'])
        self.assertEqual(compared['paired_n'], 1)
        self.assertEqual(compared['after_wins'], 1)

    def test_invalid_totals_or_unverified_child_are_rejected(self):
        for changes in ({'cpu_ns': 61}, {'elapsed_ns': 99}, {'verified': False}, {'repeat': 2}, {'entries': 0}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.parse(measurement(**changes))

    def test_noop_allows_empty_fixture_without_normalizing_overhead(self):
        row = BENCH.normalized_measurement(self.parse(measurement(stage='noop', entries=0, files=0, bytes=0, cpu_cycles=17)))
        self.assertEqual(row['cpu_cycles'], 17)
        self.assertIsNone(row['cpu_cycles_per_entry'])
        self.assertIsNone(row['cpu_cycles_per_mib'])

    def test_quantized_zero_duration_is_allowed_only_for_noop(self):
        durations = {'elapsed_ns': 0, 'work_ns': 0, 'drop_ns': 0}
        row = self.parse(measurement(stage='noop', entries=0, files=0, bytes=0, **durations))
        self.assertEqual(row['elapsed_ns'], 0)
        for stage in ('scan', 'read'):
            with self.subTest(stage=stage), self.assertRaisesRegex(ValueError, 'Empty fixture/measurement'):
                self.parse(measurement(stage=stage, **durations))
        with self.assertRaisesRegex(ValueError, 'Empty fixture/measurement'):
            self.parse(measurement(stage='noop', elapsed_ns=-1, work_ns=-1, drop_ns=0))

    def run_driver(self, directory, mismatch=False, stage='read', identical=False):
        directory = Path(directory).resolve()
        before, after = directory / 'before', directory / 'after'
        before.write_bytes(b'saved baseline')
        after.write_bytes(b'saved baseline' if identical else b'saved candidate')
        source = directory / 'source'
        source.mkdir()
        output = directory / 'report'
        command = ['bench-files-paired.py', '--before', str(before), '--after', str(after),
                   '--source', str(source), '--output', str(output), '--stages', stage,
                   '--readers', '4', '--repeats', '2']
        if identical:
            command.append('--allow-identical-binaries')

        def invoke(arguments, log):
            is_before = arguments[0] == str(before)
            write_measurement(log, measurement(
                stage=stage, readers=4 if stage == 'read' else 1,
                entries=0 if stage == 'noop' else 4, files=0 if stage == 'noop' else 3,
                bytes=0 if stage == 'noop' else 2 * BENCH.MIB,
                counters_schema=2 if is_before or not mismatch else 1,
                cycle_source='windows_process_cycle_time', io_source='windows_process_io_accounting',
                cpu_cycles=100 if is_before else 75, io_read_ops=0))
            return {'process_user_ns': 1000, 'process_kernel_ns': 500, 'process_elapsed_ns': 2000}

        with patch.object(BENCH.sys, 'argv', command), patch.object(BENCH, 'invoke', invoke), contextlib.redirect_stdout(io.StringIO()):
            BENCH.main()
        return output

    def test_driver_keeps_old_names_and_emits_normalized_resource_report(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.run_driver(directory)
            report = json.loads((output / 'comparison.json').read_text(encoding='utf-8'))
            self.assertEqual(report['comparisons'][0]['before_elapsed_ns_mean'], 100)
            self.assertEqual(report['comparisons'][0]['cpu_cycles_ratio_after_before'], .75)
            self.assertEqual(report['comparisons'][0]['cpu_cycles_after_wins'], 2)
            self.assertEqual(report['method']['measurement_contract']['counters_schema'], 2)
            self.assertEqual(report['method']['comparison_kind'], 'A/B')
            self.assertFalse(report['method']['allow_identical_binaries'])
            self.assertFalse(report['method']['identical_binaries'])
            resource = next(row for row in report['resources'] if row['metric'] == 'cpu_cycles' and row['denominator'] == 'entry')
            self.assertEqual(resource['before_mean'], 25)
            self.assertEqual(resource['after_mean'], 18.75)
            self.assertEqual(resource['ratio_after_before'], .75)
            self.assertEqual(resource['paired_n'], 2)
            unsupported = next(row for row in report['resources'] if row['metric'] == 'storage_read_bytes')
            self.assertIsNone(unsupported['before_mean'])
            with (output / 'resources.csv').open(newline='', encoding='utf-8') as stream:
                csv_row = next(row for row in csv.DictReader(stream) if row['metric'] == 'storage_read_bytes')
            self.assertEqual(csv_row['before_mean'], '')
            self.assertEqual(len(report['samples']), 4)

    def test_driver_rejects_mixed_counter_schemas(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ValueError, 'Counter schema/source mismatch'):
            self.run_driver(directory, mismatch=True)

    def test_output_inside_source_is_rejected_before_mutating_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'source'
            source.mkdir()
            binary = Path(directory) / 'binary'
            binary.write_bytes(b'saved binary')
            output = source / 'report'
            command = ['bench-files-paired.py', '--before', str(binary), '--after', str(binary),
                       '--source', str(source), '--output', str(output), '--allow-identical-binaries']
            with patch.object(BENCH.sys, 'argv', command), patch.object(BENCH, 'invoke') as invoke:
                with self.assertRaisesRegex(ValueError, 'Output must be outside the source tree'):
                    BENCH.main()
                invoke.assert_not_called()
            self.assertFalse(output.exists())
            self.assertEqual(list(source.iterdir()), [])

    def test_both_drivers_reject_identical_copies_before_output_mutation(self):
        for driver, extra in ((BENCH, []), (PIPELINE, ['--threads', '1'])):
            with self.subTest(driver=driver.__name__), tempfile.TemporaryDirectory() as directory:
                directory = Path(directory)
                before, after = directory / 'before', directory / 'after'
                before.write_bytes(b'identical executable bytes')
                after.write_bytes(before.read_bytes())
                source = directory / 'source'
                source.mkdir()
                output = directory / 'must-not-create' / 'report'
                command = ['paired.py', '--before', str(before), '--after', str(after),
                           '--source', str(source), '--output', str(output), *extra]
                with patch.object(driver.sys, 'argv', command), patch.object(Path, 'mkdir') as mkdir:
                    with self.assertRaisesRegex(ValueError, 'identical SHA256.*--allow-identical-binaries'):
                        driver.main()
                    mkdir.assert_not_called()
                self.assertFalse(output.parent.exists())
                self.assertEqual(list(source.iterdir()), [])

    def test_explicit_file_calibration_records_identical_binaries(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.run_driver(directory, identical=True)
            report = json.loads((output / 'paired-run.json').read_text(encoding='utf-8'))
            self.assertTrue(report['complete'])
            self.assertTrue(report['allow_identical_binaries'])
            self.assertTrue(report['identical_binaries'])
            self.assertEqual(report['comparison_kind'], 'A/A')
            self.assertEqual(report['binary_sha256']['before'], report['binary_sha256']['after'])

    def test_explicit_pipeline_calibration_preserves_source_and_reader_overrides(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory).resolve()
            binary = directory / 'binary'
            binary.write_bytes(b'saved binary')
            source = directory / 'source'
            source.mkdir()
            output = directory / 'report'
            command = ['bench-paired.py', '--before', str(binary), '--after', str(binary),
                       '--source', str(source), '--output', str(output), '--threads', '1',
                       '--readers', '32', '--before-readers', '16', '--after-readers', '64',
                       '--repeats', '1', '--allow-identical-binaries']

            def run(arguments, *, stdout, stderr, env, check):
                row = dict.fromkeys(PIPELINE.KEY_FIELDS, 'same')
                row.update(repeat_index='1', skipped_files='0', frame_aborts='0', frame_rebuilds='0',
                           files_count='1', raw_read_bytes='1024')
                csv_path = Path(env['FAST_DUMP_BENCH_MATRIX_DIR']) / 'results_raw.csv'
                with csv_path.open('w', newline='', encoding='utf-8') as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(row))
                    writer.writeheader()
                    writer.writerow(row)
                stdout.write(b'[bench-verify] verified fixture\n[bench-link] fixture\n')
                return types.SimpleNamespace(returncode=0)

            with patch.object(PIPELINE.sys, 'argv', command), patch.object(PIPELINE.subprocess, 'run', run), contextlib.redirect_stdout(io.StringIO()):
                PIPELINE.main()
            report = json.loads((output / 'paired-run.json').read_text(encoding='utf-8'))
            self.assertTrue(report['complete'])
            self.assertTrue(report['allow_identical_binaries'])
            self.assertTrue(report['identical_binaries'])
            self.assertEqual(report['comparison_kind'], 'A/A')
            self.assertEqual(report['binary_sha256']['before'], report['binary_sha256']['after'])
            self.assertEqual(report['requested_readers'], {'before': 16, 'after': 64})
            for invocation, count in zip(report['invocations'], ('16', '64')):
                arguments = invocation['command']
                self.assertEqual(arguments[arguments.index('--readers') + 1], count)
                self.assertEqual(arguments[arguments.index('--source') + 1], str(source))
                self.assertNotIn('--dataset', arguments)

    def test_driver_noop_reports_only_total_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.run_driver(directory, stage='noop')
            report = json.loads((output / 'comparison.json').read_text(encoding='utf-8'))
            self.assertEqual(report['method']['scenarios'], [['noop', 1]])
            for resource in report['resources']:
                if resource['denominator'] != 'total':
                    self.assertIsNone(resource['before_mean'])
                    self.assertIsNone(resource['ratio_after_before'])


if __name__ == '__main__':
    unittest.main()
