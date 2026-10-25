# Results

Run `python scripts/results.py index` from the repository root and open the
generated `results/index.html`. It works offline, including after a fresh clone.

- `benchmarks/<id>/`: tracked raw measurements, parameters and compact summaries.
- `tests/<id>/`: tracked verification commands, statuses and summaries.
- `maintenance/`: tracked cleanup receipts and validation summaries.
- `local/`: ignored full logs, reports and import/cleanup inventories.
- `history/`: ignored historical evidence; original manifests describe the
  archive at the time they were written, before later documented cleanup.

Use `python scripts/results.py record --kind benchmark --label NAME -- COMMAND...`
for new measurements, or `import PATH` to retain existing results. For paired
drivers pass `--output {output}` to select a fresh directory inside the recorded
run (quote `'{output}'` in PowerShell). No command stages or commits files.

Unknown, incomplete, invalid and superseded measurements remain visible.
An automatic comparison requires compatible conditions and complete measurements
of the selected metric. Binaries and generated dump payloads are disposable;
their hashes and verification evidence remain in the manifests.

See [the benchmark guide](../docs/benchmarks.md) for metric definitions,
comparison methodology and detailed commands.

The [2026-10-25 cleanup report](maintenance/2026-10-25-cleanup.json) links the
validation runs and A/A and A/B measurements used for this reorganization.
