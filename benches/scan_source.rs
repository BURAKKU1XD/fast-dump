mod pipeline;

use criterion::{Criterion, criterion_group, criterion_main};
use pipeline::scan_source::bench_scan_source;

fn configuration() -> Criterion {
    let output = std::env::var_os("CRITERION_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            pipeline::run_output::create_run_dir("criterion").expect("create Criterion output")
        });
    Criterion::default().output_directory(&output)
}

criterion_group! { name = scan_source_benches; config = configuration(); targets = bench_scan_source }
criterion_main!(scan_source_benches);
