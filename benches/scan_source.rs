mod pipeline;

use criterion::{criterion_group, criterion_main};
use pipeline::scan_source::bench_scan_source;

criterion_group!(scan_source_benches, bench_scan_source);
criterion_main!(scan_source_benches);
