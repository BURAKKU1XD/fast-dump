mod pipeline;

use criterion::{criterion_group, criterion_main};
use pipeline::prepare_dump::bench_prepare_dump;

criterion_group!(prepare_dump_benches, bench_prepare_dump);
criterion_main!(prepare_dump_benches);
