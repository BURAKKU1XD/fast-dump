mod pipeline;

use criterion::{criterion_group, criterion_main};
use pipeline::receive_frame::bench_receive_frame_commit;

criterion_group!(receive_frame_commit_benches, bench_receive_frame_commit);
criterion_main!(receive_frame_commit_benches);
