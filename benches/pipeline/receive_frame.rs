use criterion::{BatchSize, Criterion, Throughput, black_box};
use fast_dump::receiver::{ReceiverState, receive_frame};
use fast_dump::sender::prepare_dump;

use super::shared::{
    BenchDir, BenchStream, create_single_large_file_fixture, empty_resume_state,
    encode_prepared_frame, receive_frame_group_sample_size, total_compressed_bytes,
};

pub(crate) fn bench_receive_frame_commit(c: &mut Criterion) {
    let source_dir = BenchDir::new("receive-frame-source");
    create_single_large_file_fixture(source_dir.path(), 16 * 1024 * 1024);
    let dump = prepare_dump(
        source_dir.path().to_path_buf(),
        &empty_resume_state(),
        3,
        1,
        32 * 1024 * 1024,
    )
    .unwrap();
    let frame = dump.frames.into_iter().next().unwrap();
    let encoded_frame = encode_prepared_frame(&frame);
    let compressed_bytes = total_compressed_bytes(&frame);

    let mut group = c.benchmark_group("receive_frame_commit");
    group.sample_size(receive_frame_group_sample_size());
    group.throughput(Throughput::Bytes(compressed_bytes));
    group.bench_function("single_frame_16mib_commit", |b| {
        b.iter_batched(
            || {
                let receiver_dir = BenchDir::new("receive-frame-data");
                let state =
                    ReceiverState::open(receiver_dir.path().to_path_buf(), "bench-frame").unwrap();
                let stream = BenchStream::new(&encoded_frame);
                (receiver_dir, state, stream)
            },
            |(receiver_dir, mut state, mut stream)| {
                let ack = receive_frame(&mut stream, &mut state).unwrap();
                black_box(receiver_dir.path().to_path_buf());
                black_box(ack.frame_no);
                black_box(ack.archive_len);
                black_box(stream.written.len());
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}
