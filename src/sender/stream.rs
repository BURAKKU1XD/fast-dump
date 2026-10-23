use std::io::{self, Read, Write};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use xxhash_rust::xxh3::Xxh3Default;
use zstd::zstd_safe::zstd_sys::ZSTD_EndDirective;
use zstd::zstd_safe::{CCtx, InBuffer};

use crate::network::protocol::{BundleEntryDescriptor, BundlePlanDescriptor, wire};

use super::compress::{BundleCompressor, CompressionOutput, compress_into_vec};
use super::plan::MaterializedPayload;
use super::read::StreamedRegularFile;
use super::tar::{
    BufferedBundleWriter, CountingWriter, ExactFileReader, STREAM_READ_BYTES,
    append_streamed_regular_entry_with_buffer,
};

const QUEUED_CHUNKS: usize = 2;
const STREAM_BUFFERS: usize = QUEUED_CHUNKS + 2;

pub(crate) struct StreamWorkspace {
    buffers: Arc<Mutex<Vec<Vec<u8>>>>,
    read_scratch: Vec<u8>,
}

impl StreamWorkspace {
    pub(crate) fn new() -> Self {
        Self {
            buffers: Arc::new(Mutex::new(Vec::with_capacity(STREAM_BUFFERS))),
            read_scratch: Vec::new(),
        }
    }

    fn take_buffer(&self) -> RecycledBuffer {
        let bytes = self
            .buffers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(wire::MAX_DATA_CHUNK_BYTES));
        RecycledBuffer {
            bytes,
            pool: self.buffers.clone(),
        }
    }
}

struct RecycledBuffer {
    bytes: Vec<u8>,
    pool: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Deref for RecycledBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl DerefMut for RecycledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.bytes
    }
}

impl Drop for RecycledBuffer {
    fn drop(&mut self) {
        if self.bytes.capacity() == 0 {
            return;
        }
        self.bytes.clear();
        // Mutex нужен только при завершении попытки. Между чанками четыре
        // owned буфера возвращаются через прежнюю bounded очередь без этого lock.
        self.pool
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(std::mem::take(&mut self.bytes));
    }
}

pub(crate) struct StreamedPayload {
    pub(crate) tar_bytes: u64,
    pub(crate) compressed_bytes: u64,
    pub(crate) checksum: u64,
}

pub(crate) struct StreamAttempt {
    pub(crate) result: Result<StreamedPayload>,
    pub(crate) read_bytes: u64,
    pub(crate) read_duration: Duration,
    pub(crate) compress_duration: Duration,
    pub(crate) send_duration: Duration,
}

struct Chunk {
    bytes: RecycledBuffer,
    is_last: bool,
}

// Два готовых чанка перекрывают чтение/сжатие с блокирующей записью в сеть.
// Вместе с producer и consumer используются ровно четыре переиспользуемых буфера.
pub(crate) fn send_streamed_attempt_with_workspace(
    stream: &mut impl Write,
    file: &mut StreamedRegularFile,
    bundle_no: u64,
    attempt_no: u32,
    compressor: &mut BundleCompressor,
    workspace: &mut StreamWorkspace,
) -> Result<StreamAttempt> {
    send_attempt_with_workspace(
        stream,
        BundleInput::Streamed(file),
        bundle_no,
        attempt_no,
        compressor,
        workspace,
    )
}

pub(crate) fn send_materialized_attempt_with_workspace(
    stream: &mut impl Write,
    descriptor: &BundlePlanDescriptor,
    payloads: Vec<MaterializedPayload>,
    compressor: &mut BundleCompressor,
    workspace: &mut StreamWorkspace,
) -> Result<StreamAttempt> {
    anyhow::ensure!(
        descriptor.entries.len() == payloads.len(),
        "materialized bundle descriptor/payload count mismatch"
    );
    send_attempt_with_workspace(
        stream,
        BundleInput::Materialized {
            descriptors: &descriptor.entries,
            payloads,
        },
        descriptor.bundle_no,
        1,
        compressor,
        workspace,
    )
}

enum BundleInput<'a> {
    Streamed(&'a mut StreamedRegularFile),
    Materialized {
        descriptors: &'a [BundleEntryDescriptor],
        payloads: Vec<MaterializedPayload>,
    },
}

fn send_attempt_with_workspace(
    stream: &mut impl Write,
    input: BundleInput<'_>,
    bundle_no: u64,
    attempt_no: u32,
    compressor: &mut BundleCompressor,
    workspace: &mut StreamWorkspace,
) -> Result<StreamAttempt> {
    let (chunks_tx, chunks_rx) = bounded(QUEUED_CHUNKS);
    let (buffers_tx, buffers_rx) = bounded(QUEUED_CHUNKS + 1);
    for _ in 0..QUEUED_CHUNKS + 1 {
        buffers_tx
            .send(workspace.take_buffer())
            .map_err(|_| consumer_disconnected())?;
    }
    let pending = workspace.take_buffer();
    if matches!(&input, BundleInput::Streamed(_)) {
        workspace.read_scratch.resize(STREAM_READ_BYTES, 0);
    }
    let scratch = &mut workspace.read_scratch;
    let cancelled = AtomicBool::new(false);

    thread::scope(|scope| {
        let cancelled = &cancelled;
        let _cancel_on_drop = CancelOnDrop(cancelled);
        let producer = thread::Builder::new()
            .name("dump-stream".into())
            .spawn_scoped(scope, move || {
                produce_attempt(
                    input, compressor, chunks_tx, buffers_rx, cancelled, pending, scratch,
                )
            })
            .context("failed to start v4 streamed bundle producer")?;

        let mut send_duration = Duration::ZERO;
        let send_result = (|| -> Result<()> {
            while let Ok(mut chunk) = chunks_rx.recv() {
                let started = Instant::now();
                wire::write_bundle_data_message(
                    stream,
                    bundle_no,
                    attempt_no,
                    chunk.is_last,
                    &chunk.bytes,
                )?;
                send_duration += started.elapsed();
                chunk.bytes.clear();
                // Receiver может уже завершиться; возврат буфера не должен задерживать сеть.
                let _ = buffers_tx.try_send(chunk.bytes);
            }
            let started = Instant::now();
            stream.flush().context("failed to flush v4 streamed data")?;
            send_duration += started.elapsed();
            Ok(())
        })();

        // При сетевой ошибке сначала снимаем оба вида backpressure, затем ждём producer.
        // Текущую попытку полностью завершаем до BundleAbort, исключая поздние записи из worker.
        cancelled.store(true, Ordering::Relaxed);
        drop(chunks_rx);
        drop(buffers_tx);
        let produced = producer.join();
        send_result?;
        let mut attempt =
            produced.map_err(|_| anyhow::anyhow!("v4 streamed bundle producer panicked"))??;
        attempt.send_duration = send_duration;
        Ok(attempt)
    })
}

fn produce_attempt(
    input: BundleInput<'_>,
    compressor: &mut BundleCompressor,
    chunks: Sender<Chunk>,
    buffers: Receiver<RecycledBuffer>,
    cancelled: &AtomicBool,
    pending: RecycledBuffer,
    scratch: &mut [u8],
) -> Result<StreamAttempt> {
    let started = Instant::now();
    let mut sink = ChunkProducer {
        chunks,
        buffers,
        pending,
        compressed_bytes: 0,
        checksum: Xxh3Default::new(),
        queue_wait: Duration::ZERO,
    };
    let file = match input {
        BundleInput::Streamed(file) => file,
        BundleInput::Materialized {
            descriptors,
            payloads,
        } => {
            let mut writer = BufferedBundleWriter::new(compressor, &mut sink)?;
            for (descriptor, payload) in descriptors.iter().zip(payloads) {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(consumer_disconnected().into());
                }
                writer.append_payload(descriptor, payload.mode, &payload.contents)?;
                // После append payload больше не нужен; shared slab освобождается
                // после последнего slice, а descriptors остаются у уже отправленного плана.
            }
            let (_, tar_bytes) = writer.finish()?;
            sink.emit(true)?;
            return Ok(StreamAttempt {
                result: Ok(StreamedPayload {
                    tar_bytes,
                    compressed_bytes: sink.compressed_bytes,
                    checksum: sink.checksum.digest(),
                }),
                read_bytes: 0,
                read_duration: Duration::ZERO,
                compress_duration: started.elapsed().saturating_sub(sink.queue_wait),
                send_duration: Duration::ZERO,
            });
        }
    };
    let forced_short_limit = file.take_forced_short_read_limit();
    let mut reader = ExactFileReader::new(
        &mut file.file,
        &file.abs_path,
        file.descriptor.size_at_open,
        forced_short_limit,
    );
    let (tar_bytes, appended) = {
        let encoder = compressor.direct_encoder(&mut sink)?;
        let mut writer = CountingWriter::new(encoder);
        let appended = append_streamed_regular_entry_with_buffer(
            &mut writer,
            &file.descriptor.relative_path,
            file.mode,
            file.descriptor.size_at_open,
            &mut CancellableReader {
                inner: &mut reader,
                cancelled,
            },
            scratch,
        );
        let tar_bytes = writer.bytes_written();
        if appended.is_ok() {
            writer
                .into_inner()
                .finish()
                .context("failed to finalize v4 streamed compression")?;
        }
        (tar_bytes, appended)
    };
    let result = match appended {
        Ok(()) => {
            sink.emit(true)?;
            Ok(StreamedPayload {
                tar_bytes,
                compressed_bytes: sink.compressed_bytes,
                checksum: sink.checksum.digest(),
            })
        }
        Err(error) => Err(error),
    };
    Ok(StreamAttempt {
        result,
        read_bytes: reader.bytes_read(),
        read_duration: reader.read_duration(),
        compress_duration: started
            .elapsed()
            .saturating_sub(reader.read_duration())
            .saturating_sub(sink.queue_wait),
        send_duration: Duration::ZERO,
    })
}

struct ChunkProducer {
    chunks: Sender<Chunk>,
    buffers: Receiver<RecycledBuffer>,
    pending: RecycledBuffer,
    compressed_bytes: u64,
    checksum: Xxh3Default,
    queue_wait: Duration,
}

impl ChunkProducer {
    fn emit(&mut self, is_last: bool) -> io::Result<()> {
        if self.pending.is_empty() && !is_last {
            return Ok(());
        }
        self.compressed_bytes += self.pending.len() as u64;
        self.checksum.update(&self.pending);
        let waiting = Instant::now();
        let next = if is_last {
            RecycledBuffer {
                bytes: Vec::new(),
                pool: self.pending.pool.clone(),
            }
        } else {
            self.buffers.recv().map_err(|_| consumer_disconnected())?
        };
        let bytes = std::mem::replace(&mut self.pending, next);
        let result = self
            .chunks
            .send(Chunk { bytes, is_last })
            .map_err(|_| consumer_disconnected());
        self.queue_wait += waiting.elapsed();
        result
    }
}

impl CompressionOutput for &mut ChunkProducer {
    fn compress(
        &mut self,
        context: &mut CCtx<'static>,
        input: &mut InBuffer<'_>,
        directive: ZSTD_EndDirective,
    ) -> io::Result<usize> {
        if self.pending.len() == wire::MAX_DATA_CHUNK_BYTES {
            self.emit(false)?;
        }
        compress_into_vec(
            context,
            input,
            directive,
            &mut self.pending.bytes,
            wire::MAX_DATA_CHUNK_BYTES,
        )
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit(false)
    }
}

fn consumer_disconnected() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "v4 streamed consumer disconnected",
    )
}

struct CancellableReader<'a, R> {
    inner: &'a mut R,
    cancelled: &'a AtomicBool,
}

impl<R: Read> Read for CancellableReader<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(consumer_disconnected());
        }
        self.inner.read(bytes)
    }
}

struct CancelOnDrop<'a>(&'a AtomicBool);

impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Cursor;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;
    use crate::network::protocol::{BundleEntryDescriptor, BundleEntryKind};
    use crate::sender::plan::MaterializedBundleTask;
    use crate::sender::read::MaterializedEntry;
    use crate::sender::tar::build_compressed_bundle;
    use crate::tests::TestDir;

    const FIXTURE_BYTES: usize = 16 * 1024 * 1024 + 73;
    const COMPLETION_TIMEOUT: Duration = Duration::from_secs(15);

    fn fixture(size: usize) -> (TestDir, Vec<u8>, StreamedRegularFile) {
        let directory = TestDir::new();
        let path = directory.path().join("payload.bin");
        let mut state = 0x83b4_15e9_2d71_c6af_u64;
        let mut bytes = vec![0_u8; size];
        for chunk in bytes.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        fs::write(&path, &bytes).unwrap();
        let file = File::open(&path).unwrap();
        let metadata = file.metadata().unwrap();
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode()
        };
        #[cfg(not(unix))]
        let mode = 0o644;
        let streamed = StreamedRegularFile {
            descriptor: BundleEntryDescriptor {
                work_ordinal: 11,
                relative_path: "payload.bin".into(),
                kind: BundleEntryKind::Regular,
                size_at_open: metadata.len(),
                best_effort: false,
            },
            mode,
            abs_path: path,
            file,
            forced_short_read_limit: None,
            open_attempts: 1,
        };
        (directory, bytes, streamed)
    }

    // Если scoped join зависнет при ошибке, тест завершится с понятным timeout,
    // а не удержит весь test runner в ожидании заблокированного producer.
    fn completes_without_deadlock(work: impl FnOnce() + Send + 'static) {
        let (done_tx, done_rx) = bounded(1);
        let worker = thread::spawn(move || {
            work();
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(COMPLETION_TIMEOUT)
            .expect("streamed attempt did not complete after cancellation");
        worker.join().unwrap();
    }

    fn send_streamed_attempt(
        stream: &mut impl Write,
        file: &mut StreamedRegularFile,
        bundle_no: u64,
        attempt_no: u32,
        compressor: &mut BundleCompressor,
    ) -> Result<StreamAttempt> {
        send_streamed_attempt_with_workspace(
            stream,
            file,
            bundle_no,
            attempt_no,
            compressor,
            &mut StreamWorkspace::new(),
        )
    }

    fn producer(chunks: Sender<Chunk>, buffers: Receiver<RecycledBuffer>) -> ChunkProducer {
        let mut pending = StreamWorkspace::new().take_buffer();
        pending.resize(wire::MAX_DATA_CHUNK_BYTES, 0xa5);
        ChunkProducer {
            chunks,
            buffers,
            pending,
            compressed_bytes: 0,
            checksum: Xxh3Default::new(),
            queue_wait: Duration::ZERO,
        }
    }

    #[test]
    fn full_chunk_queue_releases_blocked_producer_on_consumer_disconnect() {
        let (chunks_tx, chunks_rx) = bounded(QUEUED_CHUNKS);
        let (buffers_tx, buffers_rx) = bounded(QUEUED_CHUNKS + 1);
        for _ in 0..QUEUED_CHUNKS + 1 {
            buffers_tx
                .send(StreamWorkspace::new().take_buffer())
                .unwrap();
        }
        let mut sink = producer(chunks_tx, buffers_rx);
        for _ in 0..QUEUED_CHUNKS {
            sink.emit(false).unwrap();
            sink.pending.resize(wire::MAX_DATA_CHUNK_BYTES, 0xa5);
        }
        assert_eq!(chunks_rx.len(), QUEUED_CHUNKS);
        assert_eq!(buffers_tx.len(), 1);
        let (done_tx, done_rx) = bounded(1);
        let worker = thread::spawn(move || {
            done_tx.send(sink.emit(false)).unwrap();
        });
        let deadline = Instant::now() + COMPLETION_TIMEOUT;
        while !buffers_tx.is_empty() {
            assert!(
                Instant::now() < deadline,
                "producer did not acquire its last buffer"
            );
            thread::yield_now();
        }
        // Последний запасной буфер уже занят, оба queue slots заполнены:
        // producer гарантированно дошёл до send и не может продвинуться.
        assert!(matches!(
            done_rx.try_recv(),
            Err(crossbeam_channel::TryRecvError::Empty)
        ));
        drop(chunks_rx);
        let error = done_rx
            .recv_timeout(COMPLETION_TIMEOUT)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        worker.join().unwrap();
    }

    #[test]
    fn exhausted_recycle_pool_releases_producer_when_writer_disappears() {
        let (chunks_tx, chunks_rx) = bounded(QUEUED_CHUNKS);
        let (buffers_tx, buffers_rx) = bounded(QUEUED_CHUNKS + 1);
        let mut sink = producer(chunks_tx, buffers_rx);
        let (started_tx, started_rx) = bounded(1);
        let (done_tx, done_rx) = bounded(1);
        let worker = thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx.send(sink.emit(false)).unwrap();
        });
        started_rx.recv_timeout(COMPLETION_TIMEOUT).unwrap();
        assert!(matches!(
            done_rx.try_recv(),
            Err(crossbeam_channel::TryRecvError::Empty)
        ));
        assert!(chunks_rx.is_empty());
        // Даже при живом chunks receiver закрытие пула снимает ожидание recycle.
        drop(buffers_tx);
        let error = done_rx
            .recv_timeout(COMPLETION_TIMEOUT)
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        worker.join().unwrap();
    }

    #[derive(Clone, Copy)]
    enum WriterFailure {
        BrokenPipe,
        Panic,
        Flush,
    }

    struct FailingWriter {
        failure: WriterFailure,
        fail_after: usize,
        written: usize,
        flushes: usize,
    }

    impl Write for FailingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.written >= self.fail_after {
                match self.failure {
                    WriterFailure::BrokenPipe => {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "test writer disconnected",
                        ));
                    }
                    WriterFailure::Panic => panic!("test writer panicked"),
                    WriterFailure::Flush => {}
                }
            }
            let written = bytes.len().min(self.fail_after - self.written);
            self.written += written;
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            if matches!(self.failure, WriterFailure::Flush) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "test flush failed",
                ));
            }
            Ok(())
        }
    }

    #[test]
    fn writer_disconnect_after_payload_joins_producer() {
        completes_without_deadlock(|| {
            let (_directory, _contents, mut file) = fixture(FIXTURE_BYTES);
            let mut compressor = BundleCompressor::new(1, 1).unwrap();
            let mut workspace = StreamWorkspace::new();
            let mut writer = FailingWriter {
                failure: WriterFailure::BrokenPipe,
                fail_after: 2 * wire::MAX_DATA_CHUNK_BYTES,
                written: 0,
                flushes: 0,
            };
            let error = send_streamed_attempt_with_workspace(
                &mut writer,
                &mut file,
                7,
                1,
                &mut compressor,
                &mut workspace,
            )
            .err()
            .unwrap();
            assert_eq!(workspace.buffers.lock().unwrap().len(), STREAM_BUFFERS);
            assert_eq!(writer.written, 2 * wire::MAX_DATA_CHUNK_BYTES);
            assert_eq!(writer.flushes, 0);
            assert!(error.chain().any(|cause| {
                cause
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
            }));
        });
    }

    #[test]
    fn writer_panic_unwinds_after_joining_producer() {
        completes_without_deadlock(|| {
            let (_directory, _contents, mut file) = fixture(FIXTURE_BYTES);
            let mut compressor = BundleCompressor::new(1, 1).unwrap();
            let mut workspace = StreamWorkspace::new();
            let mut writer = FailingWriter {
                failure: WriterFailure::Panic,
                fail_after: wire::MAX_DATA_CHUNK_BYTES,
                written: 0,
                flushes: 0,
            };
            let panic = catch_unwind(AssertUnwindSafe(|| {
                let _ = send_streamed_attempt_with_workspace(
                    &mut writer,
                    &mut file,
                    7,
                    1,
                    &mut compressor,
                    &mut workspace,
                );
            }))
            .unwrap_err();
            assert_eq!(panic.downcast_ref::<&str>(), Some(&"test writer panicked"));
            assert_eq!(writer.written, wire::MAX_DATA_CHUNK_BYTES);
            assert_eq!(writer.flushes, 0);
            assert_eq!(workspace.buffers.lock().unwrap().len(), STREAM_BUFFERS);
        });
    }

    #[test]
    fn final_flush_failure_is_returned_after_producer_finishes() {
        completes_without_deadlock(|| {
            let (_directory, _contents, mut file) = fixture(1024 + 73);
            let mut compressor = BundleCompressor::new(1, 1).unwrap();
            let mut writer = FailingWriter {
                failure: WriterFailure::Flush,
                fail_after: usize::MAX,
                written: 0,
                flushes: 0,
            };
            let error = send_streamed_attempt(&mut writer, &mut file, 7, 1, &mut compressor)
                .err()
                .unwrap();
            assert!(writer.written > 0);
            assert_eq!(writer.flushes, 1);
            assert!(format!("{error:#}").contains("test flush failed"));
        });
    }

    fn decode_chunks(wire_bytes: &[u8], bundle_no: u64, attempt_no: u32) -> Vec<wire::BundleData> {
        let mut input = Cursor::new(wire_bytes);
        let mut chunks = Vec::new();
        while input.position() < wire_bytes.len() as u64 {
            let wire::Message::BundleData(chunk) = wire::read_message(&mut input).unwrap() else {
                panic!("unexpected streamed message");
            };
            assert_eq!(chunk.bundle_no, bundle_no);
            assert_eq!(chunk.attempt_no, attempt_no);
            assert!(chunk.bytes.len() <= wire::MAX_DATA_CHUNK_BYTES);
            chunks.push(chunk);
        }
        chunks
    }

    fn verify_payload(chunks: &[wire::BundleData], payload: &StreamedPayload, contents: &[u8]) {
        assert!(!chunks.is_empty());
        assert!(chunks.last().unwrap().is_last);
        assert!(
            chunks[..chunks.len() - 1]
                .iter()
                .all(|chunk| !chunk.is_last)
        );
        let compressed = chunks
            .iter()
            .flat_map(|chunk| chunk.bytes.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(compressed.len() as u64, payload.compressed_bytes);
        assert_eq!(xxhash_rust::xxh3::xxh3_64(&compressed), payload.checksum);
        let tar_bytes = zstd::stream::decode_all(compressed.as_slice()).unwrap();
        assert_eq!(tar_bytes.len() as u64, payload.tar_bytes);
        let mut archive = tar::Archive::new(tar_bytes.as_slice());
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().unwrap().unwrap();
        assert_eq!(
            entry.path().unwrap().as_ref(),
            std::path::Path::new("payload.bin")
        );
        assert_eq!(entry.size(), contents.len() as u64);
        let mut restored = Vec::new();
        entry.read_to_end(&mut restored).unwrap();
        assert!(restored == contents, "streamed archive bytes differ");
        drop(entry);
        assert!(entries.next().is_none());
    }

    fn materialized_entry(path: &str, contents: Vec<u8>) -> MaterializedEntry {
        MaterializedEntry {
            descriptor: BundleEntryDescriptor {
                work_ordinal: 1,
                relative_path: path.into(),
                kind: BundleEntryKind::Regular,
                size_at_open: contents.len() as u64,
                best_effort: false,
            },
            mode: 0o644,
            contents: contents.into(),
        }
    }

    fn materialized_task(entries: Vec<MaterializedEntry>) -> MaterializedBundleTask {
        let last = &entries.last().unwrap().descriptor;
        let mut descriptor = BundlePlanDescriptor {
            bundle_no: 8,
            first_work_ordinal: entries[0].descriptor.work_ordinal,
            last_work_ordinal: last.work_ordinal,
            last_relative_path: last.relative_path.clone(),
            entries: Vec::with_capacity(entries.len()),
            raw_payload_bytes: 0,
        };
        let payloads = entries
            .into_iter()
            .map(|entry| {
                descriptor.raw_payload_bytes += entry.descriptor.size_at_open;
                descriptor.entries.push(entry.descriptor);
                MaterializedPayload {
                    mode: entry.mode,
                    contents: entry.contents,
                }
            })
            .collect();
        MaterializedBundleTask {
            descriptor,
            payloads,
        }
    }

    fn pool_pointers(workspace: &StreamWorkspace) -> Vec<usize> {
        let mut pointers = workspace
            .buffers
            .lock()
            .unwrap()
            .iter()
            .map(|buffer| buffer.as_ptr() as usize)
            .collect::<Vec<_>>();
        pointers.sort_unstable();
        assert_eq!(pointers.len(), STREAM_BUFFERS);
        pointers
    }

    fn verify_materialized_payload(
        wire_bytes: &[u8],
        payload: &StreamedPayload,
        expected: &[u8],
        expected_tar_bytes: u64,
    ) {
        let chunks = decode_chunks(wire_bytes, 8, 1);
        assert!(!chunks.is_empty());
        assert!(chunks.last().unwrap().is_last);
        assert!(
            chunks[..chunks.len() - 1]
                .iter()
                .all(|chunk| !chunk.is_last)
        );
        let compressed = chunks
            .into_iter()
            .flat_map(|chunk| chunk.bytes)
            .collect::<Vec<_>>();
        assert!(compressed == expected, "materialized frame bytes changed");
        assert_eq!(payload.tar_bytes, expected_tar_bytes);
        assert_eq!(payload.compressed_bytes, expected.len() as u64);
        assert_eq!(payload.checksum, xxhash_rust::xxh3::xxh3_64(expected));
        assert_eq!(
            zstd::stream::decode_all(compressed.as_slice())
                .unwrap()
                .len() as u64,
            expected_tar_bytes
        );
    }

    #[test]
    fn materialized_and_streamed_frames_share_context_and_bounded_buffers() {
        for threads in [1, 4] {
            let (_directory, contents, mut file) = fixture(FIXTURE_BYTES);
            let long_path = format!("{}/empty", "nested/ёж/".repeat(20));
            let mut link = materialized_entry("link", Vec::new());
            link.descriptor.kind = BundleEntryKind::Symlink {
                target: long_path.clone(),
            };
            let mut entries = vec![
                materialized_entry("payload.bin", contents.clone()),
                materialized_entry(&long_path, Vec::new()),
                link,
                materialized_entry("tail", vec![0xa5; 513]),
            ];
            for (index, entry) in entries.iter_mut().enumerate() {
                entry.descriptor.work_ordinal = index as u64 + 1;
            }
            let (reference, reference_tar_bytes) =
                build_compressed_bundle(&entries, 1, threads).unwrap();
            let task = materialized_task(entries);
            let mut compressor = BundleCompressor::new(1, threads).unwrap();
            let mut workspace = StreamWorkspace::new();
            let mut first_wire = Vec::new();
            let first = send_materialized_attempt_with_workspace(
                &mut first_wire,
                &task.descriptor,
                task.payloads,
                &mut compressor,
                &mut workspace,
            )
            .unwrap();
            assert_eq!(first.read_bytes, 0);
            assert_eq!(first.read_duration, Duration::ZERO);
            assert!(workspace.read_scratch.is_empty());
            let payload = first.result.unwrap();
            assert!(payload.compressed_bytes > 4 * wire::MAX_DATA_CHUNK_BYTES as u64);
            verify_materialized_payload(&first_wire, &payload, &reference, reference_tar_bytes);
            let original_pointers = pool_pointers(&workspace);

            let mut streamed_wire = Vec::new();
            let streamed = send_streamed_attempt_with_workspace(
                &mut streamed_wire,
                &mut file,
                9,
                1,
                &mut compressor,
                &mut workspace,
            )
            .unwrap();
            assert_eq!(streamed.read_bytes, contents.len() as u64);
            verify_payload(
                &decode_chunks(&streamed_wire, 9, 1),
                &streamed.result.unwrap(),
                &contents,
            );
            assert_eq!(pool_pointers(&workspace), original_pointers);
            let scratch_pointer = workspace.read_scratch.as_ptr();

            let tail = vec![materialized_entry(
                "tail",
                b"independent final frame".to_vec(),
            )];
            let (reference, reference_tar_bytes) =
                build_compressed_bundle(&tail, 1, threads).unwrap();
            let tail = materialized_task(tail);
            let mut tail_wire = Vec::new();
            let tail = send_materialized_attempt_with_workspace(
                &mut tail_wire,
                &tail.descriptor,
                tail.payloads,
                &mut compressor,
                &mut workspace,
            )
            .unwrap();
            verify_materialized_payload(
                &tail_wire,
                &tail.result.unwrap(),
                &reference,
                reference_tar_bytes,
            );
            assert_eq!(pool_pointers(&workspace), original_pointers);
            assert_eq!(workspace.read_scratch.as_ptr(), scratch_pointer);
        }
    }

    #[test]
    fn materialized_writer_failure_and_panic_join_producer_and_reclaim_buffers() {
        for failure in [WriterFailure::BrokenPipe, WriterFailure::Panic] {
            completes_without_deadlock(move || {
                use crate::sender::read::{EntryContents, ReadDecision, materialize_batch};
                use crate::sender::scan::{ScannedEntry, ScannedEntryKind};

                let (directory, contents, _file) = fixture(FIXTURE_BYTES);
                let batch = std::array::from_fn(|index| {
                    let relative_path = format!("small-{index}");
                    let abs_path = directory.path().join(&relative_path);
                    fs::write(&abs_path, [index as u8; 333]).unwrap();
                    Some(ScannedEntry {
                        work_ordinal: index as u64 + 2,
                        abs_path,
                        relative_path,
                        kind: ScannedEntryKind::Regular,
                    })
                });
                let shared = materialize_batch(batch, 1024)
                    .into_iter()
                    .map(|result| match result.unwrap().unwrap() {
                        ReadDecision::Accepted(entry) => entry,
                        _ => panic!("fixture entry was not prefetched"),
                    })
                    .collect::<Vec<_>>();
                let EntryContents::Shared { bytes, .. } = &shared[0].contents else {
                    panic!("fixture must use a shared slab");
                };
                let slab = Arc::downgrade(bytes);
                let mut entries = vec![materialized_entry("payload.bin", contents)];
                entries.extend(shared);
                let task = materialized_task(entries);
                let mut compressor = BundleCompressor::new(1, 4).unwrap();
                let mut workspace = StreamWorkspace::new();
                let mut writer = FailingWriter {
                    failure,
                    fail_after: wire::MAX_DATA_CHUNK_BYTES,
                    written: 0,
                    flushes: 0,
                };
                let result = catch_unwind(AssertUnwindSafe(|| {
                    send_materialized_attempt_with_workspace(
                        &mut writer,
                        &task.descriptor,
                        task.payloads,
                        &mut compressor,
                        &mut workspace,
                    )
                }));
                match failure {
                    WriterFailure::BrokenPipe => assert!(result.unwrap().is_err()),
                    WriterFailure::Panic => assert!(result.is_err()),
                    WriterFailure::Flush => unreachable!(),
                }
                assert_eq!(writer.written, wire::MAX_DATA_CHUNK_BYTES);
                assert_eq!(writer.flushes, 0);
                assert!(
                    slab.upgrade().is_none(),
                    "cancelled payload slab was retained"
                );
                let original_pointers = pool_pointers(&workspace);

                // Неоконченный frame не отравляет следующий: reset того же CCtx
                // и тот же набор буферов дают полноценный независимый архив.
                let retry = vec![materialized_entry(
                    "retry",
                    b"clean independent frame".to_vec(),
                )];
                let (reference, reference_tar_bytes) =
                    build_compressed_bundle(&retry, 1, 4).unwrap();
                let retry = materialized_task(retry);
                let mut wire = Vec::new();
                let sent = send_materialized_attempt_with_workspace(
                    &mut wire,
                    &retry.descriptor,
                    retry.payloads,
                    &mut compressor,
                    &mut workspace,
                )
                .unwrap();
                verify_materialized_payload(
                    &wire,
                    &sent.result.unwrap(),
                    &reference,
                    reference_tar_bytes,
                );
                assert_eq!(pool_pointers(&workspace), original_pointers);
            });
        }
    }

    #[test]
    fn streamed_chunks_preserve_payload_checksum_last_marker_and_deterministic_bytes() {
        let (_directory, contents, mut file) = fixture(FIXTURE_BYTES);
        let mut compressor = BundleCompressor::new(1, 1).unwrap();
        let mut workspace = StreamWorkspace::new();
        let mut first_wire = Vec::new();
        let first = send_streamed_attempt_with_workspace(
            &mut first_wire,
            &mut file,
            7,
            1,
            &mut compressor,
            &mut workspace,
        )
        .unwrap();
        assert_eq!(first.read_bytes, contents.len() as u64);
        let payload = first.result.unwrap();
        let chunks = decode_chunks(&first_wire, 7, 1);
        assert!(
            chunks.len() >= 5,
            "fixture did not exercise the bounded chunk pool"
        );
        assert!(payload.compressed_bytes > 4 * wire::MAX_DATA_CHUNK_BYTES as u64);
        verify_payload(&chunks, &payload, &contents);
        let pointers = |workspace: &StreamWorkspace| {
            let mut pointers = workspace
                .buffers
                .lock()
                .unwrap()
                .iter()
                .map(|buffer| buffer.as_ptr() as usize)
                .collect::<Vec<_>>();
            pointers.sort_unstable();
            assert_eq!(pointers.len(), STREAM_BUFFERS);
            pointers
        };
        let original_pointers = pointers(&workspace);
        let scratch_pointer = workspace.read_scratch.as_ptr();

        file.reopen_for_retry().unwrap();
        let mut second_wire = Vec::new();
        let second = send_streamed_attempt_with_workspace(
            &mut second_wire,
            &mut file,
            7,
            1,
            &mut compressor,
            &mut workspace,
        )
        .unwrap();
        assert_eq!(second.read_bytes, first.read_bytes);
        assert_eq!(second.result.unwrap().checksum, payload.checksum);
        assert_eq!(pointers(&workspace), original_pointers);
        assert_eq!(workspace.read_scratch.as_ptr(), scratch_pointer);
        assert!(
            second_wire == first_wire,
            "reused compressor changed deterministic wire bytes"
        );
    }

    #[test]
    fn short_read_after_emitted_payload_allows_clean_retry_with_same_compressor() {
        let (_directory, contents, mut file) = fixture(FIXTURE_BYTES);
        let forced_limit = 12 * 1024 * 1024;
        file.forced_short_read_limit = Some(forced_limit);
        let mut compressor = BundleCompressor::new(1, 1).unwrap();
        let mut workspace = StreamWorkspace::new();
        let mut aborted_wire = Vec::new();
        let aborted = send_streamed_attempt_with_workspace(
            &mut aborted_wire,
            &mut file,
            7,
            1,
            &mut compressor,
            &mut workspace,
        )
        .unwrap();
        assert_eq!(workspace.buffers.lock().unwrap().len(), STREAM_BUFFERS);
        assert_eq!(aborted.read_bytes, forced_limit);
        let error = aborted.result.err().unwrap();
        assert!(format!("{error:#}").contains("short read"));
        let aborted_chunks = decode_chunks(&aborted_wire, 7, 1);
        assert!(aborted_chunks.iter().all(|chunk| !chunk.is_last));
        assert!(
            aborted_chunks
                .iter()
                .map(|chunk| chunk.bytes.len())
                .sum::<usize>()
                >= 2 * wire::MAX_DATA_CHUNK_BYTES
        );

        file.reopen_for_retry().unwrap();
        let mut retry_wire = Vec::new();
        let retry = send_streamed_attempt_with_workspace(
            &mut retry_wire,
            &mut file,
            7,
            2,
            &mut compressor,
            &mut workspace,
        )
        .unwrap();
        assert_eq!(workspace.buffers.lock().unwrap().len(), STREAM_BUFFERS);
        assert_eq!(retry.read_bytes, contents.len() as u64);
        assert_eq!(file.open_attempts, 2);
        verify_payload(
            &decode_chunks(&retry_wire, 7, 2),
            &retry.result.unwrap(),
            &contents,
        );
    }
}
