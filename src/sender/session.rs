use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use log::debug;
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::{
    self, Ack, Finish, FrameChunk, FrameEnd, FrameStart, Message, ResumeState,
};

use super::frame::{append_tar_header, append_tar_padding, stream_staged_snapshot};
use super::{
    FrameBuildStats, InputFile, OutboundFrame, PendingFileWalker, PendingFrameFiles,
    PipelineProfile, PipelineProfileCollector, ReaderPool,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendStatus {
    Clean,
    CompletedWithWarnings,
}

impl std::fmt::Display for SendStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Clean => f.write_str("clean"),
            Self::CompletedWithWarnings => f.write_str("completed_with_warnings"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendReport {
    pub dump_id: String,
    pub resume_state: ResumeState,
    pub sent_frames: usize,
    pub final_frame_no: u64,
    pub final_file_index: u64,
    pub final_file_path: String,
    pub status: SendStatus,
    pub files_total_discovered: u64,
    pub files_sent_clean: u64,
    pub files_sent_best_effort: u64,
    pub files_skipped_open_failed: u64,
    pub files_skipped_unstable_after_retries: u64,
    pub retries_total: u64,
    pub bytes_sent_raw: u64,
    pub bytes_sent_best_effort_raw: u64,
    pub final_archive_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendOptions {
    pub level: i32,
    pub threads: u32,
    pub frame_target_bytes: u64,
    pub min_readers: usize,
    pub max_readers: usize,
    pub read_buffer_bytes: usize,
}

fn merge_frame_stats(total: &mut FrameBuildStats, frame: &FrameBuildStats) {
    total.files_sent_clean += frame.files_sent_clean;
    total.files_sent_best_effort += frame.files_sent_best_effort;
    total.files_skipped_open_failed += frame.files_skipped_open_failed;
    total.files_skipped_unstable_after_retries += frame.files_skipped_unstable_after_retries;
    total.retries_total = total.retries_total.saturating_add(frame.retries_total);
    total.bytes_sent_raw = total.bytes_sent_raw.saturating_add(frame.bytes_sent_raw);
    total.bytes_sent_best_effort_raw = total
        .bytes_sent_best_effort_raw
        .saturating_add(frame.bytes_sent_best_effort_raw);
    total.tar_bytes_sent = total.tar_bytes_sent.saturating_add(frame.tar_bytes_sent);
}

fn final_archive_footer_bytes() -> Result<u64> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 0)
        .context("failed to create zstd encoder for local archive footer estimate")?;
    encoder
        .write_all(&[0_u8; 1024])
        .context("failed to encode tar end blocks for local archive footer estimate")?;
    let footer = encoder
        .finish()
        .context("failed to finish local archive footer estimate")?;
    Ok(footer.len() as u64)
}

fn bytes_per_sec(bytes: u64, elapsed: Duration) -> u64 {
    let nanos = elapsed.as_nanos();
    if nanos == 0 {
        return bytes;
    }

    ((bytes as u128) * 1_000_000_000 / nanos) as u64
}

fn validate_send_options(options: &SendOptions) -> Result<()> {
    if options.threads == 0 {
        bail!("threads must be at least 1");
    }
    if options.frame_target_bytes == 0 {
        bail!("frame_target_bytes must be at least 1");
    }
    if options.min_readers == 0 {
        bail!("min_readers must be at least 1");
    }
    if options.max_readers == 0 {
        bail!("max_readers must be at least 1");
    }
    if options.min_readers > options.max_readers {
        bail!(
            "min_readers must not be greater than max_readers: {} > {}",
            options.min_readers,
            options.max_readers
        );
    }
    if options.read_buffer_bytes == 0 {
        bail!("read_buffer_bytes must be at least 1");
    }

    Ok(())
}

struct StreamingFrameResult {
    outbound: OutboundFrame,
    stats: FrameBuildStats,
    ack: Ack,
    compressed_len: u64,
}

struct ProtocolChunkWriter<'a, T> {
    stream: &'a mut T,
    chunk_limit: usize,
    buffer: Vec<u8>,
    compressed_len: u64,
    hasher: Xxh3Default,
}

impl<'a, T> ProtocolChunkWriter<'a, T>
where
    T: std::io::Read + std::io::Write,
{
    fn new(stream: &'a mut T, chunk_limit: usize) -> Self {
        Self {
            stream,
            chunk_limit,
            buffer: Vec::with_capacity(chunk_limit),
            compressed_len: 0,
            hasher: Xxh3Default::new(),
        }
    }

    fn total_written(&self) -> u64 {
        self.compressed_len
            .saturating_add(self.buffer.len().min(u64::MAX as usize) as u64)
    }

    fn finish_frame(mut self, frame: &OutboundFrame) -> Result<(Ack, u64)> {
        self.flush_chunk()?;
        protocol::write_message(
            self.stream,
            &Message::FrameEnd(FrameEnd {
                frame_no: frame.frame_no,
                last_file_index: frame.last_file_index,
                last_file_path: frame.last_file_path.clone(),
                compressed_len: self.compressed_len,
                compressed_xxh3_64: self.hasher.digest(),
            }),
        )?;
        self.stream
            .flush()
            .context("failed to flush frame to peer")?;
        let ack = read_frame_ack(self.stream, frame)?;
        Ok((ack, self.compressed_len))
    }

    fn flush_chunk(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.hasher.update(&self.buffer);
        self.compressed_len = self
            .compressed_len
            .checked_add(self.buffer.len() as u64)
            .context("frame compressed length overflow")?;
        protocol::write_message(
            self.stream,
            &Message::FrameChunk(FrameChunk {
                bytes: std::mem::take(&mut self.buffer),
            }),
        )?;
        self.buffer = Vec::with_capacity(self.chunk_limit);
        Ok(())
    }
}

impl<T> Write for ProtocolChunkWriter<'_, T>
where
    T: std::io::Read + std::io::Write,
{
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        let original_len = buf.len();
        while !buf.is_empty() {
            if self.buffer.len() == self.chunk_limit {
                self.flush_chunk().map_err(io::Error::other)?;
            }
            let remaining = self.chunk_limit.saturating_sub(self.buffer.len());
            let take = remaining.min(buf.len());
            self.buffer.extend_from_slice(&buf[..take]);
            buf = &buf[take..];
            if self.buffer.len() == self.chunk_limit {
                self.flush_chunk().map_err(io::Error::other)?;
            }
        }
        Ok(original_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_chunk().map_err(io::Error::other)
    }
}

fn next_streaming_file(pending: &PendingFrameFiles) -> Result<InputFile> {
    pending
        .next_expected()
        .cloned()
        .context("sender streaming frame lost its scheduled file before processing")
}

fn consume_streaming_file(pending: &mut PendingFrameFiles) -> Result<InputFile> {
    pending
        .consume_expected()
        .context("sender streaming frame lost its scheduled file after processing")
}

fn send_streaming_frame<T>(
    stream: &mut T,
    pending: &mut PendingFrameFiles,
    frame_no: u64,
    options: &SendOptions,
    readers: &mut ReaderPool,
    profile: Option<&PipelineProfileCollector>,
) -> Result<Option<StreamingFrameResult>>
where
    T: std::io::Read + std::io::Write,
{
    let _ = pending.prefetch(readers)?;
    let Some(first) = pending.next_expected().cloned() else {
        return Ok(None);
    };

    let build_started = Instant::now();
    let mut stats = FrameBuildStats::default();
    let outbound_first = OutboundFrame {
        frame_no,
        first_file_index: first.index,
        first_file_path: first.rel_path.clone(),
        last_file_index: first.index,
        last_file_path: first.rel_path.clone(),
        level: options.level,
        threads: options.threads,
    };
    protocol::write_message(
        stream,
        &Message::FrameStart(FrameStart {
            frame_no,
            first_file_index: outbound_first.first_file_index,
            first_file_path: outbound_first.first_file_path.clone(),
            level: options.level,
            threads: options.threads,
        }),
    )?;

    let mut encoder = zstd::stream::write::Encoder::new(
        ProtocolChunkWriter::new(stream, protocol::MAX_FRAME_CHUNK_BYTES),
        options.level,
    )
    .context("failed to create zstd encoder for sender frame")?;
    encoder
        .multithread(options.threads)
        .context("failed to configure zstd worker threads for sender frame")?;

    let last = loop {
        let file = next_streaming_file(pending)?;
        match readers.next_for_with_scheduler(file.index, |pool| pending.prefetch(pool))? {
            super::ReadEvent::Ready(super::FileReadOutcome::Snapshot(snapshot)) => {
                let header_started = Instant::now();
                append_tar_header(&mut encoder, &snapshot.file.rel_path, snapshot.size_at_open)?;
                if let Some(profile) = profile {
                    profile.record_compress(512, header_started.elapsed());
                }
                stream_staged_snapshot(&mut encoder, &snapshot, profile)?;
                let padding_bytes = super::frame::tar_entry_bytes(snapshot.size_at_open)
                    - 512
                    - snapshot.size_at_open;
                let padding_started = Instant::now();
                append_tar_padding(&mut encoder, &snapshot.file.rel_path, snapshot.size_at_open)?;
                if let Some(profile) = profile {
                    profile.record_compress(padding_bytes, padding_started.elapsed());
                }
                let flush_started = Instant::now();
                encoder
                    .flush()
                    .context("failed to flush zstd encoder at sender file boundary")?;
                if let Some(profile) = profile {
                    profile.record_compress(0, flush_started.elapsed());
                }
                stats.note_snapshot(
                    snapshot.disposition,
                    snapshot.size_at_open,
                    snapshot.retries,
                );
            }
            super::ReadEvent::Ready(super::FileReadOutcome::Skip(skipped)) => {
                stats.note_skip(skipped.reason, skipped.retries);
            }
            super::ReadEvent::Fatal { message, .. } => anyhow::bail!("{message}"),
        }
        let last = consume_streaming_file(pending)?;
        let _ = pending.prefetch(readers)?;
        if pending.next_expected().is_none()
            || encoder.get_ref().total_written() >= options.frame_target_bytes
        {
            break last;
        }
    };

    let finish_started = Instant::now();
    let writer = encoder
        .finish()
        .context("failed to finish sender zstd frame")?;
    if let Some(profile) = profile {
        profile.record_compress(0, finish_started.elapsed());
    }
    let outbound = OutboundFrame {
        frame_no,
        first_file_index: outbound_first.first_file_index,
        first_file_path: outbound_first.first_file_path,
        last_file_index: last.index,
        last_file_path: last.rel_path,
        level: options.level,
        threads: options.threads,
    };
    let (ack, compressed_len) = writer.finish_frame(&outbound)?;
    debug!(
        "sender streaming frame completed: frame_no={} first_file={} last_file={} raw_file_bytes={} raw_tar_bytes={} compressed_bytes={} elapsed_ms={}",
        outbound.frame_no,
        outbound.first_file_path,
        outbound.last_file_path,
        stats.bytes_sent_raw,
        stats.tar_bytes_sent,
        compressed_len,
        build_started.elapsed().as_millis()
    );
    Ok(Some(StreamingFrameResult {
        outbound,
        stats,
        ack,
        compressed_len,
    }))
}

// Этот helper соединяет resume bootstrap и передачу всех frame-ов в одну sender-side сессию.
// Благодаря этому `main` и интеграционные тесты могут работать уже с готовой dump-операцией,
// а не вручную повторять protocol loop.
pub fn send_dump<T>(
    stream: &mut T,
    source: impl Into<PathBuf>,
    dump_id: &str,
    options: &SendOptions,
) -> Result<SendReport>
where
    T: std::io::Read + std::io::Write,
{
    send_dump_impl(stream, source, dump_id, options, None)
}

// Этот helper нужен бенчмаркам: он возвращает профиль по стадиям за один прогон,
// но оставляет основной wire protocol и runtime semantics без изменений.
pub fn send_dump_profile<T>(
    stream: &mut T,
    source: impl Into<PathBuf>,
    dump_id: &str,
    options: &SendOptions,
) -> Result<(SendReport, PipelineProfile)>
where
    T: std::io::Read + std::io::Write,
{
    let profile = PipelineProfileCollector::default();
    let report = send_dump_impl(stream, source, dump_id, options, Some(&profile))?;
    Ok((report, profile.finish()))
}

fn send_dump_impl<T>(
    stream: &mut T,
    source: impl Into<PathBuf>,
    dump_id: &str,
    options: &SendOptions,
    profile: Option<&PipelineProfileCollector>,
) -> Result<SendReport>
where
    T: std::io::Read + std::io::Write,
{
    let session_started = Instant::now();
    validate_send_options(options)?;

    debug!(
        "sender dump session bootstrapping: dump_id={} level={} threads={} frame_target_bytes={} min_readers={} max_readers={} read_buffer_bytes={}",
        dump_id,
        options.level,
        options.threads,
        options.frame_target_bytes,
        options.min_readers,
        options.max_readers,
        options.read_buffer_bytes
    );

    let source = source.into();
    let resume_started = Instant::now();
    let resume_state = protocol::request_resume_state(stream, dump_id)?;
    debug!(
        "sender resume state received: dump_id={} frame={} file_index={} archive_len={} elapsed_ms={}",
        dump_id,
        resume_state.last_committed_frame,
        resume_state.last_committed_file_index,
        resume_state.archive_len,
        resume_started.elapsed().as_millis()
    );

    let discover_started = Instant::now();
    let walker = PendingFileWalker::new(source.clone(), &resume_state)?;
    let mut pending = PendingFrameFiles::new(walker);
    debug!(
        "sender streaming discovery prepared: source={} resume_file_index={} elapsed_ms={}",
        source.display(),
        resume_state.last_committed_file_index,
        discover_started.elapsed().as_millis()
    );

    let mut sent_frames = 0_usize;
    let mut next_frame_no = resume_state.last_committed_frame;
    let mut aggregate = FrameBuildStats::default();
    let mut committed_archive_len = resume_state.archive_len;
    let mut readers = ReaderPool::new(
        options.max_readers,
        options.min_readers,
        options.max_readers,
        options.read_buffer_bytes,
        profile.cloned(),
    );
    loop {
        let candidate_frame_no = next_frame_no.saturating_add(1);
        let frame_started = Instant::now();
        let Some(frame) = send_streaming_frame(
            stream,
            &mut pending,
            candidate_frame_no,
            options,
            &mut readers,
            profile,
        )?
        else {
            break;
        };
        let frame_elapsed = frame_started.elapsed();
        if let Some(profile) = profile {
            profile.record_send(frame.compressed_len, frame_elapsed);
        }
        let (discovered_index, discovered_path) = pending.last_discovered_boundary();
        debug!(
            "sender frame acknowledged: frame_no={} archive_len={} ack_last_file={} discovered_boundary={}#{} frame_ms={} frame_bytes_per_sec={}",
            frame.ack.frame_no,
            frame.ack.archive_len,
            frame.ack.last_file_path,
            discovered_index,
            discovered_path,
            frame_elapsed.as_millis(),
            bytes_per_sec(frame.compressed_len, frame_elapsed)
        );
        merge_frame_stats(&mut aggregate, &frame.stats);
        committed_archive_len = frame.ack.archive_len;
        sent_frames += 1;
        next_frame_no = frame.outbound.frame_no;
    }
    readers.finish();
    let (final_file_index, final_file_path) = pending.final_boundary()?;
    let files_total_discovered = final_file_index;

    let finish_started = Instant::now();
    send_finish(stream, next_frame_no, final_file_index, &final_file_path)?;
    let final_archive_bytes = committed_archive_len.saturating_add(final_archive_footer_bytes()?);
    let status = if aggregate.files_sent_best_effort == 0
        && aggregate.files_skipped_open_failed == 0
        && aggregate.files_skipped_unstable_after_retries == 0
    {
        SendStatus::Clean
    } else {
        SendStatus::CompletedWithWarnings
    };
    debug!(
        "sender finish acknowledged: final_frame_no={} final_file_index={} final_file_path={} elapsed_ms={}",
        next_frame_no,
        final_file_index,
        final_file_path,
        finish_started.elapsed().as_millis()
    );
    debug!(
        "sender dump session completed: dump_id={} sent_frames={} pending_files={} files_sent_clean={} files_sent_best_effort={} files_skipped_open_failed={} files_skipped_unstable_after_retries={} bytes_sent_raw={} total_elapsed_ms={}",
        dump_id,
        sent_frames,
        files_total_discovered.saturating_sub(resume_state.last_committed_file_index),
        aggregate.files_sent_clean,
        aggregate.files_sent_best_effort,
        aggregate.files_skipped_open_failed,
        aggregate.files_skipped_unstable_after_retries,
        aggregate.bytes_sent_raw,
        session_started.elapsed().as_millis()
    );
    if let Some(profile) = profile {
        profile.record_total(aggregate.bytes_sent_raw, session_started.elapsed());
    }

    Ok(SendReport {
        dump_id: dump_id.to_owned(),
        resume_state,
        sent_frames,
        final_frame_no: next_frame_no,
        final_file_index,
        final_file_path,
        status,
        files_total_discovered,
        files_sent_clean: aggregate.files_sent_clean as u64,
        files_sent_best_effort: aggregate.files_sent_best_effort as u64,
        files_skipped_open_failed: aggregate.files_skipped_open_failed as u64,
        files_skipped_unstable_after_retries: aggregate.files_skipped_unstable_after_retries as u64,
        retries_total: aggregate.retries_total,
        bytes_sent_raw: aggregate.bytes_sent_raw,
        bytes_sent_best_effort_raw: aggregate.bytes_sent_best_effort_raw,
        final_archive_bytes,
    })
}

// Sender отправляет один целый frame и ждёт Ack ровно на него.
// Пока Ack не пришёл, выше по стеку нельзя считать frame committed.
pub fn send_frame<T, I, B>(stream: &mut T, frame: &OutboundFrame, chunks: I) -> Result<Ack>
where
    T: std::io::Read + std::io::Write,
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    if frame.threads == 0 {
        bail!("frame threads must be at least 1");
    }
    if frame.last_file_index < frame.first_file_index {
        bail!("frame last file index must not be smaller than first file index");
    }

    protocol::write_message(
        stream,
        &Message::FrameStart(FrameStart {
            frame_no: frame.frame_no,
            first_file_index: frame.first_file_index,
            first_file_path: frame.first_file_path.clone(),
            level: frame.level,
            threads: frame.threads,
        }),
    )?;

    let mut hasher = Xxh3Default::new();
    let mut compressed_len = 0_u64;
    for chunk in chunks {
        let bytes = chunk.as_ref();
        protocol::write_message(
            stream,
            &Message::FrameChunk(FrameChunk {
                bytes: bytes.to_vec(),
            }),
        )?;
        hasher.update(bytes);
        compressed_len = compressed_len
            .checked_add(bytes.len() as u64)
            .context("frame compressed length overflow")?;
    }

    protocol::write_message(
        stream,
        &Message::FrameEnd(FrameEnd {
            frame_no: frame.frame_no,
            last_file_index: frame.last_file_index,
            last_file_path: frame.last_file_path.clone(),
            compressed_len,
            compressed_xxh3_64: hasher.digest(),
        }),
    )?;
    stream.flush().context("failed to flush frame to peer")?;

    read_frame_ack(stream, frame)
}

fn read_frame_ack<T>(stream: &mut T, frame: &OutboundFrame) -> Result<Ack>
where
    T: std::io::Read + std::io::Write,
{
    let ack = match protocol::read_message(stream)? {
        Message::Ack(ack) => ack,
        Message::Error(error) => bail!("peer rejected frame {}: {}", frame.frame_no, error.message),
        unexpected => bail!(
            "unexpected message while waiting for frame ack: {:?}",
            unexpected.kind()
        ),
    };

    if ack.frame_no != frame.frame_no {
        bail!(
            "ack frame number mismatch: expected {} got {}",
            frame.frame_no,
            ack.frame_no
        );
    }
    if ack.last_file_index != frame.last_file_index {
        bail!(
            "ack last file index mismatch: expected {} got {}",
            frame.last_file_index,
            ack.last_file_index
        );
    }
    if ack.last_file_path != frame.last_file_path {
        bail!(
            "ack last file path mismatch: expected {} got {}",
            frame.last_file_path,
            ack.last_file_path
        );
    }

    Ok(ack)
}

// После отправки последнего committed frame sender закрывает dump отдельным
// Finish-сообщением и ждёт FinishAck. Это отделяет frame commit от завершения
// всей сессии и сохраняет resume-модель по границе кадра.
pub fn send_finish<T>(
    stream: &mut T,
    final_frame_no: u64,
    final_file_index: u64,
    final_file_path: &str,
) -> Result<Finish>
where
    T: std::io::Read + std::io::Write,
{
    let finish = Finish {
        final_frame_no,
        final_file_index,
        final_file_path: final_file_path.to_owned(),
    };
    protocol::write_message(stream, &Message::Finish(finish.clone()))?;
    stream
        .flush()
        .context("failed to flush finish message to peer")?;

    match protocol::read_message(stream)? {
        Message::FinishAck => Ok(finish),
        Message::Error(error) => bail!("peer rejected finish: {}", error.message),
        unexpected => bail!(
            "unexpected message while waiting for finish ack: {:?}",
            unexpected.kind()
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{self, Cursor, Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{OutboundFrame, SendOptions, SendStatus, send_dump, send_finish, send_frame};
    use crate::network::protocol::{self, Ack, ErrorMessage, Message, ResumeState};
    use xxhash_rust::xxh3::Xxh3Default;

    struct ScriptedStream {
        read: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    struct ObservableStream {
        read: Cursor<Vec<u8>>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    struct TestDir {
        path: PathBuf,
    }

    struct ReadHookGuard {
        _lock: MutexGuard<'static, ()>,
    }

    impl TestDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "fast-dump-sender-session-tests-{}-{}",
                std::process::id(),
                unique
            ));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    impl ReadHookGuard {
        fn new() -> Self {
            let lock = crate::sender::readers::test_hooks::lock();
            crate::sender::readers::test_hooks::clear();
            Self { _lock: lock }
        }
    }

    impl Drop for ReadHookGuard {
        fn drop(&mut self) {
            crate::sender::readers::test_hooks::clear();
        }
    }

    impl ScriptedStream {
        fn new(read_bytes: Vec<u8>) -> Self {
            Self {
                read: Cursor::new(read_bytes),
                written: Vec::new(),
            }
        }
    }

    impl Read for ScriptedStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read.read(buf)
        }
    }

    impl Write for ScriptedStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl ObservableStream {
        fn new(read_bytes: Vec<u8>, written: Arc<Mutex<Vec<u8>>>) -> Self {
            Self {
                read: Cursor::new(read_bytes),
                written,
            }
        }
    }

    impl Read for ObservableStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read.read(buf)
        }
    }

    impl Write for ObservableStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.written.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn encode_message(message: &Message) -> Vec<u8> {
        let mut bytes = Vec::new();
        protocol::write_message(&mut bytes, message).unwrap();
        bytes
    }

    fn decode_messages(bytes: &[u8]) -> Vec<Message> {
        let mut cursor = Cursor::new(bytes.to_vec());
        let mut messages = Vec::new();
        while (cursor.position() as usize) < bytes.len() {
            messages.push(protocol::read_message(&mut cursor).unwrap());
        }
        messages
    }

    fn sample_frame() -> OutboundFrame {
        OutboundFrame {
            frame_no: 4,
            first_file_index: 11,
            first_file_path: "root/a.bin".to_owned(),
            last_file_index: 12,
            last_file_path: "root/b.bin".to_owned(),
            level: 3,
            threads: 2,
        }
    }

    fn empty_pending_frame_files(temp: &TestDir) -> crate::sender::PendingFrameFiles {
        crate::sender::PendingFrameFiles::new(
            crate::sender::PendingFileWalker::new(
                temp.path().to_path_buf(),
                &ResumeState {
                    last_committed_frame: 0,
                    last_committed_file_index: 0,
                    last_committed_file_path: String::new(),
                    archive_len: 0,
                },
            )
            .unwrap(),
        )
    }

    fn write_deterministic_file(path: &Path, size: usize, seed: u64) {
        let mut state = seed.max(1);
        let mut bytes = vec![0_u8; size];
        for byte in &mut bytes {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (state >> 32) as u8;
        }
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn send_frame_writes_start_chunks_end_and_returns_ack() {
        let frame = sample_frame();
        let expected_ack = Ack {
            frame_no: frame.frame_no,
            last_file_index: frame.last_file_index,
            last_file_path: frame.last_file_path.clone(),
            archive_len: 1234,
        };
        let mut stream = ScriptedStream::new(encode_message(&Message::Ack(expected_ack.clone())));

        let ack = send_frame(&mut stream, &frame, [b"abc".as_slice(), b"def".as_slice()]).unwrap();

        assert_eq!(ack, expected_ack);
        let messages = decode_messages(&stream.written);
        assert!(matches!(messages[0], Message::FrameStart(_)));
        assert_eq!(messages.len(), 4);
        assert!(matches!(messages[1], Message::FrameChunk(_)));
        assert!(matches!(messages[2], Message::FrameChunk(_)));
        assert!(matches!(messages[3], Message::FrameEnd(_)));
    }

    #[test]
    fn send_frame_surfaces_peer_error() {
        let frame = sample_frame();
        let mut stream = ScriptedStream::new(encode_message(&Message::Error(ErrorMessage {
            message: "checksum mismatch".to_owned(),
        })));

        let error = send_frame(&mut stream, &frame, [b"abc".as_slice()]).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected frame 4: checksum mismatch")
        );
    }

    #[test]
    fn send_frame_rejects_ack_mismatch() {
        let frame = sample_frame();
        let mut stream = ScriptedStream::new(encode_message(&Message::Ack(Ack {
            frame_no: 99,
            last_file_index: frame.last_file_index,
            last_file_path: frame.last_file_path.clone(),
            archive_len: 1234,
        })));

        let error = send_frame(&mut stream, &frame, [b"abc".as_slice()]).unwrap_err();

        assert!(error.to_string().contains("ack frame number mismatch"));
    }

    #[test]
    fn protocol_chunk_writer_flushes_incremental_chunks_before_frame_end() {
        let frame = sample_frame();
        let expected_ack = Ack {
            frame_no: frame.frame_no,
            last_file_index: frame.last_file_index,
            last_file_path: frame.last_file_path.clone(),
            archive_len: 1234,
        };
        let written = Arc::new(Mutex::new(Vec::new()));
        let mut stream = ObservableStream::new(
            encode_message(&Message::Ack(expected_ack.clone())),
            written.clone(),
        );
        let mut writer = super::ProtocolChunkWriter::new(&mut stream, 3);

        writer.write_all(b"abcdefg").unwrap();

        let mid_messages = decode_messages(&written.lock().unwrap());
        assert_eq!(mid_messages.len(), 2);
        assert!(matches!(mid_messages[0], Message::FrameChunk(_)));
        assert!(matches!(mid_messages[1], Message::FrameChunk(_)));

        let (ack, compressed_len) = writer.finish_frame(&frame).unwrap();

        assert_eq!(ack, expected_ack);
        assert_eq!(compressed_len, 7);
        let messages = decode_messages(&written.lock().unwrap());
        assert_eq!(messages.len(), 4);
        let frame_end = match &messages[3] {
            Message::FrameEnd(frame_end) => frame_end,
            other => panic!("unexpected message: {other:?}"),
        };
        let mut hasher = Xxh3Default::new();
        hasher.update(b"abcdefg");
        assert_eq!(frame_end.compressed_len, 7);
        assert_eq!(frame_end.compressed_xxh3_64, hasher.digest());
    }

    #[test]
    fn send_finish_writes_finish_and_accepts_finish_ack() {
        let mut stream = ScriptedStream::new(encode_message(&Message::FinishAck));

        let finish = send_finish(&mut stream, 4, 12, "root/b.bin").unwrap();

        assert_eq!(finish.final_frame_no, 4);
        assert_eq!(finish.final_file_index, 12);
        assert_eq!(finish.final_file_path, "root/b.bin");
        assert_eq!(
            decode_messages(&stream.written),
            vec![Message::Finish(finish)]
        );
    }

    #[test]
    fn send_finish_surfaces_peer_error() {
        let mut stream = ScriptedStream::new(encode_message(&Message::Error(ErrorMessage {
            message: "receiver state mismatch".to_owned(),
        })));

        let error = send_finish(&mut stream, 4, 12, "root/b.bin").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("peer rejected finish: receiver state mismatch")
        );
    }

    #[test]
    fn send_finish_rejects_unexpected_reply() {
        let mut stream = ScriptedStream::new(encode_message(&Message::Ack(Ack {
            frame_no: 4,
            last_file_index: 12,
            last_file_path: "root/b.bin".to_owned(),
            archive_len: 1234,
        })));

        let error = send_finish(&mut stream, 4, 12, "root/b.bin").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpected message while waiting for finish ack")
        );
    }

    #[test]
    fn next_streaming_file_returns_controlled_error_when_queue_is_empty() {
        let temp = TestDir::new();
        let pending = empty_pending_frame_files(&temp);

        let error = super::next_streaming_file(&pending).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender streaming frame lost its scheduled file before processing")
        );
    }

    #[test]
    fn consume_streaming_file_returns_controlled_error_when_queue_is_empty() {
        let temp = TestDir::new();
        let mut pending = empty_pending_frame_files(&temp);

        let error = super::consume_streaming_file(&mut pending).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("sender streaming frame lost its scheduled file after processing")
        );
    }

    #[test]
    fn send_dump_requests_resume_state_and_transfers_all_frames() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("alpha.txt"), b"alpha").unwrap();
        fs::write(temp.path().join("nested").join("beta.txt"), b"beta").unwrap();

        let mut responses = Vec::new();
        protocol::write_message(
            &mut responses,
            &Message::ResumeState(ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 1,
                last_file_index: 1,
                last_file_path: "alpha.txt".to_owned(),
                archive_len: 128,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 2,
                last_file_index: 2,
                last_file_path: "nested/beta.txt".to_owned(),
                archive_len: 256,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 1,
            min_readers: 2,
            max_readers: 4,
            read_buffer_bytes: 1024,
        };

        let report =
            send_dump(&mut stream, temp.path().to_path_buf(), "dump-01", &options).unwrap();

        assert_eq!(report.dump_id, "dump-01");
        assert_eq!(report.resume_state.archive_len, 0);
        assert_eq!(report.sent_frames, 2);
        assert_eq!(report.final_frame_no, 2);
        assert_eq!(report.final_file_index, 2);
        assert_eq!(report.final_file_path, "nested/beta.txt");
        assert_eq!(report.status, SendStatus::Clean);
        assert_eq!(report.files_total_discovered, 2);
        assert_eq!(report.files_sent_clean, 2);
        assert_eq!(report.files_sent_best_effort, 0);
        assert_eq!(report.files_skipped_open_failed, 0);
        assert_eq!(report.files_skipped_unstable_after_retries, 0);
        assert_eq!(report.retries_total, 0);
        assert_eq!(report.bytes_sent_raw, 9);
        assert_eq!(report.bytes_sent_best_effort_raw, 0);
        assert_eq!(
            report.final_archive_bytes,
            256 + super::final_archive_footer_bytes().unwrap()
        );

        let messages = decode_messages(&stream.written);
        assert!(matches!(messages[0], Message::ResumeQuery(_)));
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameStart(_)))
                .count(),
            2
        );
        assert!(matches!(messages.last(), Some(Message::Finish(_))));
    }

    #[test]
    fn send_dump_uses_runtime_settings_provided_by_caller() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        fs::write(temp.path().join("alpha.txt"), b"alpha").unwrap();
        fs::write(temp.path().join("nested").join("beta.txt"), b"beta").unwrap();

        let mut responses = Vec::new();
        protocol::write_message(
            &mut responses,
            &Message::ResumeState(ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 1,
                last_file_index: 1,
                last_file_path: "alpha.txt".to_owned(),
                archive_len: 1,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 2,
                last_file_index: 2,
                last_file_path: "nested/beta.txt".to_owned(),
                archive_len: 2,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 7,
            threads: 3,
            frame_target_bytes: 1,
            min_readers: 2,
            max_readers: 4,
            read_buffer_bytes: 2,
        };

        let report =
            send_dump(&mut stream, temp.path().to_path_buf(), "dump-02", &options).unwrap();

        assert_eq!(report.sent_frames, 2);
        let frame_starts: Vec<_> = decode_messages(&stream.written)
            .into_iter()
            .filter_map(|message| match message {
                Message::FrameStart(frame_start) => Some(frame_start),
                _ => None,
            })
            .collect();
        assert_eq!(frame_starts.len(), 2);
        assert_eq!(frame_starts[0].level, 7);
        assert_eq!(frame_starts[0].threads, 3);
        assert_eq!(frame_starts[1].level, 7);
        assert_eq!(frame_starts[1].threads, 3);
    }

    #[test]
    fn send_dump_reuses_prefetched_reader_task_across_frame_boundary() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("nested")).unwrap();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("nested").join("beta.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        fs::write(&beta_path, b"beta").unwrap();

        let alpha_before =
            crate::sender::readers::test_hooks::scheduled_reader_task_count(&alpha_path);
        let beta_before =
            crate::sender::readers::test_hooks::scheduled_reader_task_count(&beta_path);

        let mut responses = Vec::new();
        protocol::write_message(
            &mut responses,
            &Message::ResumeState(ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 1,
                last_file_index: 1,
                last_file_path: "alpha.txt".to_owned(),
                archive_len: 1,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 2,
                last_file_index: 2,
                last_file_path: "nested/beta.txt".to_owned(),
                archive_len: 2,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 1,
            min_readers: 2,
            max_readers: 2,
            read_buffer_bytes: 2,
        };

        let report =
            send_dump(&mut stream, temp.path().to_path_buf(), "dump-03", &options).unwrap();

        assert_eq!(report.sent_frames, 2);
        assert_eq!(
            crate::sender::readers::test_hooks::scheduled_reader_task_count(&alpha_path)
                - alpha_before,
            1
        );
        assert_eq!(
            crate::sender::readers::test_hooks::scheduled_reader_task_count(&beta_path)
                - beta_before,
            1
        );
    }

    #[test]
    fn send_dump_streams_large_frame_as_multiple_frame_chunks() {
        let temp = TestDir::new();
        let payload_path = temp.path().join("payload.bin");
        write_deterministic_file(&payload_path, protocol::MAX_FRAME_CHUNK_BYTES * 2 + 17, 7);

        let mut responses = Vec::new();
        protocol::write_message(
            &mut responses,
            &Message::ResumeState(ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 1,
                last_file_index: 1,
                last_file_path: "payload.bin".to_owned(),
                archive_len: 777,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: u64::MAX,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 64 * 1024,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-large-frame",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 1);
        let messages = decode_messages(&stream.written);
        let frame_start_pos = messages
            .iter()
            .position(|message| matches!(message, Message::FrameStart(_)))
            .unwrap();
        let first_chunk_pos = messages
            .iter()
            .position(|message| matches!(message, Message::FrameChunk(_)))
            .unwrap();
        let frame_end_pos = messages
            .iter()
            .position(|message| matches!(message, Message::FrameEnd(_)))
            .unwrap();
        assert!(frame_start_pos < first_chunk_pos);
        assert!(first_chunk_pos < frame_end_pos);
        assert!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameChunk(_)))
                .count()
                >= 2
        );
    }

    #[test]
    fn send_dump_reports_aggregate_counters_for_best_effort_and_skip() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        let gamma_path = temp.path().join("gamma.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        fs::write(&beta_path, b"beta").unwrap();
        fs::write(&gamma_path, b"gamma").unwrap();

        crate::sender::readers::test_hooks::force_short_read_after_next_open(&beta_path, 2);
        for _ in 0..3 {
            crate::sender::readers::test_hooks::fail_next_open(&gamma_path);
        }

        let mut responses = Vec::new();
        protocol::write_message(
            &mut responses,
            &Message::ResumeState(ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 1,
                last_file_index: 3,
                last_file_path: "gamma.txt".to_owned(),
                archive_len: 321,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 1024 * 1024,
            min_readers: 1,
            max_readers: 2,
            read_buffer_bytes: 4,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-warnings",
            &options,
        )
        .unwrap();

        assert_eq!(report.status, SendStatus::CompletedWithWarnings);
        assert_eq!(report.files_total_discovered, 3);
        assert_eq!(report.files_sent_clean, 1);
        assert_eq!(report.files_sent_best_effort, 1);
        assert_eq!(report.files_skipped_open_failed, 1);
        assert_eq!(report.files_skipped_unstable_after_retries, 0);
        assert_eq!(report.retries_total, 3);
        assert_eq!(report.bytes_sent_raw, 9);
        assert_eq!(report.bytes_sent_best_effort_raw, 4);
        assert_eq!(report.sent_frames, 1);
        assert_eq!(report.final_frame_no, 1);
        assert_eq!(report.final_file_index, 3);
        assert_eq!(report.final_file_path, "gamma.txt");
        assert_eq!(
            report.final_archive_bytes,
            321 + super::final_archive_footer_bytes().unwrap()
        );

        let messages = decode_messages(&stream.written);
        let frame_start = match &messages[1] {
            Message::FrameStart(frame_start) => frame_start,
            other => panic!("unexpected message: {other:?}"),
        };
        let frame_end = match messages.iter().find_map(|message| match message {
            Message::FrameEnd(frame_end) => Some(frame_end),
            _ => None,
        }) {
            Some(frame_end) => frame_end,
            None => panic!("missing frame end"),
        };
        assert_eq!(frame_start.first_file_index, 1);
        assert_eq!(frame_end.last_file_index, 3);
        assert_eq!(frame_end.last_file_path, "gamma.txt");
    }

    #[test]
    fn send_dump_does_not_retry_skipped_file_after_resume() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        fs::write(&beta_path, b"beta").unwrap();
        for _ in 0..3 {
            crate::sender::readers::test_hooks::fail_next_open(&alpha_path);
        }

        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 1024 * 1024,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 4,
        };

        let mut first_responses = Vec::new();
        protocol::write_message(
            &mut first_responses,
            &Message::ResumeState(ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut first_responses,
            &Message::Ack(Ack {
                frame_no: 1,
                last_file_index: 2,
                last_file_path: "beta.txt".to_owned(),
                archive_len: 50,
            }),
        )
        .unwrap();
        protocol::write_message(&mut first_responses, &Message::FinishAck).unwrap();

        let mut first_stream = ScriptedStream::new(first_responses);
        let first_report = send_dump(
            &mut first_stream,
            temp.path().to_path_buf(),
            "dump-resume-skip",
            &options,
        )
        .unwrap();
        assert_eq!(first_report.files_skipped_open_failed, 1);
        assert_eq!(first_report.files_sent_clean, 1);
        assert_eq!(first_report.sent_frames, 1);
        assert_eq!(first_report.final_frame_no, 1);
        assert_eq!(first_report.final_file_index, 2);

        let first_messages = decode_messages(&first_stream.written);
        assert_eq!(
            first_messages
                .iter()
                .filter(|message| matches!(message, Message::FrameStart(_)))
                .count(),
            1
        );

        let mut second_responses = Vec::new();
        protocol::write_message(
            &mut second_responses,
            &Message::ResumeState(ResumeState {
                last_committed_frame: 1,
                last_committed_file_index: 2,
                last_committed_file_path: "beta.txt".to_owned(),
                archive_len: 50,
            }),
        )
        .unwrap();
        protocol::write_message(&mut second_responses, &Message::FinishAck).unwrap();

        let mut second_stream = ScriptedStream::new(second_responses);
        let second_report = send_dump(
            &mut second_stream,
            temp.path().to_path_buf(),
            "dump-resume-skip",
            &options,
        )
        .unwrap();

        assert_eq!(second_report.sent_frames, 0);
        assert_eq!(second_report.final_frame_no, 1);
        assert_eq!(second_report.final_file_index, 2);
        assert_eq!(second_report.final_file_path, "beta.txt");
        assert_eq!(second_report.status, SendStatus::Clean);
        assert_eq!(second_report.files_total_discovered, 2);
        assert_eq!(second_report.files_sent_clean, 0);
        assert_eq!(second_report.files_skipped_open_failed, 0);

        let second_messages = decode_messages(&second_stream.written);
        assert!(matches!(second_messages[0], Message::ResumeQuery(_)));
        assert!(matches!(second_messages[1], Message::Finish(_)));
        assert_eq!(second_messages.len(), 2);
    }

    #[test]
    fn send_dump_rejects_invalid_runtime_settings_before_network_io() {
        let cases = [
            (
                SendOptions {
                    level: 3,
                    threads: 0,
                    frame_target_bytes: 1,
                    min_readers: 1,
                    max_readers: 1,
                    read_buffer_bytes: 1,
                },
                "threads must be at least 1",
            ),
            (
                SendOptions {
                    level: 3,
                    threads: 1,
                    frame_target_bytes: 0,
                    min_readers: 1,
                    max_readers: 1,
                    read_buffer_bytes: 1,
                },
                "frame_target_bytes must be at least 1",
            ),
            (
                SendOptions {
                    level: 3,
                    threads: 1,
                    frame_target_bytes: 1,
                    min_readers: 0,
                    max_readers: 1,
                    read_buffer_bytes: 1,
                },
                "min_readers must be at least 1",
            ),
            (
                SendOptions {
                    level: 3,
                    threads: 1,
                    frame_target_bytes: 1,
                    min_readers: 1,
                    max_readers: 0,
                    read_buffer_bytes: 1,
                },
                "max_readers must be at least 1",
            ),
            (
                SendOptions {
                    level: 3,
                    threads: 1,
                    frame_target_bytes: 1,
                    min_readers: 2,
                    max_readers: 1,
                    read_buffer_bytes: 1,
                },
                "min_readers must not be greater than max_readers",
            ),
            (
                SendOptions {
                    level: 3,
                    threads: 1,
                    frame_target_bytes: 1,
                    min_readers: 1,
                    max_readers: 1,
                    read_buffer_bytes: 0,
                },
                "read_buffer_bytes must be at least 1",
            ),
        ];

        for (options, expected_message) in cases {
            let mut stream = ScriptedStream::new(Vec::new());
            let error = send_dump(&mut stream, PathBuf::from("unused"), "dump-guard", &options)
                .unwrap_err();
            assert!(
                error.to_string().contains(expected_message),
                "{expected_message}: {error}"
            );
            assert!(stream.written.is_empty());
        }
    }
}
