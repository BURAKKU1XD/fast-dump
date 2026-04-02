use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use log::debug;
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::{
    self, AbortAck, Ack, Finish, FrameChunk, FrameEnd, FrameStart, IssueJournalChunk,
    IssueJournalEntry, IssueJournalReason, Message, ResumeState,
};

use super::frame::{
    FramePolicy, FrameProgress, RetryableStreamFailure, stream_direct_file, stream_staged_snapshot,
};
use super::readers::{FileDisposition, FileSkipReason, MAX_SNAPSHOT_ATTEMPTS};
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
pub enum SendIssueReason {
    OpenFailed,
    UnstableAfterRetries,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendIssue {
    pub file_index: u64,
    pub file_path: String,
    pub attempts: u32,
    pub reason: SendIssueReason,
    pub last_error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendReport {
    pub dump_id: String,
    pub resume_state: ResumeState,
    pub sent_frames: usize,
    pub frame_aborts: u64,
    pub frame_rebuilds: u64,
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
    pub issue_journal: Vec<SendIssue>,
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

fn protocol_issue_reason(reason: &SendIssueReason) -> IssueJournalReason {
    match reason {
        SendIssueReason::OpenFailed => IssueJournalReason::OpenFailed,
        SendIssueReason::UnstableAfterRetries => IssueJournalReason::UnstableAfterRetries,
    }
}

fn protocol_issue_entry(issue: &SendIssue) -> IssueJournalEntry {
    IssueJournalEntry {
        file_index: issue.file_index,
        file_path: issue.file_path.clone(),
        attempts: issue.attempts,
        reason: protocol_issue_reason(&issue.reason),
        last_error: issue.last_error.clone(),
    }
}

fn send_issue_journal<T>(stream: &mut T, issues: &[SendIssue]) -> Result<usize>
where
    T: Write,
{
    if issues.is_empty() {
        return Ok(0);
    }

    let mut chunks_sent = 0_usize;
    let mut pending = Vec::new();

    for issue in issues {
        let entry = protocol_issue_entry(issue);
        let mut candidate = pending.clone();
        candidate.push(entry.clone());
        let candidate_message =
            Message::IssueJournalChunk(IssueJournalChunk { entries: candidate });
        if protocol::write_message(&mut Vec::new(), &candidate_message).is_ok() {
            pending.push(entry);
            continue;
        }

        if pending.is_empty() {
            protocol::write_message(stream, &candidate_message)
                .context("issue journal entry does not fit into a single protocol chunk")?;
            return Ok(1);
        }

        protocol::write_message(
            stream,
            &Message::IssueJournalChunk(IssueJournalChunk {
                entries: std::mem::take(&mut pending),
            }),
        )?;
        chunks_sent += 1;
        pending.push(entry);
    }

    if !pending.is_empty() {
        protocol::write_message(
            stream,
            &Message::IssueJournalChunk(IssueJournalChunk { entries: pending }),
        )?;
        chunks_sent += 1;
    }

    Ok(chunks_sent)
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
    issues: Vec<SendIssue>,
    ack: Ack,
    compressed_len: u64,
}

enum StreamingFrameAttempt {
    Empty,
    Committed(StreamingFrameResult),
    Aborted {
        failed_file: InputFile,
        last_error: String,
    },
}

#[derive(Default)]
struct StreamRetryJournal {
    attempts: BTreeMap<u64, u32>,
    last_errors: BTreeMap<u64, String>,
}

impl StreamRetryJournal {
    fn attempts_for(&self, file_index: u64) -> u32 {
        self.attempts.get(&file_index).copied().unwrap_or(0)
    }

    fn note_abort(&mut self, file_index: u64, last_error: String) -> u32 {
        let attempts = self.attempts.entry(file_index).or_default();
        *attempts = attempts.saturating_add(1);
        self.last_errors.insert(file_index, last_error);
        *attempts
    }

    fn take_attempts(&mut self, file_index: u64) -> u32 {
        self.attempts.remove(&file_index).unwrap_or(0)
    }

    fn take_last_error(&mut self, file_index: u64) -> Option<String> {
        self.last_errors.remove(&file_index)
    }

    fn clear_file(&mut self, file_index: u64) {
        self.attempts.remove(&file_index);
        self.last_errors.remove(&file_index);
    }
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

    fn finish_frame(mut self, frame: &OutboundFrame) -> Result<(Ack, u64, Duration)> {
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
        let waited_at = Instant::now();
        let ack = read_frame_ack(self.stream, frame)?;
        Ok((ack, self.compressed_len, waited_at.elapsed()))
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

fn new_reader_pool(
    options: &SendOptions,
    profile: Option<&PipelineProfileCollector>,
) -> ReaderPool {
    ReaderPool::new(
        options.max_readers,
        options.min_readers,
        options.max_readers,
        options.read_buffer_bytes,
        profile.cloned(),
    )
}

fn prefetch_streaming_files(
    pending: &mut PendingFrameFiles,
    readers: &mut ReaderPool,
    retry_journal: &StreamRetryJournal,
) -> Result<(usize, bool)> {
    pending.prefetch_with(readers, |pool, file| {
        if !file.is_regular() {
            return Ok(());
        }
        let attempts = retry_journal.attempts_for(file.index);
        if attempts >= MAX_SNAPSHOT_ATTEMPTS {
            return Ok(());
        }
        pool.schedule_with_attempt_limit(
            file,
            MAX_SNAPSHOT_ATTEMPTS.saturating_sub(attempts).max(1),
        )
    })
}

fn adjusted_snapshot_outcome(
    disposition: FileDisposition,
    retries: u32,
    stream_retries: u32,
) -> (FileDisposition, u32) {
    (
        if stream_retries > 0 {
            FileDisposition::BestEffort
        } else {
            disposition
        },
        retries.saturating_add(stream_retries),
    )
}

fn adjusted_skip_outcome(
    reason: FileSkipReason,
    retries: u32,
    stream_retries: u32,
) -> (FileSkipReason, u32) {
    (
        if stream_retries > 0 {
            FileSkipReason::UnstableAfterRetries
        } else {
            reason
        },
        retries.saturating_add(stream_retries),
    )
}

fn issue_reason_from_skip_reason(reason: FileSkipReason) -> SendIssueReason {
    match reason {
        FileSkipReason::OpenFailed => SendIssueReason::OpenFailed,
        FileSkipReason::UnstableAfterRetries => SendIssueReason::UnstableAfterRetries,
    }
}

fn send_streaming_frame<T>(
    stream: &mut T,
    pending: &mut PendingFrameFiles,
    frame_no: u64,
    options: &SendOptions,
    readers: &mut ReaderPool,
    profile: Option<&PipelineProfileCollector>,
    retry_journal: &mut StreamRetryJournal,
) -> Result<StreamingFrameAttempt>
where
    T: std::io::Read + std::io::Write,
{
    let _ = prefetch_streaming_files(pending, readers, retry_journal)?;
    let Some(first) = pending.next_expected().cloned() else {
        return Ok(StreamingFrameAttempt::Empty);
    };

    let build_started = Instant::now();
    let policy = FramePolicy::from_frame_target_bytes(options.frame_target_bytes);
    let mut stats = FrameBuildStats::default();
    let mut progress = FrameProgress::default();
    let mut issues = Vec::new();
    let mut last_completed: Option<InputFile> = None;
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
        let stream_retries = retry_journal.attempts_for(file.index);
        if let Some(last) = &last_completed
            && policy.should_close_before_next(progress, &file, stream_retries)
        {
            break last.clone();
        }
        let mut completed_payload = None;
        if stream_retries >= MAX_SNAPSHOT_ATTEMPTS {
            stats.note_skip(FileSkipReason::UnstableAfterRetries, stream_retries);
            issues.push(SendIssue {
                file_index: file.index,
                file_path: file.rel_path.clone(),
                attempts: stream_retries,
                reason: SendIssueReason::UnstableAfterRetries,
                last_error: retry_journal
                    .take_last_error(file.index)
                    .unwrap_or_else(|| {
                        "retryable sender stream failure exhausted per-file attempt budget"
                            .to_owned()
                    }),
            });
            retry_journal.take_attempts(file.index);
        } else if file.is_regular() {
            match readers.next_for_with_scheduler(file.index, |pool| {
                prefetch_streaming_files(pending, pool, retry_journal)
            })? {
                super::ReadEvent::Ready(super::FileReadOutcome::Snapshot(snapshot)) => {
                    let header_started = Instant::now();
                    super::frame::append_tar_file_header(
                        &mut encoder,
                        &snapshot.file,
                        snapshot.size_at_open,
                    )?;
                    if let Some(profile) = profile {
                        profile.record_compress(
                            super::frame::tar_header_bytes_for_file(&snapshot.file),
                            header_started.elapsed(),
                        );
                    }
                    if let Err(error) = stream_staged_snapshot(&mut encoder, &snapshot, profile) {
                        if error.downcast_ref::<RetryableStreamFailure>().is_some() {
                            return Ok(StreamingFrameAttempt::Aborted {
                                failed_file: snapshot.file.clone(),
                                last_error: error.to_string(),
                            });
                        }
                        return Err(error);
                    }
                    let padding_bytes =
                        super::frame::tar_padding_bytes(snapshot.size_at_open) as u64;
                    let padding_started = Instant::now();
                    super::frame::append_tar_file_padding(
                        &mut encoder,
                        &snapshot.file,
                        snapshot.size_at_open,
                    )?;
                    if let Some(profile) = profile {
                        profile.record_compress(padding_bytes, padding_started.elapsed());
                    }
                    let extra_stream_retries = retry_journal.take_attempts(snapshot.file.index);
                    retry_journal.clear_file(snapshot.file.index);
                    let (disposition, retries) = adjusted_snapshot_outcome(
                        snapshot.disposition,
                        snapshot.retries,
                        extra_stream_retries,
                    );
                    stats.note_snapshot(
                        &snapshot.file,
                        disposition,
                        snapshot.size_at_open,
                        retries,
                    );
                    completed_payload = Some((snapshot.size_at_open, retries));
                }
                super::ReadEvent::Ready(super::FileReadOutcome::Direct(mut direct)) => {
                    let header_started = Instant::now();
                    super::frame::append_tar_file_header(
                        &mut encoder,
                        &direct.file,
                        direct.size_at_open,
                    )?;
                    if let Some(profile) = profile {
                        profile.record_compress(
                            super::frame::tar_header_bytes_for_file(&direct.file),
                            header_started.elapsed(),
                        );
                    }
                    if let Err(error) = stream_direct_file(&mut encoder, &mut direct, profile) {
                        if error.downcast_ref::<RetryableStreamFailure>().is_some() {
                            return Ok(StreamingFrameAttempt::Aborted {
                                failed_file: direct.file.clone(),
                                last_error: error.to_string(),
                            });
                        }
                        return Err(error);
                    }
                    let padding_bytes = super::frame::tar_padding_bytes(direct.size_at_open) as u64;
                    let padding_started = Instant::now();
                    super::frame::append_tar_file_padding(
                        &mut encoder,
                        &direct.file,
                        direct.size_at_open,
                    )?;
                    if let Some(profile) = profile {
                        profile.record_compress(padding_bytes, padding_started.elapsed());
                    }
                    let extra_stream_retries = retry_journal.take_attempts(direct.file.index);
                    retry_journal.clear_file(direct.file.index);
                    let (disposition, retries) = adjusted_snapshot_outcome(
                        direct.disposition,
                        direct.retries,
                        extra_stream_retries,
                    );
                    stats.note_snapshot(&direct.file, disposition, direct.size_at_open, retries);
                    completed_payload = Some((direct.size_at_open, retries));
                }
                super::ReadEvent::Ready(super::FileReadOutcome::Skip(skipped)) => {
                    let extra_stream_retries = retry_journal.take_attempts(skipped.file.index);
                    let last_error = if extra_stream_retries > 0 {
                        retry_journal
                            .take_last_error(skipped.file.index)
                            .unwrap_or(skipped.last_error)
                    } else {
                        skipped.last_error
                    };
                    let (reason, retries) = adjusted_skip_outcome(
                        skipped.reason,
                        skipped.retries,
                        extra_stream_retries,
                    );
                    stats.note_skip(reason, retries);
                    issues.push(SendIssue {
                        file_index: skipped.file.index,
                        file_path: skipped.file.rel_path.clone(),
                        attempts: retries,
                        reason: issue_reason_from_skip_reason(reason),
                        last_error,
                    });
                }
                super::ReadEvent::Fatal { message, .. } => anyhow::bail!("{message}"),
            }
        } else {
            let header_started = Instant::now();
            super::frame::append_tar_file_header(&mut encoder, &file, 0)?;
            if let Some(profile) = profile {
                profile.record_compress(
                    super::frame::tar_header_bytes_for_file(&file),
                    header_started.elapsed(),
                );
            }
            stats.note_snapshot(&file, FileDisposition::Clean, 0, 0);
            completed_payload = Some((0, 0));
        }
        let last = consume_streaming_file(pending)?;
        last_completed = Some(last.clone());
        let _ = prefetch_streaming_files(pending, readers, retry_journal)?;
        if let Some((size_at_open, retries)) = completed_payload {
            progress.note_payload_file(&last, size_at_open);
            if policy.should_close_after_current(
                progress,
                size_at_open,
                retries,
                pending.next_expected().is_some(),
            ) {
                break last;
            }
        } else if pending.next_expected().is_none() {
            break last;
        }
    };

    let finish_started = Instant::now();
    let writer = encoder
        .finish()
        .context("failed to finish sender zstd frame")?;
    if let Some(profile) = profile {
        profile.record_flush();
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
    let (ack, compressed_len, ack_wait) = writer.finish_frame(&outbound)?;
    if let Some(profile) = profile {
        profile.record_ack_wait(ack_wait);
    }
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
    Ok(StreamingFrameAttempt::Committed(StreamingFrameResult {
        outbound,
        stats,
        issues,
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
    let walker = PendingFileWalker::new_with_profile(source.clone(), &resume_state, profile)?;
    let mut pending = PendingFrameFiles::new(walker);
    debug!(
        "sender streaming discovery prepared: source={} resume_file_index={} elapsed_ms={}",
        source.display(),
        resume_state.last_committed_file_index,
        discover_started.elapsed().as_millis()
    );

    let mut sent_frames = 0_usize;
    let mut frame_aborts = 0_u64;
    let mut frame_rebuilds = 0_u64;
    let mut next_frame_no = resume_state.last_committed_frame;
    let mut aggregate = FrameBuildStats::default();
    let mut committed_archive_len = resume_state.archive_len;
    let mut issue_journal = Vec::new();
    let mut retry_journal = StreamRetryJournal::default();
    let mut readers = new_reader_pool(options, profile);
    loop {
        let candidate_frame_no = next_frame_no.saturating_add(1);
        let frame_started = Instant::now();
        let frame_pending = pending.clone();
        let frame = send_streaming_frame(
            stream,
            &mut pending,
            candidate_frame_no,
            options,
            &mut readers,
            profile,
            &mut retry_journal,
        )?;
        let frame = match frame {
            StreamingFrameAttempt::Empty => break,
            StreamingFrameAttempt::Committed(frame) => frame,
            StreamingFrameAttempt::Aborted {
                failed_file,
                last_error,
            } => {
                let attempts = retry_journal.note_abort(failed_file.index, last_error);
                let abort_ack = send_frame_abort(stream, candidate_frame_no)?;
                if abort_ack.archive_len != committed_archive_len {
                    bail!(
                        "abort ack archive length mismatch: expected {} got {}",
                        committed_archive_len,
                        abort_ack.archive_len
                    );
                }
                frame_aborts = frame_aborts.saturating_add(1);
                frame_rebuilds = frame_rebuilds.saturating_add(1);
                debug!(
                    "sender frame aborted and will rebuild: frame_no={} file_index={} file_path={} attempts={} archive_len={}",
                    candidate_frame_no,
                    failed_file.index,
                    failed_file.rel_path,
                    attempts,
                    abort_ack.archive_len
                );
                pending = frame_pending;
                readers.finish();
                readers = new_reader_pool(options, profile);
                continue;
            }
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
        issue_journal.extend(frame.issues);
        committed_archive_len = frame.ack.archive_len;
        sent_frames += 1;
        next_frame_no = frame.outbound.frame_no;
    }
    readers.finish();
    let (final_file_index, final_file_path) = pending.final_boundary()?;
    let files_total_discovered = final_file_index;

    if !issue_journal.is_empty() {
        let issue_chunks = send_issue_journal(stream, &issue_journal)?;
        debug!(
            "sender uploaded issue journal: dump_id={} issues={} chunks={}",
            dump_id,
            issue_journal.len(),
            issue_chunks
        );
    }

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
        frame_aborts,
        frame_rebuilds,
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
        issue_journal,
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

pub fn send_frame_abort<T>(stream: &mut T, frame_no: u64) -> Result<AbortAck>
where
    T: std::io::Read + std::io::Write,
{
    protocol::write_message(
        stream,
        &Message::FrameAbort(protocol::FrameAbort { frame_no }),
    )?;
    stream
        .flush()
        .context("failed to flush frame abort to peer")?;

    let abort_ack = match protocol::read_message(stream)? {
        Message::AbortAck(abort_ack) => abort_ack,
        Message::Error(error) => bail!("peer rejected frame abort {}: {}", frame_no, error.message),
        unexpected => bail!(
            "unexpected message while waiting for abort ack: {:?}",
            unexpected.kind()
        ),
    };
    if abort_ack.frame_no != frame_no {
        bail!(
            "abort ack frame number mismatch: expected {} got {}",
            frame_no,
            abort_ack.frame_no
        );
    }

    Ok(abort_ack)
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::{self, Cursor, Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{
        OutboundFrame, SendOptions, SendStatus, send_dump, send_finish, send_frame,
        send_frame_abort,
    };
    use crate::network::protocol::{
        self, AbortAck, Ack, ErrorMessage, IssueJournalChunk, IssueJournalReason, Message,
        ResumeState,
    };
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
            crate::sender::frame::test_hooks::clear();
            Self { _lock: lock }
        }
    }

    impl Drop for ReadHookGuard {
        fn drop(&mut self) {
            crate::sender::readers::test_hooks::clear();
            crate::sender::frame::test_hooks::clear();
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

    fn write_sized_file(path: &Path, size: u64) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let file = File::create(path).unwrap();
        file.set_len(size).unwrap();
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

        let (ack, compressed_len, ack_wait) = writer.finish_frame(&frame).unwrap();

        assert_eq!(ack, expected_ack);
        assert_eq!(compressed_len, 7);
        assert!(ack_wait >= Duration::ZERO);
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
    fn send_frame_abort_writes_abort_and_accepts_abort_ack() {
        let mut stream = ScriptedStream::new(encode_message(&Message::AbortAck(AbortAck {
            frame_no: 4,
            archive_len: 1234,
        })));

        let abort_ack = send_frame_abort(&mut stream, 4).unwrap();

        assert_eq!(
            abort_ack,
            AbortAck {
                frame_no: 4,
                archive_len: 1234,
            }
        );
        assert_eq!(
            decode_messages(&stream.written),
            vec![Message::FrameAbort(protocol::FrameAbort { frame_no: 4 })]
        );
    }

    #[test]
    fn send_frame_abort_rejects_unexpected_reply() {
        let mut stream = ScriptedStream::new(encode_message(&Message::Ack(Ack {
            frame_no: 4,
            last_file_index: 12,
            last_file_path: "root/b.bin".to_owned(),
            archive_len: 1234,
        })));

        let error = send_frame_abort(&mut stream, 4).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpected message while waiting for abort ack")
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
    fn send_dump_batches_small_files_without_per_file_flush() {
        let temp = TestDir::new();
        fs::write(temp.path().join("alpha.txt"), b"alpha").unwrap();
        fs::write(temp.path().join("beta.txt"), b"beta").unwrap();

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
                last_file_index: 2,
                last_file_path: "beta.txt".to_owned(),
                archive_len: 64,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 16 * 1024 * 1024,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 4,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-batched-small-files",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 1);
        let messages = decode_messages(&stream.written);
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameChunk(_)))
                .count(),
            1
        );
    }

    #[test]
    fn send_dump_isolates_retried_file_into_dedicated_frames() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        let gamma_path = temp.path().join("gamma.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        fs::write(&beta_path, b"beta").unwrap();
        fs::write(&gamma_path, b"gamma").unwrap();

        crate::sender::frame::test_hooks::fail_stream_after_next_write(&beta_path, 1);

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
            &Message::AbortAck(protocol::AbortAck {
                frame_no: 1,
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
                archive_len: 64,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 2,
                last_file_index: 2,
                last_file_path: "beta.txt".to_owned(),
                archive_len: 128,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 3,
                last_file_index: 3,
                last_file_path: "gamma.txt".to_owned(),
                archive_len: 192,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 16 * 1024 * 1024,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 4,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-retried-isolated",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 3);
        let messages = decode_messages(&stream.written);
        let frame_starts: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                Message::FrameStart(frame_start) => Some(frame_start),
                _ => None,
            })
            .collect();
        assert_eq!(frame_starts.len(), 4);
        assert_eq!(frame_starts[1].first_file_path, "alpha.txt");
        assert_eq!(frame_starts[2].first_file_path, "beta.txt");
        assert_eq!(frame_starts[3].first_file_path, "gamma.txt");
    }

    #[test]
    fn send_dump_isolates_large_direct_file_into_dedicated_frame() {
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let payload_path = temp.path().join("payload.bin");
        let tail_path = temp.path().join("zeta.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        write_sized_file(
            &payload_path,
            crate::sender::frame::DEFAULT_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES + 4096,
        );
        fs::write(&tail_path, b"tail").unwrap();

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
                archive_len: 64,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 2,
                last_file_index: 2,
                last_file_path: "payload.bin".to_owned(),
                archive_len: 128,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 3,
                last_file_index: 3,
                last_file_path: "zeta.txt".to_owned(),
                archive_len: 192,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 16 * 1024 * 1024,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 32 * 1024,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-large-isolated",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 3);
        let messages = decode_messages(&stream.written);
        let frame_starts: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                Message::FrameStart(frame_start) => Some(frame_start),
                _ => None,
            })
            .collect();
        assert_eq!(frame_starts.len(), 3);
        assert_eq!(frame_starts[0].first_file_path, "alpha.txt");
        assert_eq!(frame_starts[1].first_file_path, "payload.bin");
        assert_eq!(frame_starts[2].first_file_path, "zeta.txt");
    }

    #[test]
    fn send_dump_keeps_medium_direct_stream_file_in_shared_frame() {
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let payload_path = temp.path().join("payload.bin");
        let tail_path = temp.path().join("zeta.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        write_sized_file(
            &payload_path,
            crate::sender::readers::IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES + 4096,
        );
        fs::write(&tail_path, b"tail").unwrap();

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
                last_file_path: "zeta.txt".to_owned(),
                archive_len: 192,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 16 * 1024 * 1024,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 32 * 1024,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-medium-shared",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 1);
        let messages = decode_messages(&stream.written);
        let frame_starts: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                Message::FrameStart(frame_start) => Some(frame_start),
                _ => None,
            })
            .collect();
        assert_eq!(frame_starts.len(), 1);
        assert_eq!(frame_starts[0].first_file_path, "alpha.txt");
        let frame_ends: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                Message::FrameEnd(frame_end) => Some(frame_end),
                _ => None,
            })
            .collect();
        assert_eq!(frame_ends.len(), 1);
        assert_eq!(frame_ends[0].last_file_path, "zeta.txt");
    }

    #[test]
    fn send_dump_aborts_and_rebuilds_large_direct_stream_after_retryable_stream_failure() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let payload_path = temp.path().join("payload.bin");
        write_sized_file(
            &payload_path,
            crate::sender::frame::DEFAULT_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES + 4096,
        );

        crate::sender::frame::test_hooks::fail_stream_after_next_write(&payload_path, 1024);

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
            &Message::AbortAck(protocol::AbortAck {
                frame_no: 1,
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
                archive_len: 512,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 16 * 1024 * 1024,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 32 * 1024,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-large-direct-abort",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 1);
        assert_eq!(report.files_sent_clean, 0);
        assert_eq!(report.files_sent_best_effort, 1);
        assert_eq!(report.files_skipped_unstable_after_retries, 0);
        assert_eq!(report.retries_total, 1);
        assert_eq!(report.frame_aborts, 1);
        assert_eq!(report.frame_rebuilds, 1);

        let messages = decode_messages(&stream.written);
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameAbort(_)))
                .count(),
            1
        );
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameStart(_)))
                .count(),
            2
        );
    }

    #[test]
    fn send_dump_aborts_and_rebuilds_shared_frame_after_medium_direct_stream_failure() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let payload_path = temp.path().join("payload.bin");
        let tail_path = temp.path().join("zeta.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        write_sized_file(
            &payload_path,
            crate::sender::readers::IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES + 4096,
        );
        fs::write(&tail_path, b"tail").unwrap();

        crate::sender::frame::test_hooks::fail_stream_after_next_write(&payload_path, 1024);

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
            &Message::AbortAck(protocol::AbortAck {
                frame_no: 1,
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
                archive_len: 64,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 2,
                last_file_index: 2,
                last_file_path: "payload.bin".to_owned(),
                archive_len: 128,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 3,
                last_file_index: 3,
                last_file_path: "zeta.txt".to_owned(),
                archive_len: 192,
            }),
        )
        .unwrap();
        protocol::write_message(&mut responses, &Message::FinishAck).unwrap();

        let mut stream = ScriptedStream::new(responses);
        let options = SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 16 * 1024 * 1024,
            min_readers: 1,
            max_readers: 1,
            read_buffer_bytes: 32 * 1024,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-medium-direct-abort",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 3);
        assert_eq!(report.files_sent_clean, 2);
        assert_eq!(report.files_sent_best_effort, 1);
        assert_eq!(report.files_skipped_unstable_after_retries, 0);
        assert_eq!(report.retries_total, 1);
        assert_eq!(report.frame_aborts, 1);
        assert_eq!(report.frame_rebuilds, 1);

        let messages = decode_messages(&stream.written);
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameAbort(_)))
                .count(),
            1
        );
        let frame_starts: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                Message::FrameStart(frame_start) => Some(frame_start),
                _ => None,
            })
            .collect();
        assert_eq!(frame_starts.len(), 4);
        assert_eq!(frame_starts[0].frame_no, 1);
        assert_eq!(frame_starts[0].first_file_path, "alpha.txt");
        assert_eq!(frame_starts[1].frame_no, 1);
        assert_eq!(frame_starts[1].first_file_path, "alpha.txt");
        assert_eq!(frame_starts[2].frame_no, 2);
        assert_eq!(frame_starts[2].first_file_path, "payload.bin");
        assert_eq!(frame_starts[3].frame_no, 3);
        assert_eq!(frame_starts[3].first_file_path, "zeta.txt");
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
        assert_eq!(report.issue_journal.len(), 1);
        assert_eq!(report.issue_journal[0].file_index, 3);
        assert_eq!(report.issue_journal[0].file_path, "gamma.txt");
        assert_eq!(report.issue_journal[0].attempts, 2);
        assert_eq!(
            report.issue_journal[0].reason,
            super::SendIssueReason::OpenFailed
        );
        assert!(report.issue_journal[0].last_error.contains("sender file"));
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
    fn send_dump_aborts_and_rebuilds_frame_after_retryable_stream_failure() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        fs::write(&beta_path, b"beta").unwrap();

        crate::sender::frame::test_hooks::fail_stream_after_next_write(&beta_path, 1);

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
            &Message::AbortAck(protocol::AbortAck {
                frame_no: 1,
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
                archive_len: 64,
            }),
        )
        .unwrap();
        protocol::write_message(
            &mut responses,
            &Message::Ack(Ack {
                frame_no: 2,
                last_file_index: 2,
                last_file_path: "beta.txt".to_owned(),
                archive_len: 128,
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
            max_readers: 1,
            read_buffer_bytes: 4,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-abort-rebuild",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 2);
        assert_eq!(report.files_sent_clean, 1);
        assert_eq!(report.files_sent_best_effort, 1);
        assert_eq!(report.files_skipped_open_failed, 0);
        assert_eq!(report.files_skipped_unstable_after_retries, 0);
        assert_eq!(report.retries_total, 1);
        assert_eq!(report.frame_aborts, 1);
        assert_eq!(report.frame_rebuilds, 1);

        let messages = decode_messages(&stream.written);
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameAbort(_)))
                .count(),
            1
        );
        let frame_starts: Vec<_> = messages
            .iter()
            .filter_map(|message| match message {
                Message::FrameStart(frame_start) => Some(frame_start),
                _ => None,
            })
            .collect();
        assert_eq!(frame_starts.len(), 3);
        assert_eq!(frame_starts[0].frame_no, 1);
        assert_eq!(frame_starts[1].frame_no, 1);
        assert_eq!(frame_starts[2].frame_no, 2);
    }

    #[test]
    fn send_dump_skips_file_after_three_retryable_reader_failures() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        let gamma_path = temp.path().join("gamma.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        fs::write(&beta_path, b"beta").unwrap();
        fs::write(&gamma_path, b"gamma").unwrap();

        for _ in 0..3 {
            crate::sender::readers::test_hooks::force_short_read_after_next_open(&beta_path, 1);
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
                archive_len: 64,
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
            max_readers: 1,
            read_buffer_bytes: 4,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-abort-skip",
            &options,
        )
        .unwrap();

        assert_eq!(report.sent_frames, 1);
        assert_eq!(report.files_sent_clean, 2);
        assert_eq!(report.files_sent_best_effort, 0);
        assert_eq!(report.files_skipped_open_failed, 0);
        assert_eq!(report.files_skipped_unstable_after_retries, 1);
        assert_eq!(report.retries_total, 2);
        assert_eq!(report.frame_aborts, 0);
        assert_eq!(report.frame_rebuilds, 0);
        assert_eq!(report.issue_journal.len(), 1);
        assert_eq!(report.issue_journal[0].file_index, 2);
        assert_eq!(report.issue_journal[0].file_path, "beta.txt");
        assert_eq!(report.issue_journal[0].attempts, 2);
        assert_eq!(
            report.issue_journal[0].reason,
            super::SendIssueReason::UnstableAfterRetries
        );
        assert!(report.issue_journal[0].last_error.contains("sender file"));
        assert_eq!(report.final_file_index, 3);
        assert_eq!(report.final_file_path, "gamma.txt");

        let messages = decode_messages(&stream.written);
        assert_eq!(
            messages
                .iter()
                .filter(|message| matches!(message, Message::FrameAbort(_)))
                .count(),
            0
        );
    }

    #[test]
    fn send_dump_uploads_issue_journal_before_finish() {
        let _guard = ReadHookGuard::new();
        let temp = TestDir::new();
        let alpha_path = temp.path().join("alpha.txt");
        let beta_path = temp.path().join("beta.txt");
        let gamma_path = temp.path().join("gamma.txt");
        fs::write(&alpha_path, b"alpha").unwrap();
        fs::write(&beta_path, b"beta").unwrap();
        fs::write(&gamma_path, b"gamma").unwrap();

        for _ in 0..3 {
            crate::sender::readers::test_hooks::force_short_read_after_next_open(&beta_path, 1);
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
                archive_len: 64,
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
            max_readers: 1,
            read_buffer_bytes: 4,
        };

        let report = send_dump(
            &mut stream,
            temp.path().to_path_buf(),
            "dump-issue-upload",
            &options,
        )
        .unwrap();

        assert_eq!(report.issue_journal.len(), 1);

        let messages = decode_messages(&stream.written);
        let issue_chunk_pos = messages
            .iter()
            .position(|message| matches!(message, Message::IssueJournalChunk(_)))
            .unwrap();
        let finish_pos = messages
            .iter()
            .position(|message| matches!(message, Message::Finish(_)))
            .unwrap();
        assert!(issue_chunk_pos < finish_pos);

        let issue_chunk = match &messages[issue_chunk_pos] {
            Message::IssueJournalChunk(chunk) => chunk,
            other => panic!("unexpected message: {other:?}"),
        };
        assert_eq!(
            issue_chunk,
            &IssueJournalChunk {
                entries: vec![protocol::IssueJournalEntry {
                    file_index: 2,
                    file_path: "beta.txt".to_owned(),
                    attempts: 2,
                    reason: IssueJournalReason::UnstableAfterRetries,
                    last_error: report.issue_journal[0].last_error.clone(),
                }],
            }
        );
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
