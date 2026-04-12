use std::collections::BTreeMap;
use std::env;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use log::{debug, warn};
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::{
    self, AbortAck, Ack, Finish, FrameChunk, FrameEnd, FrameStart, IssueChunk, IssueCode,
    IssueEntry, IssueScope, ManifestAck, ManifestBoundary, ManifestSeal, ManifestStart, Message,
    ResumeState,
};

use super::frame::{
    FramePolicy, FrameProgress, RetryableStreamFailure, stream_direct_file, stream_staged_snapshot,
};
use super::readers::{FileDisposition, FileSkipReason, MAX_SNAPSHOT_ATTEMPTS};
use super::source::{
    self, SenderSource, SenderSourceKind, SourceDiscoveryIssue, SourceDiscoveryIssueReason,
    SourceDiscoveryIssueScope,
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendDiscoveryIssueScope {
    Entry,
    Subtree,
    SenderState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendDiscoveryIssueReason {
    UnsupportedEntry { kind: String },
    UnreadableSymlinkTarget { error: String },
    VirtualFilesystem { kind: String },
    Io { operation: String, error: String },
    SenderState { event: String, detail: String },
    ReadDirFailed { error: String },
    SourceInvalidated { detail: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendDiscoveryIssue {
    pub record_seq: Option<u64>,
    pub path: String,
    pub scope: SendDiscoveryIssueScope,
    pub reason: SendDiscoveryIssueReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendReport {
    pub dump_id: String,
    pub resume_state: source::FileResumeState,
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
    pub discovery_issues_total: u64,
    pub subtrees_skipped: u64,
    pub manifest_resets_total: u64,
    pub retries_total: u64,
    pub bytes_sent_raw: u64,
    pub bytes_sent_best_effort_raw: u64,
    pub final_archive_bytes: u64,
    pub issue_journal: Vec<SendIssue>,
    pub discovery_issue_journal: Vec<SendDiscoveryIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendOptions {
    pub level: i32,
    pub threads: u32,
    pub frame_target_bytes: u64,
    pub min_readers: usize,
    pub max_readers: usize,
    pub read_buffer_bytes: usize,
    pub manifest_chunk_bytes: usize,
    pub manifest_ack_records: u64,
    pub manifest_ack_bytes: usize,
}

const PROGRESSIVE_DISCOVERY_WINDOW_FILES: usize = 4096;
const PROGRESSIVE_BOOTSTRAP_WINDOW_FILES: usize = 128;
const PROGRESSIVE_BOOTSTRAP_LARGE_FILE_WINDOW_FILES: usize = 1024;
const PROGRESSIVE_BOOTSTRAP_WINDOW_FILES_ENV: &str = "FAST_DUMP_PROGRESSIVE_BOOTSTRAP_WINDOW_FILES";

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

fn issue_scope(scope: SendDiscoveryIssueScope) -> IssueScope {
    match scope {
        SendDiscoveryIssueScope::Entry => IssueScope::DiscoveryEntry,
        SendDiscoveryIssueScope::Subtree => IssueScope::DiscoverySubtree,
        SendDiscoveryIssueScope::SenderState => IssueScope::SenderState,
    }
}

fn issue_code(reason: &SendIssueReason) -> IssueCode {
    match reason {
        SendIssueReason::OpenFailed => IssueCode::OpenFailed,
        SendIssueReason::UnstableAfterRetries => IssueCode::UnstableAfterRetries,
    }
}

fn discovery_issue_code(reason: &SendDiscoveryIssueReason) -> IssueCode {
    match reason {
        SendDiscoveryIssueReason::UnsupportedEntry { .. } => IssueCode::UnsupportedEntry,
        SendDiscoveryIssueReason::UnreadableSymlinkTarget { .. } => {
            IssueCode::UnreadableSymlinkTarget
        }
        SendDiscoveryIssueReason::VirtualFilesystem { .. } => IssueCode::VirtualFilesystem,
        SendDiscoveryIssueReason::Io { .. } => IssueCode::Io,
        SendDiscoveryIssueReason::SenderState { .. } => IssueCode::SenderStateEvent,
        SendDiscoveryIssueReason::ReadDirFailed { .. } => IssueCode::ReadDirFailed,
        SendDiscoveryIssueReason::SourceInvalidated { .. } => IssueCode::SourceInvalidated,
    }
}

fn typed_issue_entry(issue: &SendIssue) -> IssueEntry {
    IssueEntry {
        scope: IssueScope::File,
        record_seq: None,
        work_ordinal: Some(issue.file_index),
        path: issue.file_path.clone(),
        attempts: issue.attempts,
        code: issue_code(&issue.reason),
        detail: issue.last_error.clone(),
        context: String::new(),
    }
}

fn typed_discovery_issue_entry(issue: &SendDiscoveryIssue) -> IssueEntry {
    let (detail, context) = match &issue.reason {
        SendDiscoveryIssueReason::UnsupportedEntry { kind } => (kind.clone(), String::new()),
        SendDiscoveryIssueReason::UnreadableSymlinkTarget { error } => {
            (error.clone(), String::new())
        }
        SendDiscoveryIssueReason::VirtualFilesystem { kind } => (kind.clone(), String::new()),
        SendDiscoveryIssueReason::Io { operation, error } => (error.clone(), operation.clone()),
        SendDiscoveryIssueReason::SenderState { event, detail } => (detail.clone(), event.clone()),
        SendDiscoveryIssueReason::ReadDirFailed { error } => (error.clone(), String::new()),
        SendDiscoveryIssueReason::SourceInvalidated { detail } => (detail.clone(), String::new()),
    };
    IssueEntry {
        scope: issue_scope(issue.scope),
        record_seq: issue.record_seq,
        work_ordinal: None,
        path: issue.path.clone(),
        attempts: 0,
        code: discovery_issue_code(&issue.reason),
        detail,
        context,
    }
}

fn send_discovery_scope(scope: SourceDiscoveryIssueScope) -> SendDiscoveryIssueScope {
    match scope {
        SourceDiscoveryIssueScope::Entry => SendDiscoveryIssueScope::Entry,
        SourceDiscoveryIssueScope::Subtree => SendDiscoveryIssueScope::Subtree,
        SourceDiscoveryIssueScope::SenderState => SendDiscoveryIssueScope::SenderState,
    }
}

fn special_entry_kind_label(kind: source::manifest::ManifestSpecialEntryKind) -> &'static str {
    match kind {
        source::manifest::ManifestSpecialEntryKind::Socket => "socket",
        source::manifest::ManifestSpecialEntryKind::Fifo => "fifo",
        source::manifest::ManifestSpecialEntryKind::BlockDevice => "block_device",
        source::manifest::ManifestSpecialEntryKind::CharDevice => "char_device",
        source::manifest::ManifestSpecialEntryKind::Other => "other",
    }
}

fn manifest_io_operation_label(operation: source::manifest::ManifestIoOperation) -> &'static str {
    match operation {
        source::manifest::ManifestIoOperation::ReadDir => "read_dir",
        source::manifest::ManifestIoOperation::ReadLink => "read_link",
        source::manifest::ManifestIoOperation::ClassifyEntry => "classify_entry",
        source::manifest::ManifestIoOperation::Replay => "replay",
    }
}

fn manifest_sender_state_event_label(
    event: source::manifest::ManifestSenderStateEvent,
) -> &'static str {
    match event {
        source::manifest::ManifestSenderStateEvent::Invalidated => "invalidated",
        source::manifest::ManifestSenderStateEvent::Corrupted => "corrupted",
        source::manifest::ManifestSenderStateEvent::Rebuilt => "rebuilt",
    }
}

fn send_discovery_reason(reason: SourceDiscoveryIssueReason) -> SendDiscoveryIssueReason {
    match reason {
        SourceDiscoveryIssueReason::UnsupportedEntry { kind } => {
            SendDiscoveryIssueReason::UnsupportedEntry {
                kind: special_entry_kind_label(kind).to_owned(),
            }
        }
        SourceDiscoveryIssueReason::UnreadableSymlinkTarget { error } => {
            SendDiscoveryIssueReason::UnreadableSymlinkTarget { error }
        }
        SourceDiscoveryIssueReason::VirtualFilesystem { kind } => {
            SendDiscoveryIssueReason::VirtualFilesystem { kind }
        }
        SourceDiscoveryIssueReason::Io { operation, error } => SendDiscoveryIssueReason::Io {
            operation: manifest_io_operation_label(operation).to_owned(),
            error,
        },
        SourceDiscoveryIssueReason::SenderState { event, detail } => {
            SendDiscoveryIssueReason::SenderState {
                event: manifest_sender_state_event_label(event).to_owned(),
                detail,
            }
        }
        SourceDiscoveryIssueReason::ReadDirFailed { error } => {
            SendDiscoveryIssueReason::ReadDirFailed { error }
        }
        SourceDiscoveryIssueReason::SourceInvalidated { detail } => {
            SendDiscoveryIssueReason::SourceInvalidated { detail }
        }
    }
}

fn send_discovery_issue(issue: SourceDiscoveryIssue) -> SendDiscoveryIssue {
    SendDiscoveryIssue {
        record_seq: issue.record_seq,
        path: issue.path,
        scope: send_discovery_scope(issue.scope),
        reason: send_discovery_reason(issue.reason),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn send_issue_journal<T>(
    stream: &mut T,
    file_issues: &[SendIssue],
    discovery_issues: &[SendDiscoveryIssue],
) -> Result<usize>
where
    T: Write,
{
    let mut chunks_sent = 0_usize;
    let mut pending = Vec::<IssueEntry>::new();
    let push_entry = |entry: IssueEntry,
                      stream: &mut T,
                      pending: &mut Vec<IssueEntry>,
                      chunks_sent: &mut usize|
     -> Result<()> {
        let mut candidate = pending.clone();
        candidate.push(entry.clone());
        let candidate_message = Message::IssueChunk(IssueChunk { entries: candidate });
        if protocol::write_message(&mut Vec::new(), &candidate_message).is_ok() {
            pending.push(entry);
            return Ok(());
        }
        if pending.is_empty() {
            protocol::write_message(stream, &candidate_message)
                .context("typed v3 issue entry does not fit into a single protocol chunk")?;
            *chunks_sent = chunks_sent.saturating_add(1);
            return Ok(());
        }

        protocol::write_message(
            stream,
            &Message::IssueChunk(IssueChunk {
                entries: std::mem::take(pending),
            }),
        )?;
        *chunks_sent = chunks_sent.saturating_add(1);
        pending.push(entry);
        Ok(())
    };

    for issue in file_issues {
        push_entry(
            typed_issue_entry(issue),
            stream,
            &mut pending,
            &mut chunks_sent,
        )?;
    }
    for issue in discovery_issues {
        push_entry(
            typed_discovery_issue_entry(issue),
            stream,
            &mut pending,
            &mut chunks_sent,
        )?;
    }

    if !pending.is_empty() {
        protocol::write_message(
            stream,
            &Message::IssueChunk(IssueChunk { entries: pending }),
        )?;
        chunks_sent = chunks_sent.saturating_add(1);
    }

    Ok(chunks_sent)
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestTransferChunkPlan {
    bytes: Vec<u8>,
    last_record_seq: Option<u64>,
    last_work_ordinal: Option<u64>,
    record_count: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedManifestTransfer {
    chunks: Vec<ManifestTransferChunkPlan>,
    final_record_seq: Option<u64>,
    final_work_ordinal: Option<u64>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestTransferState {
    base_manifest: ManifestBoundary,
    transfer: PreparedManifestTransfer,
    committed: ManifestBoundary,
    next_chunk_index: usize,
    unacked_bytes: usize,
    unacked_records: u64,
    started: bool,
    seal_sent: bool,
    payload_bytes_sent: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ProgressiveManifestTransferState {
    base_manifest: ManifestBoundary,
    committed: ManifestBoundary,
    started: bool,
    seal_sent: bool,
    unacked_bytes: usize,
    unacked_records: u64,
    payload_bytes_sent: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
enum ManifestTransferStateKind {
    Prepared(ManifestTransferState),
    Progressive(ProgressiveManifestTransferState),
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct SessionTimings {
    manifest_transfer_started: Option<Instant>,
    manifest_transfer_finished: Option<Instant>,
    first_frame_started: Option<Instant>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct FrameWindow {
    max_work_ordinal: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
fn prepare_manifest_transfer(
    records: &[source::manifest::ManifestRecord],
    base_manifest: &ManifestBoundary,
    chunk_bytes: usize,
    chunk_records: u64,
) -> Result<PreparedManifestTransfer> {
    let final_record_seq = records.last().map(|record| record.record_seq.0);
    let final_work_ordinal = records
        .iter()
        .filter_map(|record| record.work_ordinal.map(|value| value.0))
        .next_back();

    if let Some(base_record_seq) = base_manifest.last_record_seq {
        let base_record = records
            .iter()
            .find(|record| record.record_seq.0 == base_record_seq)
            .with_context(|| {
                format!(
                    "receiver manifest boundary record_seq {} is not present in sender spool",
                    base_record_seq
                )
            })?;
        let sender_work_ordinal = base_record.work_ordinal.map(|value| value.0);
        if sender_work_ordinal != base_manifest.last_work_ordinal {
            bail!(
                "receiver manifest boundary work_ordinal mismatch at record_seq {}: sender={:?} receiver={:?}",
                base_record_seq,
                sender_work_ordinal,
                base_manifest.last_work_ordinal
            );
        }
    }

    if final_record_seq.unwrap_or(0) < base_manifest.last_record_seq.unwrap_or(0) {
        bail!(
            "receiver manifest boundary record_seq {} is ahead of sender spool {:?}",
            base_manifest.last_record_seq.unwrap_or(0),
            final_record_seq
        );
    }
    if final_work_ordinal.unwrap_or(0) < base_manifest.last_work_ordinal.unwrap_or(0) {
        bail!(
            "receiver manifest boundary work_ordinal {} is ahead of sender spool {:?}",
            base_manifest.last_work_ordinal.unwrap_or(0),
            final_work_ordinal
        );
    }
    if base_manifest.sealed
        && (base_manifest.last_record_seq != final_record_seq
            || base_manifest.last_work_ordinal != final_work_ordinal)
    {
        bail!(
            "receiver manifest boundary is sealed at {:?}/{:?}, but sender final boundary is {:?}/{:?}",
            base_manifest.last_record_seq,
            base_manifest.last_work_ordinal,
            final_record_seq,
            final_work_ordinal
        );
    }

    let resume_after_record_seq = base_manifest.last_record_seq.unwrap_or(0);
    let mut chunks = Vec::new();
    let mut pending_bytes = Vec::new();
    let mut pending_last_record_seq = None;
    let mut pending_last_work_ordinal = None;
    let mut pending_record_count = 0_u64;
    let mut carried_last_work_ordinal = base_manifest.last_work_ordinal;

    for record in records {
        if record.record_seq.0 <= resume_after_record_seq {
            continue;
        }

        let encoded = source::manifest::ManifestSpoolWriter::encode_record(record)?;
        if encoded.len() > chunk_bytes {
            bail!(
                "manifest record {} exceeds configured manifest chunk window: {} > {} bytes",
                record.record_seq.0,
                encoded.len(),
                chunk_bytes
            );
        }

        let chunk_full = !pending_bytes.is_empty()
            && (pending_bytes.len() + encoded.len() > chunk_bytes
                || pending_record_count >= chunk_records);
        if chunk_full {
            chunks.push(ManifestTransferChunkPlan {
                bytes: std::mem::take(&mut pending_bytes),
                last_record_seq: pending_last_record_seq,
                last_work_ordinal: pending_last_work_ordinal,
                record_count: pending_record_count,
            });
            carried_last_work_ordinal = pending_last_work_ordinal;
            pending_record_count = 0;
        }
        if pending_record_count == 0 {
            pending_last_work_ordinal = carried_last_work_ordinal;
        }

        pending_bytes.extend_from_slice(&encoded);
        pending_last_record_seq = Some(record.record_seq.0);
        if let Some(work_ordinal) = record.work_ordinal {
            pending_last_work_ordinal = Some(work_ordinal.0);
        }
        pending_record_count = pending_record_count.saturating_add(1);
    }

    if !pending_bytes.is_empty() {
        chunks.push(ManifestTransferChunkPlan {
            bytes: pending_bytes,
            last_record_seq: pending_last_record_seq,
            last_work_ordinal: pending_last_work_ordinal,
            record_count: pending_record_count,
        });
    }

    Ok(PreparedManifestTransfer {
        chunks,
        final_record_seq,
        final_work_ordinal,
    })
}

#[cfg_attr(not(test), allow(dead_code))]
impl ManifestTransferState {
    fn new(base_manifest: ManifestBoundary, transfer: PreparedManifestTransfer) -> Self {
        Self {
            base_manifest: base_manifest.clone(),
            transfer,
            committed: base_manifest,
            next_chunk_index: 0,
            unacked_bytes: 0,
            unacked_records: 0,
            started: false,
            seal_sent: false,
            payload_bytes_sent: 0,
        }
    }

    fn committed_work_ordinal(&self) -> u64 {
        self.committed.last_work_ordinal.unwrap_or(0)
    }

    fn is_complete(&self) -> bool {
        self.committed.sealed
            && self.committed.last_record_seq == self.transfer.final_record_seq
            && self.committed.last_work_ordinal == self.transfer.final_work_ordinal
    }

    fn start_if_needed<T>(&mut self, stream: &mut T, timings: &mut SessionTimings) -> Result<()>
    where
        T: std::io::Write,
    {
        if self.started {
            return Ok(());
        }
        protocol::write_message(
            stream,
            &Message::ManifestStart(ManifestStart {
                base_manifest: self.base_manifest.clone(),
            }),
        )?;
        self.started = true;
        timings
            .manifest_transfer_started
            .get_or_insert_with(Instant::now);
        Ok(())
    }

    fn advance_until_cover<T>(
        &mut self,
        stream: &mut T,
        target_work_ordinal: u64,
        options: &SendOptions,
        timings: &mut SessionTimings,
    ) -> Result<()>
    where
        T: std::io::Read + std::io::Write,
    {
        self.start_if_needed(stream, timings)?;
        if target_work_ordinal == 0 || self.committed_work_ordinal() >= target_work_ordinal {
            return Ok(());
        }

        while self.committed_work_ordinal() < target_work_ordinal {
            if self.next_chunk_index >= self.transfer.chunks.len() {
                self.finish(stream, options, timings)?;
                break;
            }

            let chunk = &self.transfer.chunks[self.next_chunk_index];
            protocol::write_manifest_chunk_message(
                stream,
                &chunk.bytes,
                chunk.last_record_seq,
                chunk.last_work_ordinal,
            )?;
            self.payload_bytes_sent = self
                .payload_bytes_sent
                .saturating_add(chunk.bytes.len() as u64);
            self.unacked_bytes = self.unacked_bytes.saturating_add(chunk.bytes.len());
            self.unacked_records = self.unacked_records.saturating_add(chunk.record_count);
            self.next_chunk_index = self.next_chunk_index.saturating_add(1);

            if self.unacked_bytes >= options.manifest_ack_bytes
                || self.unacked_records >= options.manifest_ack_records
            {
                self.read_ack(stream, false, timings)?;
            }
        }

        if self.committed_work_ordinal() < target_work_ordinal {
            bail!(
                "durable manifest boundary {} does not cover required work ordinal {}",
                self.committed_work_ordinal(),
                target_work_ordinal
            );
        }

        Ok(())
    }

    fn finish<T>(
        &mut self,
        stream: &mut T,
        options: &SendOptions,
        timings: &mut SessionTimings,
    ) -> Result<ManifestBoundary>
    where
        T: std::io::Read + std::io::Write,
    {
        self.start_if_needed(stream, timings)?;
        if self.is_complete() {
            timings
                .manifest_transfer_finished
                .get_or_insert_with(Instant::now);
            return Ok(self.committed.clone());
        }

        while self.next_chunk_index < self.transfer.chunks.len() {
            let chunk = &self.transfer.chunks[self.next_chunk_index];
            protocol::write_manifest_chunk_message(
                stream,
                &chunk.bytes,
                chunk.last_record_seq,
                chunk.last_work_ordinal,
            )?;
            self.payload_bytes_sent = self
                .payload_bytes_sent
                .saturating_add(chunk.bytes.len() as u64);
            self.unacked_bytes = self.unacked_bytes.saturating_add(chunk.bytes.len());
            self.unacked_records = self.unacked_records.saturating_add(chunk.record_count);
            self.next_chunk_index = self.next_chunk_index.saturating_add(1);

            if self.unacked_bytes >= options.manifest_ack_bytes
                || self.unacked_records >= options.manifest_ack_records
            {
                self.read_ack(stream, false, timings)?;
            }
        }

        if !self.seal_sent {
            protocol::write_message(
                stream,
                &Message::ManifestSeal(ManifestSeal {
                    final_record_seq: self.transfer.final_record_seq,
                    final_work_ordinal: self.transfer.final_work_ordinal,
                }),
            )?;
            self.seal_sent = true;
        }

        self.read_ack(stream, true, timings)?;
        timings
            .manifest_transfer_finished
            .get_or_insert_with(Instant::now);
        Ok(self.committed.clone())
    }

    fn read_ack<T>(
        &mut self,
        stream: &mut T,
        require_sealed: bool,
        timings: &mut SessionTimings,
    ) -> Result<()>
    where
        T: std::io::Read,
    {
        if self.unacked_bytes == 0 && self.unacked_records == 0 && !require_sealed {
            return Ok(());
        }
        self.committed = read_manifest_ack(
            stream,
            &self.committed,
            self.transfer.final_record_seq,
            self.transfer.final_work_ordinal,
            require_sealed,
        )?;
        self.unacked_bytes = 0;
        self.unacked_records = 0;
        if self.committed.sealed {
            timings
                .manifest_transfer_finished
                .get_or_insert_with(Instant::now);
        }
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl ProgressiveManifestTransferState {
    fn new(base_manifest: ManifestBoundary) -> Self {
        Self {
            base_manifest: base_manifest.clone(),
            committed: base_manifest,
            started: false,
            seal_sent: false,
            unacked_bytes: 0,
            unacked_records: 0,
            payload_bytes_sent: 0,
        }
    }

    fn committed_work_ordinal(&self) -> u64 {
        self.committed.last_work_ordinal.unwrap_or(0)
    }

    fn start_if_needed<T>(&mut self, stream: &mut T, timings: &mut SessionTimings) -> Result<()>
    where
        T: std::io::Write,
    {
        if self.started {
            return Ok(());
        }
        protocol::write_message(
            stream,
            &Message::ManifestStart(ManifestStart {
                base_manifest: self.base_manifest.clone(),
            }),
        )?;
        self.started = true;
        timings
            .manifest_transfer_started
            .get_or_insert_with(Instant::now);
        Ok(())
    }

    fn seal_if_needed<T>(&mut self, stream: &mut T, pending: &PendingFrameFiles) -> Result<()>
    where
        T: std::io::Write,
    {
        if self.seal_sent {
            return Ok(());
        }

        let (final_record_seq, final_work_ordinal) = pending.final_manifest_boundary()?;
        protocol::write_message(
            stream,
            &Message::ManifestSeal(ManifestSeal {
                final_record_seq,
                final_work_ordinal,
            }),
        )?;
        self.seal_sent = true;
        Ok(())
    }

    fn advance_until_cover<T>(
        &mut self,
        stream: &mut T,
        pending: &mut PendingFrameFiles,
        target_work_ordinal: u64,
        options: &SendOptions,
        timings: &mut SessionTimings,
    ) -> Result<()>
    where
        T: std::io::Read + std::io::Write,
    {
        self.start_if_needed(stream, timings)?;
        if target_work_ordinal == 0 || self.committed_work_ordinal() >= target_work_ordinal {
            return Ok(());
        }

        loop {
            if self.committed_work_ordinal() >= target_work_ordinal {
                return Ok(());
            }

            let produced_work_ordinal = pending.produced_manifest_work_ordinal();
            let force_flush =
                produced_work_ordinal >= target_work_ordinal || pending.manifest_exhausted();
            if let Some(chunk) = pending.pop_manifest_chunk(force_flush)? {
                protocol::write_manifest_chunk_message(
                    stream,
                    &chunk.bytes,
                    chunk.last_record_seq,
                    chunk.last_work_ordinal,
                )?;
                self.payload_bytes_sent = self
                    .payload_bytes_sent
                    .saturating_add(chunk.bytes.len() as u64);
                self.unacked_bytes = self.unacked_bytes.saturating_add(chunk.bytes.len());
                self.unacked_records = self.unacked_records.saturating_add(chunk.record_count);
                if self.unacked_bytes >= options.manifest_ack_bytes
                    || self.unacked_records >= options.manifest_ack_records
                {
                    self.read_ack(stream, false, pending, timings)?;
                }
                continue;
            }

            if self.unacked_bytes > 0 || self.unacked_records > 0 {
                if pending.manifest_exhausted() {
                    self.seal_if_needed(stream, pending)?;
                    self.read_ack(stream, true, pending, timings)?;
                    continue;
                }

                let produced_before = pending.produced_manifest_work_ordinal();
                let _ = pending.grow_discovery_window(progressive_manifest_growth_step(
                    pending,
                    target_work_ordinal,
                ))?;
                if pending.produced_manifest_work_ordinal() > produced_before
                    || pending.manifest_exhausted()
                {
                    continue;
                }

                bail!(
                    "progressive manifest producer stalled before durable ack for work ordinal {}",
                    target_work_ordinal
                );
            }

            if pending.manifest_exhausted() {
                self.seal_if_needed(stream, pending)?;
                self.read_ack(stream, true, pending, timings)?;
                continue;
            }

            let produced_before = produced_work_ordinal;
            let _ = pending.grow_discovery_window(progressive_manifest_growth_step(
                pending,
                target_work_ordinal,
            ))?;
            if pending.produced_manifest_work_ordinal() > produced_before
                || pending.manifest_exhausted()
            {
                continue;
            }

            bail!(
                "progressive manifest producer has not covered required work ordinal {} (produced boundary {})",
                target_work_ordinal,
                produced_work_ordinal
            );
        }
    }

    fn finish<T>(
        &mut self,
        stream: &mut T,
        pending: &mut PendingFrameFiles,
        options: &SendOptions,
        timings: &mut SessionTimings,
    ) -> Result<ManifestBoundary>
    where
        T: std::io::Read + std::io::Write,
    {
        if self.committed.sealed {
            timings
                .manifest_transfer_finished
                .get_or_insert_with(Instant::now);
            return Ok(self.committed.clone());
        }

        self.start_if_needed(stream, timings)?;
        while let Some(chunk) = pending.pop_manifest_chunk(true)? {
            protocol::write_manifest_chunk_message(
                stream,
                &chunk.bytes,
                chunk.last_record_seq,
                chunk.last_work_ordinal,
            )?;
            self.payload_bytes_sent = self
                .payload_bytes_sent
                .saturating_add(chunk.bytes.len() as u64);
            self.unacked_bytes = self.unacked_bytes.saturating_add(chunk.bytes.len());
            self.unacked_records = self.unacked_records.saturating_add(chunk.record_count);
            if self.unacked_bytes >= options.manifest_ack_bytes
                || self.unacked_records >= options.manifest_ack_records
            {
                self.read_ack(stream, false, pending, timings)?;
            }
        }

        self.seal_if_needed(stream, pending)?;

        self.read_ack(stream, true, pending, timings)?;
        timings
            .manifest_transfer_finished
            .get_or_insert_with(Instant::now);
        Ok(self.committed.clone())
    }

    fn read_ack<T>(
        &mut self,
        stream: &mut T,
        require_sealed: bool,
        pending: &PendingFrameFiles,
        timings: &mut SessionTimings,
    ) -> Result<()>
    where
        T: std::io::Read,
    {
        if self.unacked_bytes == 0 && self.unacked_records == 0 && !require_sealed {
            return Ok(());
        }
        let (final_record_seq, final_work_ordinal) = if require_sealed {
            pending.final_manifest_boundary()?
        } else {
            pending.produced_manifest_boundary()
        };
        self.committed = read_manifest_ack(
            stream,
            &self.committed,
            final_record_seq,
            final_work_ordinal,
            require_sealed,
        )?;
        self.unacked_bytes = 0;
        self.unacked_records = 0;
        if self.committed.sealed {
            timings
                .manifest_transfer_finished
                .get_or_insert_with(Instant::now);
        }
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
impl ManifestTransferStateKind {
    fn is_progressive(&self) -> bool {
        matches!(self, Self::Progressive(_))
    }

    fn committed_work_ordinal(&self) -> u64 {
        match self {
            Self::Prepared(state) => state.committed_work_ordinal(),
            Self::Progressive(state) => state.committed_work_ordinal(),
        }
    }

    fn payload_bytes_sent(&self) -> u64 {
        match self {
            Self::Prepared(state) => state.payload_bytes_sent,
            Self::Progressive(state) => state.payload_bytes_sent,
        }
    }

    fn advance_until_cover<T>(
        &mut self,
        stream: &mut T,
        pending: &mut PendingFrameFiles,
        target_work_ordinal: u64,
        options: &SendOptions,
        timings: &mut SessionTimings,
    ) -> Result<()>
    where
        T: std::io::Read + std::io::Write,
    {
        match self {
            Self::Prepared(state) => {
                state.advance_until_cover(stream, target_work_ordinal, options, timings)
            }
            Self::Progressive(state) => {
                state.advance_until_cover(stream, pending, target_work_ordinal, options, timings)
            }
        }
    }

    fn finish<T>(
        &mut self,
        stream: &mut T,
        pending: &mut PendingFrameFiles,
        options: &SendOptions,
        timings: &mut SessionTimings,
    ) -> Result<ManifestBoundary>
    where
        T: std::io::Read + std::io::Write,
    {
        match self {
            Self::Prepared(state) => state.finish(stream, options, timings),
            Self::Progressive(state) => state.finish(stream, pending, options, timings),
        }
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn manifest_relative_path(path_stack: &[Box<str>], leaf_name: &str) -> String {
    if path_stack.is_empty() {
        return leaf_name.to_owned();
    }

    let mut result = String::new();
    for component in path_stack {
        if !result.is_empty() {
            result.push('/');
        }
        result.push_str(component);
    }
    result.push('/');
    result.push_str(leaf_name);
    result
}

#[cfg_attr(not(test), allow(dead_code))]
fn manifest_path_for_work_ordinal(
    records: &[source::manifest::ManifestRecord],
    target_work_ordinal: u64,
) -> Result<String> {
    let mut path_stack = Vec::<Box<str>>::new();
    for record in records {
        match record.kind {
            source::manifest::ManifestRecordKind::DirectoryEnter => {
                if path_stack.len() != usize::from(record.depth) {
                    bail!(
                        "manifest directory enter depth {} does not match path stack length {}",
                        record.depth,
                        path_stack.len()
                    );
                }
                path_stack.push(record.name_component.clone());
            }
            source::manifest::ManifestRecordKind::DirectoryLeave => {
                if path_stack.len() != usize::from(record.depth).saturating_add(1) {
                    bail!(
                        "manifest directory leave depth {} does not match path stack length {}",
                        record.depth,
                        path_stack.len()
                    );
                }
                let Some(last_component) = path_stack.pop() else {
                    bail!("manifest directory leave encountered empty path stack");
                };
                if last_component.as_ref() != record.name_component.as_ref() {
                    bail!(
                        "manifest directory leave component mismatch: expected `{}`, got `{}`",
                        last_component,
                        record.name_component
                    );
                }
            }
            source::manifest::ManifestRecordKind::DiscoverySkip
            | source::manifest::ManifestRecordKind::SubtreeSkip => {}
            source::manifest::ManifestRecordKind::File
            | source::manifest::ManifestRecordKind::Symlink => {
                if record.work_ordinal.map(|value| value.0) == Some(target_work_ordinal) {
                    return Ok(manifest_relative_path(
                        &path_stack,
                        record.name_component.as_ref(),
                    ));
                }
            }
        }
    }

    bail!(
        "receiver archive boundary work ordinal {} is not present in sender manifest spool",
        target_work_ordinal
    );
}

#[cfg_attr(not(test), allow(dead_code))]
fn resume_state_from_archive(
    records: &[source::manifest::ManifestRecord],
    resume_state: &ResumeState,
) -> Result<source::FileResumeState> {
    let archive = &resume_state.archive;
    let last_committed_work_ordinal = archive.last_committed_work_ordinal.unwrap_or(0);
    let last_committed_file_path = if last_committed_work_ordinal == 0 {
        String::new()
    } else {
        manifest_path_for_work_ordinal(records, last_committed_work_ordinal)?
    };
    if archive.last_committed_work_ordinal.unwrap_or(0)
        > resume_state.manifest.last_work_ordinal.unwrap_or(0)
    {
        bail!(
            "receiver archive boundary {:?} is ahead of durable manifest boundary {:?}",
            archive.last_committed_work_ordinal,
            resume_state.manifest.last_work_ordinal
        );
    }

    Ok(source::FileResumeState {
        last_committed_frame: archive.last_committed_frame,
        last_committed_file_index: last_committed_work_ordinal,
        last_committed_file_path,
        archive_len: archive.archive_len,
    })
}

fn can_use_progressive_manifest_path(resume_state: &ResumeState) -> bool {
    resume_state.archive.last_committed_frame == 0
        && resume_state
            .archive
            .last_committed_work_ordinal
            .unwrap_or(0)
            == 0
        && resume_state.archive.archive_len == 0
        && resume_state.manifest.last_record_seq.is_none()
        && resume_state.manifest.last_work_ordinal.is_none()
        && !resume_state.manifest.sealed
}

#[cfg_attr(not(test), allow(dead_code))]
fn read_manifest_ack<T>(
    stream: &mut T,
    current: &ManifestBoundary,
    final_record_seq: Option<u64>,
    final_work_ordinal: Option<u64>,
    require_sealed: bool,
) -> Result<ManifestBoundary>
where
    T: std::io::Read,
{
    match protocol::read_message(stream)? {
        Message::ManifestAck(ManifestAck { committed_manifest }) => {
            if committed_manifest.manifest_bytes_persisted < current.manifest_bytes_persisted {
                bail!("manifest ack bytes must not move backwards");
            }
            if committed_manifest.last_record_seq.unwrap_or(0)
                < current.last_record_seq.unwrap_or(0)
            {
                bail!("manifest ack record_seq must not move backwards");
            }
            if committed_manifest.last_work_ordinal.unwrap_or(0)
                < current.last_work_ordinal.unwrap_or(0)
            {
                bail!("manifest ack work_ordinal must not move backwards");
            }
            if current.sealed && !committed_manifest.sealed {
                bail!("manifest ack must not move from sealed back to unsealed");
            }
            if require_sealed && !committed_manifest.sealed {
                bail!("manifest seal must be acknowledged with a sealed manifest boundary");
            }
            if committed_manifest.last_record_seq.unwrap_or(0) > final_record_seq.unwrap_or(0) {
                bail!("manifest ack record_seq exceeds sender spool boundary");
            }
            if committed_manifest.last_work_ordinal.unwrap_or(0) > final_work_ordinal.unwrap_or(0) {
                bail!("manifest ack work_ordinal exceeds sender spool boundary");
            }
            if committed_manifest.sealed
                && (committed_manifest.last_record_seq != final_record_seq
                    || committed_manifest.last_work_ordinal != final_work_ordinal)
            {
                bail!("sealed manifest ack does not match sender final boundary");
            }
            Ok(committed_manifest)
        }
        Message::Error(error) => bail!("peer rejected v3 manifest transfer: {}", error.message),
        unexpected => bail!(
            "unexpected message while waiting for v3 manifest ack: {:?}",
            unexpected.kind()
        ),
    }
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
    if options.manifest_chunk_bytes == 0 {
        bail!("manifest_chunk_bytes must be at least 1");
    }
    if options.manifest_chunk_bytes > protocol::MAX_MANIFEST_CHUNK_PAYLOAD_BYTES {
        bail!(
            "manifest_chunk_bytes exceeds protocol cap: {} > {} bytes",
            options.manifest_chunk_bytes,
            protocol::MAX_MANIFEST_CHUNK_PAYLOAD_BYTES
        );
    }
    if options.manifest_ack_records == 0 {
        bail!("manifest_ack_records must be at least 1");
    }
    if options.manifest_ack_records > protocol::MAX_UNACKED_MANIFEST_RECORDS {
        bail!(
            "manifest_ack_records exceeds protocol cap: {} > {}",
            options.manifest_ack_records,
            protocol::MAX_UNACKED_MANIFEST_RECORDS
        );
    }
    if options.manifest_ack_bytes == 0 {
        bail!("manifest_ack_bytes must be at least 1");
    }
    if options.manifest_ack_bytes > protocol::MAX_UNACKED_MANIFEST_BYTES {
        bail!(
            "manifest_ack_bytes exceeds protocol cap: {} > {} bytes",
            options.manifest_ack_bytes,
            protocol::MAX_UNACKED_MANIFEST_BYTES
        );
    }
    if options.manifest_ack_bytes < options.manifest_chunk_bytes {
        bail!(
            "manifest_ack_bytes must not be smaller than manifest_chunk_bytes: {} < {}",
            options.manifest_ack_bytes,
            options.manifest_chunk_bytes
        );
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
                last_work_ordinal: frame.last_work_ordinal,
                compressed_len: self.compressed_len,
                compressed_xxh3_64: self.hasher.digest(),
            }),
        )?;
        self.stream
            .flush()
            .context("failed to flush v3 frame to peer")?;
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
            .context("v3 frame compressed length overflow")?;
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

fn resolve_progressive_bootstrap_window_files_override() -> Option<usize> {
    let raw = env::var(PROGRESSIVE_BOOTSTRAP_WINDOW_FILES_ENV).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    match trimmed.parse::<usize>() {
        Ok(value) => Some(value.clamp(1, PROGRESSIVE_DISCOVERY_WINDOW_FILES)),
        Err(error) => {
            debug!(
                "ignoring invalid {}={:?}: {}",
                PROGRESSIVE_BOOTSTRAP_WINDOW_FILES_ENV, raw, error
            );
            None
        }
    }
}

// В bootstrap-фазе выгодно держать manifest frontier маленьким для file-heavy деревьев,
// но для truly large first file дополнительное сжатие окна не окупается. Поэтому
// по умолчанию оставляем маленький horizon и расширяем его только для явно крупного старта.
fn progressive_bootstrap_window_files(pending: &PendingFrameFiles, policy: &FramePolicy) -> usize {
    if let Some(value) = resolve_progressive_bootstrap_window_files_override() {
        return value;
    }

    let Some(first) = pending.next_expected() else {
        return PROGRESSIVE_BOOTSTRAP_WINDOW_FILES;
    };

    if policy.is_probably_large(first) {
        return PROGRESSIVE_BOOTSTRAP_LARGE_FILE_WINDOW_FILES;
    }

    PROGRESSIVE_BOOTSTRAP_WINDOW_FILES
}

fn progressive_manifest_window_target(
    pending: &PendingFrameFiles,
    options: &SendOptions,
    bootstrap: bool,
) -> usize {
    let policy = FramePolicy::from_frame_target_bytes(options.frame_target_bytes);
    let max_window = if bootstrap {
        progressive_bootstrap_window_files(pending, &policy)
    } else {
        PROGRESSIVE_DISCOVERY_WINDOW_FILES
    };
    policy.max_files_per_frame.clamp(1, max_window)
}

fn discover_progressive_frame_window(
    pending: &mut PendingFrameFiles,
    options: &SendOptions,
    bootstrap: bool,
) -> Result<(usize, bool)> {
    let target = progressive_manifest_window_target(pending, options, bootstrap);
    pending.discover_ahead(target)
}

fn progressive_manifest_target_work_ordinal(
    pending: &PendingFrameFiles,
    next_expected_index: u64,
    options: &SendOptions,
    bootstrap: bool,
) -> u64 {
    pending
        .queued_manifest_target_work_ordinal_with_limit(progressive_manifest_window_target(
            pending, options, bootstrap,
        ))
        .unwrap_or(next_expected_index)
}

fn progressive_manifest_growth_step(
    pending: &PendingFrameFiles,
    target_work_ordinal: u64,
) -> usize {
    let covered = pending.queued_manifest_target_work_ordinal().unwrap_or(0);
    target_work_ordinal
        .saturating_sub(covered)
        .try_into()
        .unwrap_or(PROGRESSIVE_DISCOVERY_WINDOW_FILES)
        .clamp(1, PROGRESSIVE_DISCOVERY_WINDOW_FILES)
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

#[cfg_attr(not(test), allow(dead_code))]
#[allow(clippy::too_many_arguments)]
fn send_streaming_frame<T>(
    stream: &mut T,
    pending: &mut PendingFrameFiles,
    frame_no: u64,
    frame_window: FrameWindow,
    options: &SendOptions,
    readers: &mut ReaderPool,
    profile: Option<&PipelineProfileCollector>,
    retry_journal: &mut StreamRetryJournal,
    timings: &mut SessionTimings,
) -> Result<StreamingFrameAttempt>
where
    T: std::io::Read + std::io::Write,
{
    let _ = prefetch_streaming_files(pending, readers, retry_journal)?;
    let Some(first) = pending.next_expected().cloned() else {
        return Ok(StreamingFrameAttempt::Empty);
    };
    if first.index > frame_window.max_work_ordinal {
        bail!(
            "next pending work ordinal {} exceeds durable manifest boundary {}",
            first.index,
            frame_window.max_work_ordinal
        );
    }

    let build_started = Instant::now();
    let policy = FramePolicy::from_frame_target_bytes(options.frame_target_bytes);
    let mut stats = FrameBuildStats::default();
    let mut progress = FrameProgress::default();
    let mut issues = Vec::new();
    let mut last_completed: Option<InputFile> = None;
    protocol::write_message(
        stream,
        &Message::FrameStart(FrameStart {
            frame_no,
            first_work_ordinal: first.index,
            level: options.level,
            threads: options.threads,
        }),
    )?;
    timings.first_frame_started.get_or_insert_with(Instant::now);

    let mut encoder = zstd::stream::write::Encoder::new(
        ProtocolChunkWriter::new(stream, protocol::MAX_FRAME_CHUNK_BYTES),
        options.level,
    )
    .context("failed to create v3 zstd encoder for sender frame")?;
    encoder
        .multithread(options.threads)
        .context("failed to configure zstd worker threads for v3 sender frame")?;

    let last = loop {
        let file = next_streaming_file(pending)?;
        let stream_retries = retry_journal.attempts_for(file.index);
        if let Some(last) = &last_completed
            && (file.index > frame_window.max_work_ordinal
                || policy.should_close_before_next(progress, &file, stream_retries))
        {
            break last.clone();
        }
        let mut completed_payload = None;
        if stream_retries >= MAX_SNAPSHOT_ATTEMPTS {
            stats.note_skip(FileSkipReason::UnstableAfterRetries, stream_retries);
            issues.push(SendIssue {
                file_index: file.index,
                file_path: file.rel_path().to_owned(),
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
                        file_path: skipped.file.rel_path().to_owned(),
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
            if last.index >= frame_window.max_work_ordinal
                || policy.should_close_after_current(
                    progress,
                    size_at_open,
                    retries,
                    pending.next_expected().is_some()
                        && pending
                            .next_expected()
                            .map(|next| next.index <= frame_window.max_work_ordinal)
                            .unwrap_or(false),
                )
            {
                break last;
            }
        } else if pending
            .next_expected()
            .map(|next| next.index > frame_window.max_work_ordinal)
            .unwrap_or(true)
        {
            break last;
        }
    };

    let finish_started = Instant::now();
    let writer = encoder
        .finish()
        .context("failed to finish v3 sender zstd frame")?;
    if let Some(profile) = profile {
        profile.record_flush();
        profile.record_compress(0, finish_started.elapsed());
    }
    let outbound = OutboundFrame {
        frame_no,
        first_work_ordinal: first.index,
        last_work_ordinal: last.index,
        level: options.level,
        threads: options.threads,
    };
    let (ack, compressed_len, ack_wait) = writer.finish_frame(&outbound)?;
    if let Some(profile) = profile {
        profile.record_ack_wait(ack_wait);
    }
    debug!(
        "sender v3 streaming frame completed: frame_no={} first_work_ordinal={} last_work_ordinal={} raw_file_bytes={} raw_tar_bytes={} compressed_bytes={} durable_manifest_boundary={} elapsed_ms={}",
        outbound.frame_no,
        outbound.first_work_ordinal,
        outbound.last_work_ordinal,
        stats.bytes_sent_raw,
        stats.tar_bytes_sent,
        compressed_len,
        frame_window.max_work_ordinal,
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
#[cfg_attr(not(test), allow(dead_code))]
pub fn send_dump<T>(
    stream: &mut T,
    source: impl Into<SenderSource>,
    dump_id: &str,
    options: &SendOptions,
) -> Result<SendReport>
where
    T: std::io::Read + std::io::Write,
{
    send_dump_impl(stream, source, dump_id, options, None)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn send_dump_profile<T>(
    stream: &mut T,
    source: impl Into<SenderSource>,
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

#[cfg_attr(not(test), allow(dead_code))]
fn send_dump_impl<T>(
    stream: &mut T,
    source: impl Into<SenderSource>,
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
        "sender v3 dump session bootstrapping: dump_id={} level={} threads={} frame_target_bytes={} min_readers={} max_readers={} read_buffer_bytes={} manifest_chunk_bytes={} manifest_ack_records={} manifest_ack_bytes={}",
        dump_id,
        options.level,
        options.threads,
        options.frame_target_bytes,
        options.min_readers,
        options.max_readers,
        options.read_buffer_bytes,
        options.manifest_chunk_bytes,
        options.manifest_ack_records,
        options.manifest_ack_bytes
    );

    let source = source.into();
    let resume_started = Instant::now();
    let remote_resume_state = protocol::request_resume_state(stream, dump_id)?;
    debug!(
        "sender v3 resume state received: dump_id={} manifest_record_seq={:?} manifest_work_ordinal={:?} archive_frame={} archive_work_ordinal={:?} archive_len={} elapsed_ms={}",
        dump_id,
        remote_resume_state.manifest.last_record_seq,
        remote_resume_state.manifest.last_work_ordinal,
        remote_resume_state.archive.last_committed_frame,
        remote_resume_state.archive.last_committed_work_ordinal,
        remote_resume_state.archive.archive_len,
        resume_started.elapsed().as_millis()
    );

    let progressive_fresh_path = can_use_progressive_manifest_path(&remote_resume_state);
    let (resume_state, mut manifest_transfer, walker) = if progressive_fresh_path {
        let resume_state = source::FileResumeState {
            last_committed_frame: remote_resume_state.archive.last_committed_frame,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            archive_len: remote_resume_state.archive.archive_len,
        };
        let walker = PendingFileWalker::new_progressive(
            source.clone(),
            dump_id,
            &resume_state,
            &remote_resume_state.manifest,
            options.manifest_chunk_bytes,
            options.manifest_ack_records,
            profile,
        )?;
        (
            resume_state,
            ManifestTransferStateKind::Progressive(ProgressiveManifestTransferState::new(
                remote_resume_state.manifest.clone(),
            )),
            walker,
        )
    } else {
        let manifest_state = source::ensure_sealed_manifest_state(&source, dump_id, profile)?;
        let recovery = manifest_state.recover_manifest_prefix()?;
        let resume_state = resume_state_from_archive(&recovery.records, &remote_resume_state)?;
        let transfer = prepare_manifest_transfer(
            &recovery.records,
            &remote_resume_state.manifest,
            options.manifest_chunk_bytes,
            options.manifest_ack_records,
        )?;
        let walker = PendingFileWalker::new_with_manifest_state(
            source.clone(),
            &resume_state,
            manifest_state,
            profile,
        )?;
        (
            resume_state,
            ManifestTransferStateKind::Prepared(ManifestTransferState::new(
                remote_resume_state.manifest.clone(),
                transfer,
            )),
            walker,
        )
    };

    let discover_started = Instant::now();
    let mut pending = PendingFrameFiles::new(walker);
    debug!(
        "sender v3 streaming discovery prepared: source={} source_backend={} resume_work_ordinal={} progressive_fresh_path={} elapsed_ms={}",
        source.path().display(),
        source_backend_label(source.kind()),
        resume_state.last_committed_file_index,
        progressive_fresh_path,
        discover_started.elapsed().as_millis()
    );

    let mut sent_frames = 0_usize;
    let mut frame_aborts = 0_u64;
    let mut frame_rebuilds = 0_u64;
    let mut next_frame_no = remote_resume_state.archive.last_committed_frame;
    let mut aggregate = FrameBuildStats::default();
    let mut committed_archive_len = remote_resume_state.archive.archive_len;
    let mut file_issue_journal = Vec::new();
    let mut retry_journal = StreamRetryJournal::default();
    let mut readers = new_reader_pool(options, profile);
    let mut timings = SessionTimings::default();
    let mut progressive_bootstrap = manifest_transfer.is_progressive();
    loop {
        let _ = prefetch_streaming_files(&mut pending, &mut readers, &retry_journal)?;
        if manifest_transfer.is_progressive() {
            let _ =
                discover_progressive_frame_window(&mut pending, options, progressive_bootstrap)?;
        }

        let Some(next_expected) = pending.next_expected().cloned() else {
            break;
        };
        let manifest_target_work_ordinal = if manifest_transfer.is_progressive() {
            progressive_manifest_target_work_ordinal(
                &pending,
                next_expected.index,
                options,
                progressive_bootstrap,
            )
        } else {
            next_expected.index
        };
        manifest_transfer.advance_until_cover(
            stream,
            &mut pending,
            manifest_target_work_ordinal,
            options,
            &mut timings,
        )?;
        let candidate_frame_no = next_frame_no.saturating_add(1);
        let frame_started = Instant::now();
        let frame_pending = pending.clone();
        let frame = send_streaming_frame(
            stream,
            &mut pending,
            candidate_frame_no,
            FrameWindow {
                max_work_ordinal: manifest_transfer.committed_work_ordinal(),
            },
            options,
            &mut readers,
            profile,
            &mut retry_journal,
            &mut timings,
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
                        "v3 abort ack archive length mismatch: expected {} got {}",
                        committed_archive_len,
                        abort_ack.archive_len
                    );
                }
                frame_aborts = frame_aborts.saturating_add(1);
                frame_rebuilds = frame_rebuilds.saturating_add(1);
                debug!(
                    "sender v3 frame aborted and will rebuild: frame_no={} work_ordinal={} file_path={} attempts={} archive_len={}",
                    candidate_frame_no,
                    failed_file.index,
                    failed_file.rel_path(),
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
            "sender v3 frame acknowledged: frame_no={} archive_len={} ack_last_work_ordinal={} discovered_boundary={}#{} manifest_boundary={} frame_ms={} frame_bytes_per_sec={}",
            frame.ack.frame_no,
            frame.ack.archive_len,
            frame.ack.last_work_ordinal,
            discovered_index,
            discovered_path,
            manifest_transfer.committed_work_ordinal(),
            frame_elapsed.as_millis(),
            bytes_per_sec(frame.compressed_len, frame_elapsed)
        );
        merge_frame_stats(&mut aggregate, &frame.stats);
        file_issue_journal.extend(frame.issues);
        committed_archive_len = frame.ack.archive_len;
        sent_frames += 1;
        next_frame_no = frame.outbound.frame_no;
        progressive_bootstrap = false;
    }
    readers.finish();

    manifest_transfer.finish(stream, &mut pending, options, &mut timings)?;

    let (final_file_index, final_file_path) = pending.final_boundary()?;
    let files_total_discovered = final_file_index;
    let discovery_issue_summary = pending.discovery_issue_summary();
    let discovery_issue_journal = pending
        .discovery_issues()
        .into_iter()
        .map(send_discovery_issue)
        .collect::<Vec<_>>();

    if !file_issue_journal.is_empty() || !discovery_issue_journal.is_empty() {
        let issue_chunks =
            send_issue_journal(stream, &file_issue_journal, &discovery_issue_journal)?;
        debug!(
            "sender uploaded v3 issue journal: dump_id={} file_issues={} discovery_issues={} chunks={}",
            dump_id,
            file_issue_journal.len(),
            discovery_issue_journal.len(),
            issue_chunks
        );
    }

    let finish_started = Instant::now();
    send_finish(stream, next_frame_no, Some(final_file_index))?;
    if let Err(error) = pending.cleanup_after_finish() {
        warn!(
            "sender manifest cleanup failed after successful v3 finish: dump_id={} error={}",
            dump_id, error
        );
    }
    let final_archive_bytes = committed_archive_len.saturating_add(final_archive_footer_bytes()?);
    let status = if aggregate.files_sent_best_effort == 0
        && aggregate.files_skipped_open_failed == 0
        && aggregate.files_skipped_unstable_after_retries == 0
        && discovery_issue_summary.discovery_issues_total == 0
    {
        SendStatus::Clean
    } else {
        SendStatus::CompletedWithWarnings
    };
    debug!(
        "sender v3 finish acknowledged: final_frame_no={} final_work_ordinal={} elapsed_ms={}",
        next_frame_no,
        final_file_index,
        finish_started.elapsed().as_millis()
    );
    debug!(
        "sender v3 dump session completed: dump_id={} sent_frames={} pending_files={} files_sent_clean={} files_sent_best_effort={} files_skipped_open_failed={} files_skipped_unstable_after_retries={} discovery_issues_total={} subtrees_skipped={} manifest_resets_total={} bytes_sent_raw={} manifest_payload_bytes_sent={} total_elapsed_ms={}",
        dump_id,
        sent_frames,
        files_total_discovered.saturating_sub(resume_state.last_committed_file_index),
        aggregate.files_sent_clean,
        aggregate.files_sent_best_effort,
        aggregate.files_skipped_open_failed,
        aggregate.files_skipped_unstable_after_retries,
        discovery_issue_summary.discovery_issues_total,
        discovery_issue_summary.subtrees_skipped,
        discovery_issue_summary.manifest_resets_total,
        aggregate.bytes_sent_raw,
        manifest_transfer.payload_bytes_sent(),
        session_started.elapsed().as_millis()
    );
    if let Some(profile) = profile {
        if let (Some(started), Some(finished)) = (
            timings.manifest_transfer_started,
            timings.manifest_transfer_finished,
        ) {
            profile.record_manifest_transfer(
                manifest_transfer.payload_bytes_sent(),
                finished.saturating_duration_since(started),
            );
        }
        if let Some(first_frame_started) = timings.first_frame_started {
            profile.record_time_to_first_frame(
                first_frame_started.saturating_duration_since(session_started),
            );
        }
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
        discovery_issues_total: discovery_issue_summary.discovery_issues_total,
        subtrees_skipped: discovery_issue_summary.subtrees_skipped,
        manifest_resets_total: discovery_issue_summary.manifest_resets_total,
        retries_total: aggregate.retries_total,
        bytes_sent_raw: aggregate.bytes_sent_raw,
        bytes_sent_best_effort_raw: aggregate.bytes_sent_best_effort_raw,
        final_archive_bytes,
        issue_journal: file_issue_journal,
        discovery_issue_journal,
    })
}

fn source_backend_label(kind: SenderSourceKind) -> &'static str {
    match kind {
        SenderSourceKind::OrdinaryPath => "ordinary_path",
        SenderSourceKind::SnapshotPath => "snapshot_path",
        SenderSourceKind::PinnedPath => "pinned_path",
    }
}

// Sender отправляет один целый frame и ждёт Ack ровно на него.
// Пока Ack не пришёл, выше по стеку нельзя считать frame committed.
#[cfg_attr(not(test), allow(dead_code))]
pub fn send_frame<T, I, B>(stream: &mut T, frame: &OutboundFrame, chunks: I) -> Result<Ack>
where
    T: std::io::Read + std::io::Write,
    I: IntoIterator<Item = B>,
    B: AsRef<[u8]>,
{
    if frame.threads == 0 {
        bail!("v3 frame threads must be at least 1");
    }
    if frame.last_work_ordinal < frame.first_work_ordinal {
        bail!("v3 frame last work ordinal must not be smaller than first work ordinal");
    }

    protocol::write_message(
        stream,
        &Message::FrameStart(FrameStart {
            frame_no: frame.frame_no,
            first_work_ordinal: frame.first_work_ordinal,
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
            .context("v3 frame compressed length overflow")?;
    }

    protocol::write_message(
        stream,
        &Message::FrameEnd(FrameEnd {
            frame_no: frame.frame_no,
            last_work_ordinal: frame.last_work_ordinal,
            compressed_len,
            compressed_xxh3_64: hasher.digest(),
        }),
    )?;
    stream.flush().context("failed to flush v3 frame to peer")?;

    read_frame_ack(stream, frame)
}

#[cfg_attr(not(test), allow(dead_code))]
fn read_frame_ack<T>(stream: &mut T, frame: &OutboundFrame) -> Result<Ack>
where
    T: std::io::Read + std::io::Write,
{
    let ack = match protocol::read_message(stream)? {
        Message::Ack(ack) => ack,
        Message::Error(error) => {
            bail!(
                "peer rejected v3 frame {}: {}",
                frame.frame_no,
                error.message
            )
        }
        unexpected => bail!(
            "unexpected v3 message while waiting for frame ack: {:?}",
            unexpected.kind()
        ),
    };

    if ack.frame_no != frame.frame_no {
        bail!(
            "v3 ack frame number mismatch: expected {} got {}",
            frame.frame_no,
            ack.frame_no
        );
    }
    if ack.last_work_ordinal != frame.last_work_ordinal {
        bail!(
            "v3 ack last work ordinal mismatch: expected {} got {}",
            frame.last_work_ordinal,
            ack.last_work_ordinal
        );
    }

    Ok(ack)
}

// После отправки последнего committed frame sender закрывает dump отдельным
// Finish-сообщением и ждёт FinishAck. Это отделяет frame commit от завершения
// всей сессии и сохраняет resume-модель по границе кадра.
#[cfg_attr(not(test), allow(dead_code))]
pub fn send_finish<T>(
    stream: &mut T,
    final_frame_no: u64,
    final_work_ordinal: Option<u64>,
) -> Result<Finish>
where
    T: std::io::Read + std::io::Write,
{
    let finish = Finish {
        final_frame_no,
        final_work_ordinal,
    };
    protocol::write_message(stream, &Message::Finish(finish.clone()))?;
    stream
        .flush()
        .context("failed to flush v3 finish message to peer")?;

    match protocol::read_message(stream)? {
        Message::FinishAck => Ok(finish),
        Message::Error(error) => bail!("peer rejected v3 finish: {}", error.message),
        unexpected => bail!(
            "unexpected v3 message while waiting for finish ack: {:?}",
            unexpected.kind()
        ),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
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
        .context("failed to flush v3 frame abort to peer")?;

    let abort_ack = match protocol::read_message(stream)? {
        Message::AbortAck(abort_ack) => abort_ack,
        Message::Error(error) => {
            bail!(
                "peer rejected v3 frame abort {}: {}",
                frame_no,
                error.message
            )
        }
        unexpected => bail!(
            "unexpected v3 message while waiting for abort ack: {:?}",
            unexpected.kind()
        ),
    };
    if abort_ack.frame_no != frame_no {
        bail!(
            "v3 abort ack frame number mismatch: expected {} got {}",
            frame_no,
            abort_ack.frame_no
        );
    }

    Ok(abort_ack)
}
