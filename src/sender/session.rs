use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use log::{debug, warn};
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::{
    self, AbortAck, Ack, AckV3, Finish, FinishV3, FrameChunk, FrameEnd, FrameEndV3, FrameStart,
    FrameStartV3, IssueChunkV3, IssueCodeV3, IssueEntryV3, IssueJournalChunk, IssueJournalEntry,
    IssueJournalReason, IssueScopeV3, ManifestAck, ManifestBoundaryV3, ManifestChunk, ManifestSeal,
    ManifestStart, Message, ResumeState, ResumeStateV3, V3Message,
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
    FrameBuildStats, InputFile, OutboundFrame, OutboundFrameV3, PendingFileWalker,
    PendingFrameFiles, PipelineProfile, PipelineProfileCollector, ReaderPool,
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

fn issue_scope_v3(scope: SendDiscoveryIssueScope) -> IssueScopeV3 {
    match scope {
        SendDiscoveryIssueScope::Entry => IssueScopeV3::DiscoveryEntry,
        SendDiscoveryIssueScope::Subtree => IssueScopeV3::DiscoverySubtree,
        SendDiscoveryIssueScope::SenderState => IssueScopeV3::SenderState,
    }
}

fn issue_code_v3(reason: &SendIssueReason) -> IssueCodeV3 {
    match reason {
        SendIssueReason::OpenFailed => IssueCodeV3::OpenFailed,
        SendIssueReason::UnstableAfterRetries => IssueCodeV3::UnstableAfterRetries,
    }
}

fn discovery_issue_code_v3(reason: &SendDiscoveryIssueReason) -> IssueCodeV3 {
    match reason {
        SendDiscoveryIssueReason::UnsupportedEntry { .. } => IssueCodeV3::UnsupportedEntry,
        SendDiscoveryIssueReason::UnreadableSymlinkTarget { .. } => {
            IssueCodeV3::UnreadableSymlinkTarget
        }
        SendDiscoveryIssueReason::VirtualFilesystem { .. } => IssueCodeV3::VirtualFilesystem,
        SendDiscoveryIssueReason::Io { .. } => IssueCodeV3::Io,
        SendDiscoveryIssueReason::SenderState { .. } => IssueCodeV3::SenderStateEvent,
        SendDiscoveryIssueReason::ReadDirFailed { .. } => IssueCodeV3::ReadDirFailed,
        SendDiscoveryIssueReason::SourceInvalidated { .. } => IssueCodeV3::SourceInvalidated,
    }
}

fn typed_issue_entry_v3(issue: &SendIssue) -> IssueEntryV3 {
    IssueEntryV3 {
        scope: IssueScopeV3::File,
        record_seq: None,
        work_ordinal: Some(issue.file_index),
        path: issue.file_path.clone(),
        attempts: issue.attempts,
        code: issue_code_v3(&issue.reason),
        detail: issue.last_error.clone(),
        context: String::new(),
    }
}

fn typed_discovery_issue_entry_v3(issue: &SendDiscoveryIssue) -> IssueEntryV3 {
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
    IssueEntryV3 {
        scope: issue_scope_v3(issue.scope),
        record_seq: issue.record_seq,
        work_ordinal: None,
        path: issue.path.clone(),
        attempts: 0,
        code: discovery_issue_code_v3(&issue.reason),
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

#[cfg_attr(not(test), allow(dead_code))]
fn send_issue_journal_v3<T>(
    stream: &mut T,
    file_issues: &[SendIssue],
    discovery_issues: &[SendDiscoveryIssue],
) -> Result<usize>
where
    T: Write,
{
    let mut chunks_sent = 0_usize;
    let mut pending = Vec::<IssueEntryV3>::new();
    let push_entry = |entry: IssueEntryV3,
                      stream: &mut T,
                      pending: &mut Vec<IssueEntryV3>,
                      chunks_sent: &mut usize|
     -> Result<()> {
        let mut candidate = pending.clone();
        candidate.push(entry.clone());
        let candidate_message = V3Message::IssueChunk(IssueChunkV3 { entries: candidate });
        if protocol::write_message_v3(&mut Vec::new(), &candidate_message).is_ok() {
            pending.push(entry);
            return Ok(());
        }
        if pending.is_empty() {
            protocol::write_message_v3(stream, &candidate_message)
                .context("typed v3 issue entry does not fit into a single protocol chunk")?;
            *chunks_sent = chunks_sent.saturating_add(1);
            return Ok(());
        }

        protocol::write_message_v3(
            stream,
            &V3Message::IssueChunk(IssueChunkV3 {
                entries: std::mem::take(pending),
            }),
        )?;
        *chunks_sent = chunks_sent.saturating_add(1);
        pending.push(entry);
        Ok(())
    };

    for issue in file_issues {
        push_entry(
            typed_issue_entry_v3(issue),
            stream,
            &mut pending,
            &mut chunks_sent,
        )?;
    }
    for issue in discovery_issues {
        push_entry(
            typed_discovery_issue_entry_v3(issue),
            stream,
            &mut pending,
            &mut chunks_sent,
        )?;
    }

    if !pending.is_empty() {
        protocol::write_message_v3(
            stream,
            &V3Message::IssueChunk(IssueChunkV3 { entries: pending }),
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
struct ManifestTransferStateV3 {
    base_manifest: ManifestBoundaryV3,
    transfer: PreparedManifestTransfer,
    committed: ManifestBoundaryV3,
    next_chunk_index: usize,
    unacked_bytes: usize,
    unacked_records: u64,
    started: bool,
    seal_sent: bool,
    payload_bytes_sent: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct V3SessionTimings {
    manifest_transfer_started: Option<Instant>,
    manifest_transfer_finished: Option<Instant>,
    first_frame_started: Option<Instant>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct V3FrameWindow {
    max_work_ordinal: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
fn prepare_manifest_transfer(
    records: &[source::manifest::ManifestRecord],
    base_manifest: &ManifestBoundaryV3,
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
impl ManifestTransferStateV3 {
    fn new(base_manifest: ManifestBoundaryV3, transfer: PreparedManifestTransfer) -> Self {
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

    fn start_if_needed<T>(&mut self, stream: &mut T, timings: &mut V3SessionTimings) -> Result<()>
    where
        T: std::io::Write,
    {
        if self.started {
            return Ok(());
        }
        protocol::write_message_v3(
            stream,
            &V3Message::ManifestStart(ManifestStart {
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
        timings: &mut V3SessionTimings,
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
            protocol::write_message_v3(
                stream,
                &V3Message::ManifestChunk(ManifestChunk {
                    bytes: chunk.bytes.clone(),
                    last_record_seq: chunk.last_record_seq,
                    last_work_ordinal: chunk.last_work_ordinal,
                }),
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
        timings: &mut V3SessionTimings,
    ) -> Result<ManifestBoundaryV3>
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
            protocol::write_message_v3(
                stream,
                &V3Message::ManifestChunk(ManifestChunk {
                    bytes: chunk.bytes.clone(),
                    last_record_seq: chunk.last_record_seq,
                    last_work_ordinal: chunk.last_work_ordinal,
                }),
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
            protocol::write_message_v3(
                stream,
                &V3Message::ManifestSeal(ManifestSeal {
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
        timings: &mut V3SessionTimings,
    ) -> Result<()>
    where
        T: std::io::Read,
    {
        if self.unacked_bytes == 0 && self.unacked_records == 0 && !require_sealed {
            return Ok(());
        }
        self.committed = read_manifest_ack_v3(
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
fn manifest_relative_path_v3(path_stack: &[Box<str>], leaf_name: &str) -> String {
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
fn manifest_path_for_work_ordinal_v3(
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
                    return Ok(manifest_relative_path_v3(
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
fn resume_state_from_v3_archive(
    records: &[source::manifest::ManifestRecord],
    resume_state_v3: &ResumeStateV3,
) -> Result<ResumeState> {
    let archive = &resume_state_v3.archive;
    let last_committed_file_index = archive.last_committed_work_ordinal.unwrap_or(0);
    let last_committed_file_path = if last_committed_file_index == 0 {
        String::new()
    } else {
        manifest_path_for_work_ordinal_v3(records, last_committed_file_index)?
    };
    if archive.last_committed_work_ordinal.unwrap_or(0)
        > resume_state_v3.manifest.last_work_ordinal.unwrap_or(0)
    {
        bail!(
            "receiver archive boundary {:?} is ahead of durable manifest boundary {:?}",
            archive.last_committed_work_ordinal,
            resume_state_v3.manifest.last_work_ordinal
        );
    }

    Ok(ResumeState {
        last_committed_frame: archive.last_committed_frame,
        last_committed_file_index,
        last_committed_file_path,
        archive_len: archive.archive_len,
    })
}

#[cfg_attr(not(test), allow(dead_code))]
fn read_manifest_ack_v3<T>(
    stream: &mut T,
    current: &ManifestBoundaryV3,
    final_record_seq: Option<u64>,
    final_work_ordinal: Option<u64>,
    require_sealed: bool,
) -> Result<ManifestBoundaryV3>
where
    T: std::io::Read,
{
    match protocol::read_message_v3(stream)? {
        V3Message::ManifestAck(ManifestAck { committed_manifest }) => {
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
        V3Message::Error(error) => bail!("peer rejected v3 manifest transfer: {}", error.message),
        unexpected => bail!(
            "unexpected message while waiting for v3 manifest ack: {:?}",
            unexpected.kind()
        ),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn send_manifest_v3<T>(
    stream: &mut T,
    source: &SenderSource,
    dump_id: &str,
    base_manifest: &ManifestBoundaryV3,
    options: &SendOptions,
) -> Result<ManifestBoundaryV3>
where
    T: std::io::Read + std::io::Write,
{
    validate_send_options(options)?;

    let manifest_state = source::ensure_sealed_manifest_state(source, dump_id, None)?;
    let log_bytes = fs::read(&manifest_state.paths.log_path).with_context(|| {
        format!(
            "failed to read sender manifest spool {}",
            manifest_state.paths.log_path.display()
        )
    })?;
    let recovery = source::manifest::ManifestSpoolReader::recover_prefix_bytes(&log_bytes)?;
    if recovery.truncate_to_len != log_bytes.len() as u64 {
        bail!(
            "sender sealed manifest spool ended with a torn tail: {} < {} bytes",
            recovery.truncate_to_len,
            log_bytes.len()
        );
    }

    let chunk_bytes = options.manifest_chunk_bytes.min(options.manifest_ack_bytes);
    let transfer = prepare_manifest_transfer(
        &recovery.records,
        base_manifest,
        chunk_bytes,
        options.manifest_ack_records,
    )?;
    debug!(
        "sender v3 manifest transfer prepared: dump_id={} base_record_seq={:?} final_record_seq={:?} chunk_bytes={} ack_records={} ack_bytes={} chunks={}",
        dump_id,
        base_manifest.last_record_seq,
        transfer.final_record_seq,
        chunk_bytes,
        options.manifest_ack_records,
        options.manifest_ack_bytes,
        transfer.chunks.len()
    );

    protocol::write_message_v3(
        stream,
        &V3Message::ManifestStart(ManifestStart {
            base_manifest: base_manifest.clone(),
        }),
    )?;

    if base_manifest.sealed
        && base_manifest.last_record_seq == transfer.final_record_seq
        && base_manifest.last_work_ordinal == transfer.final_work_ordinal
    {
        return Ok(base_manifest.clone());
    }

    let mut committed = base_manifest.clone();
    let mut unacked_bytes = 0_usize;
    let mut unacked_records = 0_u64;
    for chunk in &transfer.chunks {
        protocol::write_message_v3(
            stream,
            &V3Message::ManifestChunk(ManifestChunk {
                bytes: chunk.bytes.clone(),
                last_record_seq: chunk.last_record_seq,
                last_work_ordinal: chunk.last_work_ordinal,
            }),
        )?;
        unacked_bytes = unacked_bytes.saturating_add(chunk.bytes.len());
        unacked_records = unacked_records.saturating_add(chunk.record_count);
        if unacked_bytes >= options.manifest_ack_bytes
            || unacked_records >= options.manifest_ack_records
        {
            committed = read_manifest_ack_v3(
                stream,
                &committed,
                transfer.final_record_seq,
                transfer.final_work_ordinal,
                false,
            )?;
            unacked_bytes = 0;
            unacked_records = 0;
        }
    }

    protocol::write_message_v3(
        stream,
        &V3Message::ManifestSeal(ManifestSeal {
            final_record_seq: transfer.final_record_seq,
            final_work_ordinal: transfer.final_work_ordinal,
        }),
    )?;
    read_manifest_ack_v3(
        stream,
        &committed,
        transfer.final_record_seq,
        transfer.final_work_ordinal,
        true,
    )
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
    if options.manifest_chunk_bytes > protocol::MAX_MANIFEST_CHUNK_BYTES {
        bail!(
            "manifest_chunk_bytes exceeds protocol cap: {} > {} bytes",
            options.manifest_chunk_bytes,
            protocol::MAX_MANIFEST_CHUNK_BYTES
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

struct StreamingFrameResultV3 {
    outbound: OutboundFrameV3,
    stats: FrameBuildStats,
    issues: Vec<SendIssue>,
    ack: AckV3,
    compressed_len: u64,
}

enum StreamingFrameAttemptV3 {
    Empty,
    Committed(StreamingFrameResultV3),
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

struct ProtocolChunkWriterV3<'a, T> {
    stream: &'a mut T,
    chunk_limit: usize,
    buffer: Vec<u8>,
    compressed_len: u64,
    hasher: Xxh3Default,
}

impl<'a, T> ProtocolChunkWriterV3<'a, T>
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

    fn finish_frame(mut self, frame: &OutboundFrameV3) -> Result<(AckV3, u64, Duration)> {
        self.flush_chunk()?;
        protocol::write_message_v3(
            self.stream,
            &V3Message::FrameEnd(FrameEndV3 {
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
        let ack = read_frame_ack_v3(self.stream, frame)?;
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
        protocol::write_message_v3(
            self.stream,
            &V3Message::FrameChunk(FrameChunk {
                bytes: std::mem::take(&mut self.buffer),
            }),
        )?;
        self.buffer = Vec::with_capacity(self.chunk_limit);
        Ok(())
    }
}

impl<T> Write for ProtocolChunkWriterV3<'_, T>
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
        first_file_path: first.rel_path().to_owned(),
        last_file_index: first.index,
        last_file_path: first.rel_path().to_owned(),
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
        last_file_path: last.rel_path().to_owned(),
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

#[cfg_attr(not(test), allow(dead_code))]
#[allow(clippy::too_many_arguments)]
fn send_streaming_frame_v3<T>(
    stream: &mut T,
    pending: &mut PendingFrameFiles,
    frame_no: u64,
    frame_window: V3FrameWindow,
    options: &SendOptions,
    readers: &mut ReaderPool,
    profile: Option<&PipelineProfileCollector>,
    retry_journal: &mut StreamRetryJournal,
    timings: &mut V3SessionTimings,
) -> Result<StreamingFrameAttemptV3>
where
    T: std::io::Read + std::io::Write,
{
    let _ = prefetch_streaming_files(pending, readers, retry_journal)?;
    let Some(first) = pending.next_expected().cloned() else {
        return Ok(StreamingFrameAttemptV3::Empty);
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
    protocol::write_message_v3(
        stream,
        &V3Message::FrameStart(FrameStartV3 {
            frame_no,
            first_work_ordinal: first.index,
            level: options.level,
            threads: options.threads,
        }),
    )?;
    timings.first_frame_started.get_or_insert_with(Instant::now);

    let mut encoder = zstd::stream::write::Encoder::new(
        ProtocolChunkWriterV3::new(stream, protocol::MAX_FRAME_CHUNK_BYTES),
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
                            return Ok(StreamingFrameAttemptV3::Aborted {
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
                            return Ok(StreamingFrameAttemptV3::Aborted {
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
    let outbound = OutboundFrameV3 {
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
    Ok(StreamingFrameAttemptV3::Committed(StreamingFrameResultV3 {
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
    source: impl Into<SenderSource>,
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
pub fn send_dump_v3<T>(
    stream: &mut T,
    source: impl Into<SenderSource>,
    dump_id: &str,
    options: &SendOptions,
) -> Result<SendReport>
where
    T: std::io::Read + std::io::Write,
{
    send_dump_impl_v3(stream, source, dump_id, options, None)
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn send_dump_profile_v3<T>(
    stream: &mut T,
    source: impl Into<SenderSource>,
    dump_id: &str,
    options: &SendOptions,
) -> Result<(SendReport, PipelineProfile)>
where
    T: std::io::Read + std::io::Write,
{
    let profile = PipelineProfileCollector::default();
    let report = send_dump_impl_v3(stream, source, dump_id, options, Some(&profile))?;
    Ok((report, profile.finish()))
}

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
        "sender dump session bootstrapping: dump_id={} level={} threads={} frame_target_bytes={} min_readers={} max_readers={} read_buffer_bytes={} manifest_chunk_bytes={} manifest_ack_records={} manifest_ack_bytes={}",
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
    let walker =
        PendingFileWalker::new_with_manifest(source.clone(), dump_id, &resume_state, profile)?;
    let mut pending = PendingFrameFiles::new(walker);
    debug!(
        "sender streaming discovery prepared: source={} source_backend={} resume_file_index={} elapsed_ms={}",
        source.path().display(),
        source_backend_label(source.kind()),
        resume_state.last_committed_file_index,
        discover_started.elapsed().as_millis()
    );

    let mut sent_frames = 0_usize;
    let mut frame_aborts = 0_u64;
    let mut frame_rebuilds = 0_u64;
    let mut next_frame_no = resume_state.last_committed_frame;
    let mut aggregate = FrameBuildStats::default();
    let mut committed_archive_len = resume_state.archive_len;
    let mut file_issue_journal = Vec::new();
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
        file_issue_journal.extend(frame.issues);
        committed_archive_len = frame.ack.archive_len;
        sent_frames += 1;
        next_frame_no = frame.outbound.frame_no;
    }
    readers.finish();
    let (final_file_index, final_file_path) = pending.final_boundary()?;
    let files_total_discovered = final_file_index;
    let discovery_issue_summary = pending.discovery_issue_summary();
    let discovery_issue_journal = pending
        .discovery_issues()
        .into_iter()
        .map(send_discovery_issue)
        .collect::<Vec<_>>();

    if !file_issue_journal.is_empty() {
        let issue_chunks = send_issue_journal(stream, &file_issue_journal)?;
        debug!(
            "sender uploaded file issue journal: dump_id={} issues={} chunks={}",
            dump_id,
            file_issue_journal.len(),
            issue_chunks
        );
    }

    let finish_started = Instant::now();
    send_finish(stream, next_frame_no, final_file_index, &final_file_path)?;
    if let Err(error) = pending.cleanup_after_finish() {
        warn!(
            "sender manifest cleanup failed after successful finish: dump_id={} error={}",
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
        "sender finish acknowledged: final_frame_no={} final_file_index={} final_file_path={} elapsed_ms={}",
        next_frame_no,
        final_file_index,
        final_file_path,
        finish_started.elapsed().as_millis()
    );
    debug!(
        "sender dump session completed: dump_id={} sent_frames={} pending_files={} files_sent_clean={} files_sent_best_effort={} files_skipped_open_failed={} files_skipped_unstable_after_retries={} discovery_issues_total={} subtrees_skipped={} manifest_resets_total={} bytes_sent_raw={} total_elapsed_ms={}",
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

#[cfg_attr(not(test), allow(dead_code))]
fn send_dump_impl_v3<T>(
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
    let resume_state_v3 = protocol::request_resume_state_v3(stream, dump_id)?;
    debug!(
        "sender v3 resume state received: dump_id={} manifest_record_seq={:?} manifest_work_ordinal={:?} archive_frame={} archive_work_ordinal={:?} archive_len={} elapsed_ms={}",
        dump_id,
        resume_state_v3.manifest.last_record_seq,
        resume_state_v3.manifest.last_work_ordinal,
        resume_state_v3.archive.last_committed_frame,
        resume_state_v3.archive.last_committed_work_ordinal,
        resume_state_v3.archive.archive_len,
        resume_started.elapsed().as_millis()
    );

    let manifest_state = source::ensure_sealed_manifest_state(&source, dump_id, profile)?;
    let log_bytes = fs::read(&manifest_state.paths.log_path).with_context(|| {
        format!(
            "failed to read sender manifest spool {}",
            manifest_state.paths.log_path.display()
        )
    })?;
    let recovery = source::manifest::ManifestSpoolReader::recover_prefix_bytes(&log_bytes)?;
    if recovery.truncate_to_len != log_bytes.len() as u64 {
        bail!(
            "sender sealed manifest spool ended with a torn tail: {} < {} bytes",
            recovery.truncate_to_len,
            log_bytes.len()
        );
    }
    let resume_state = resume_state_from_v3_archive(&recovery.records, &resume_state_v3)?;
    let transfer = prepare_manifest_transfer(
        &recovery.records,
        &resume_state_v3.manifest,
        options.manifest_chunk_bytes,
        options.manifest_ack_records,
    )?;
    let mut manifest_transfer =
        ManifestTransferStateV3::new(resume_state_v3.manifest.clone(), transfer);

    let discover_started = Instant::now();
    let walker =
        PendingFileWalker::new_with_manifest(source.clone(), dump_id, &resume_state, profile)?;
    let mut pending = PendingFrameFiles::new(walker);
    debug!(
        "sender v3 streaming discovery prepared: source={} source_backend={} resume_work_ordinal={} elapsed_ms={}",
        source.path().display(),
        source_backend_label(source.kind()),
        resume_state.last_committed_file_index,
        discover_started.elapsed().as_millis()
    );

    let mut sent_frames = 0_usize;
    let mut frame_aborts = 0_u64;
    let mut frame_rebuilds = 0_u64;
    let mut next_frame_no = resume_state_v3.archive.last_committed_frame;
    let mut aggregate = FrameBuildStats::default();
    let mut committed_archive_len = resume_state_v3.archive.archive_len;
    let mut file_issue_journal = Vec::new();
    let mut retry_journal = StreamRetryJournal::default();
    let mut readers = new_reader_pool(options, profile);
    let mut timings = V3SessionTimings::default();
    loop {
        let _ = prefetch_streaming_files(&mut pending, &mut readers, &retry_journal)?;
        let Some(next_expected) = pending.next_expected().cloned() else {
            break;
        };
        manifest_transfer.advance_until_cover(
            stream,
            next_expected.index,
            options,
            &mut timings,
        )?;
        let candidate_frame_no = next_frame_no.saturating_add(1);
        let frame_started = Instant::now();
        let frame_pending = pending.clone();
        let frame = send_streaming_frame_v3(
            stream,
            &mut pending,
            candidate_frame_no,
            V3FrameWindow {
                max_work_ordinal: manifest_transfer.committed_work_ordinal(),
            },
            options,
            &mut readers,
            profile,
            &mut retry_journal,
            &mut timings,
        )?;
        let frame = match frame {
            StreamingFrameAttemptV3::Empty => break,
            StreamingFrameAttemptV3::Committed(frame) => frame,
            StreamingFrameAttemptV3::Aborted {
                failed_file,
                last_error,
            } => {
                let attempts = retry_journal.note_abort(failed_file.index, last_error);
                let abort_ack = send_frame_abort_v3(stream, candidate_frame_no)?;
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
    }
    readers.finish();

    manifest_transfer.finish(stream, options, &mut timings)?;

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
            send_issue_journal_v3(stream, &file_issue_journal, &discovery_issue_journal)?;
        debug!(
            "sender uploaded v3 issue journal: dump_id={} file_issues={} discovery_issues={} chunks={}",
            dump_id,
            file_issue_journal.len(),
            discovery_issue_journal.len(),
            issue_chunks
        );
    }

    let finish_started = Instant::now();
    send_finish_v3(stream, next_frame_no, Some(final_file_index))?;
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
        manifest_transfer.payload_bytes_sent,
        session_started.elapsed().as_millis()
    );
    if let Some(profile) = profile {
        if let (Some(started), Some(finished)) = (
            timings.manifest_transfer_started,
            timings.manifest_transfer_finished,
        ) {
            profile.record_manifest_transfer(
                manifest_transfer.payload_bytes_sent,
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

#[cfg_attr(not(test), allow(dead_code))]
pub fn send_frame_v3<T, I, B>(stream: &mut T, frame: &OutboundFrameV3, chunks: I) -> Result<AckV3>
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

    protocol::write_message_v3(
        stream,
        &V3Message::FrameStart(FrameStartV3 {
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
        protocol::write_message_v3(
            stream,
            &V3Message::FrameChunk(FrameChunk {
                bytes: bytes.to_vec(),
            }),
        )?;
        hasher.update(bytes);
        compressed_len = compressed_len
            .checked_add(bytes.len() as u64)
            .context("v3 frame compressed length overflow")?;
    }

    protocol::write_message_v3(
        stream,
        &V3Message::FrameEnd(FrameEndV3 {
            frame_no: frame.frame_no,
            last_work_ordinal: frame.last_work_ordinal,
            compressed_len,
            compressed_xxh3_64: hasher.digest(),
        }),
    )?;
    stream.flush().context("failed to flush v3 frame to peer")?;

    read_frame_ack_v3(stream, frame)
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

#[cfg_attr(not(test), allow(dead_code))]
fn read_frame_ack_v3<T>(stream: &mut T, frame: &OutboundFrameV3) -> Result<AckV3>
where
    T: std::io::Read + std::io::Write,
{
    let ack = match protocol::read_message_v3(stream)? {
        V3Message::Ack(ack) => ack,
        V3Message::Error(error) => {
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

#[cfg_attr(not(test), allow(dead_code))]
pub fn send_finish_v3<T>(
    stream: &mut T,
    final_frame_no: u64,
    final_work_ordinal: Option<u64>,
) -> Result<FinishV3>
where
    T: std::io::Read + std::io::Write,
{
    let finish = FinishV3 {
        final_frame_no,
        final_work_ordinal,
    };
    protocol::write_message_v3(stream, &V3Message::Finish(finish.clone()))?;
    stream
        .flush()
        .context("failed to flush v3 finish message to peer")?;

    match protocol::read_message_v3(stream)? {
        V3Message::FinishAck => Ok(finish),
        V3Message::Error(error) => bail!("peer rejected v3 finish: {}", error.message),
        unexpected => bail!(
            "unexpected v3 message while waiting for finish ack: {:?}",
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

#[cfg_attr(not(test), allow(dead_code))]
pub fn send_frame_abort_v3<T>(stream: &mut T, frame_no: u64) -> Result<AbortAck>
where
    T: std::io::Read + std::io::Write,
{
    protocol::write_message_v3(
        stream,
        &V3Message::FrameAbort(protocol::FrameAbort { frame_no }),
    )?;
    stream
        .flush()
        .context("failed to flush v3 frame abort to peer")?;

    let abort_ack = match protocol::read_message_v3(stream)? {
        V3Message::AbortAck(abort_ack) => abort_ack,
        V3Message::Error(error) => {
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

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::{self, Cursor, Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{
        OutboundFrame, OutboundFrameV3, SendOptions, SendStatus, send_dump, send_finish,
        send_finish_v3, send_frame, send_frame_abort, send_frame_v3,
    };
    use crate::network::protocol::{
        self, AbortAck, Ack, AckV3, ErrorMessage, FrameEndV3, FrameStartV3, IssueJournalChunk,
        IssueJournalReason, ManifestAck, ManifestBoundaryV3, ManifestChunk, ManifestSeal,
        ManifestStart, Message, ResumeState, V3Message,
    };
    use crate::sender::source::manifest::{
        ManifestMeta, ManifestRecord, ManifestRecordKind, ManifestRecordPayload, ManifestRecordSeq,
        ManifestSourceIdentity, ManifestSourceKind, ManifestSpoolPaths, ManifestSpoolReader,
        ManifestSpoolWriter, ManifestSubtreeSkipReason, ManifestWorkOrdinal,
    };
    use crate::sender::{PendingFileWalker, PendingFrameFiles};
    use xxhash_rust::xxh3::Xxh3Default;

    const TEST_MANIFEST_BUILDER_VERSION: &str = "manifest-builder-v3-live-v1";

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

    fn encode_message_v3(message: &V3Message) -> Vec<u8> {
        let mut bytes = Vec::new();
        protocol::write_message_v3(&mut bytes, message).unwrap();
        bytes
    }

    fn decode_messages_v3(bytes: &[u8]) -> Vec<V3Message> {
        let mut cursor = Cursor::new(bytes.to_vec());
        let mut messages = Vec::new();
        while (cursor.position() as usize) < bytes.len() {
            messages.push(protocol::read_message_v3(&mut cursor).unwrap());
        }
        messages
    }

    fn sample_send_options() -> SendOptions {
        SendOptions {
            level: 3,
            threads: 1,
            frame_target_bytes: 1,
            min_readers: 2,
            max_readers: 4,
            read_buffer_bytes: 1024,
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
        }
    }

    fn manifest_file_record(record_seq: u64, work_ordinal: u64, name: &str) -> ManifestRecord {
        ManifestRecord::new(
            ManifestRecordSeq(record_seq),
            ManifestRecordKind::File,
            0,
            name,
            Some(ManifestWorkOrdinal(work_ordinal)),
            ManifestRecordPayload::None,
        )
        .unwrap()
    }

    fn manifest_directory_enter_record(record_seq: u64, depth: u16, name: &str) -> ManifestRecord {
        ManifestRecord::new(
            ManifestRecordSeq(record_seq),
            ManifestRecordKind::DirectoryEnter,
            depth,
            name,
            None,
            ManifestRecordPayload::None,
        )
        .unwrap()
    }

    fn manifest_directory_leave_record(record_seq: u64, depth: u16, name: &str) -> ManifestRecord {
        ManifestRecord::new(
            ManifestRecordSeq(record_seq),
            ManifestRecordKind::DirectoryLeave,
            depth,
            name,
            None,
            ManifestRecordPayload::None,
        )
        .unwrap()
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

    fn sample_frame_v3() -> OutboundFrameV3 {
        OutboundFrameV3 {
            frame_no: 4,
            first_work_ordinal: 11,
            last_work_ordinal: 12,
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

    fn sender_manifest_state_root() -> PathBuf {
        std::env::temp_dir().join("fast-dump").join("sender-state")
    }

    fn manifest_root_identity_path(source: &Path) -> String {
        fs::canonicalize(source)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/")
    }

    fn write_sealed_manifest_spool(
        source: &Path,
        dump_id: &str,
        records: &[ManifestRecord],
    ) -> ManifestSpoolPaths {
        let paths = ManifestSpoolPaths::for_dump(&sender_manifest_state_root(), dump_id);
        let _ = fs::remove_dir_all(&paths.state_dir);
        fs::create_dir_all(&paths.state_dir).unwrap();

        let mut log_bytes = Vec::new();
        for record in records {
            ManifestSpoolWriter::append_record(&mut log_bytes, record).unwrap();
        }
        fs::write(&paths.log_path, &log_bytes).unwrap();

        let checkpoint_report =
            ManifestSpoolReader::build_sparse_checkpoints(&log_bytes, 1).unwrap();
        let checkpoint = checkpoint_report
            .checkpoints
            .last()
            .cloned()
            .expect("sealed test manifest must produce at least one checkpoint");
        checkpoint.write_atomic(&paths.checkpoint_path).unwrap();

        let root = manifest_root_identity_path(source);
        let identity = ManifestSourceIdentity::new(
            root.clone(),
            root.clone(),
            TEST_MANIFEST_BUILDER_VERSION,
            ManifestSourceKind::LivePath,
        )
        .unwrap();
        let mut meta = ManifestMeta::new(dump_id, identity, root).unwrap();
        meta.commit_checkpoint(
            checkpoint.last_record_seq,
            checkpoint.last_work_ordinal,
            checkpoint.checkpoint_seq,
        )
        .unwrap();
        meta.mark_sealed();
        meta.write_atomic(&paths.meta_path).unwrap();
        paths
    }

    #[test]
    fn send_manifest_v3_batches_record_aligned_chunks_and_waits_for_durable_acks() {
        let source = TestDir::new();
        let records = vec![
            manifest_file_record(1, 1, "alpha.txt"),
            manifest_file_record(2, 2, "beta.txt"),
            manifest_file_record(3, 3, "gamma.txt"),
        ];
        write_sealed_manifest_spool(source.path(), "dump-v3-manifest", &records);
        let encoded_first = ManifestSpoolWriter::encode_record(&records[0]).unwrap();
        let encoded_second = ManifestSpoolWriter::encode_record(&records[1]).unwrap();
        let encoded_third = ManifestSpoolWriter::encode_record(&records[2]).unwrap();
        let first_batch_len = encoded_first.len() + encoded_second.len();
        let base_manifest = ManifestBoundaryV3 {
            manifest_bytes_persisted: 0,
            last_record_seq: None,
            last_work_ordinal: None,
            sealed: false,
        };
        let final_ack = ManifestBoundaryV3 {
            manifest_bytes_persisted: 222,
            last_record_seq: Some(3),
            last_work_ordinal: Some(3),
            sealed: true,
        };
        let mut stream = ScriptedStream::new(
            [
                encode_message_v3(&V3Message::ManifestAck(ManifestAck {
                    committed_manifest: ManifestBoundaryV3 {
                        manifest_bytes_persisted: 111,
                        last_record_seq: Some(2),
                        last_work_ordinal: Some(2),
                        sealed: false,
                    },
                })),
                encode_message_v3(&V3Message::ManifestAck(ManifestAck {
                    committed_manifest: final_ack.clone(),
                })),
            ]
            .concat(),
        );
        let options = SendOptions {
            manifest_chunk_bytes: first_batch_len,
            manifest_ack_records: 2,
            manifest_ack_bytes: first_batch_len,
            ..sample_send_options()
        };

        let committed = super::send_manifest_v3(
            &mut stream,
            &crate::sender::SenderSource::ordinary_path(source.path().to_path_buf()),
            "dump-v3-manifest",
            &base_manifest,
            &options,
        )
        .unwrap();

        assert_eq!(committed, final_ack);
        assert_eq!(
            decode_messages_v3(&stream.written),
            vec![
                V3Message::ManifestStart(ManifestStart {
                    base_manifest: base_manifest.clone(),
                }),
                V3Message::ManifestChunk(ManifestChunk {
                    bytes: [encoded_first, encoded_second].concat(),
                    last_record_seq: Some(2),
                    last_work_ordinal: Some(2),
                }),
                V3Message::ManifestChunk(ManifestChunk {
                    bytes: encoded_third,
                    last_record_seq: Some(3),
                    last_work_ordinal: Some(3),
                }),
                V3Message::ManifestSeal(ManifestSeal {
                    final_record_seq: Some(3),
                    final_work_ordinal: Some(3),
                }),
            ]
        );
    }

    #[test]
    fn prepare_manifest_transfer_keeps_chunk_work_ordinals_monotonic_across_non_file_records() {
        let enter = manifest_directory_enter_record(1, 0, "nested");
        let file = manifest_file_record(2, 1, "alpha.txt");
        let leave = manifest_directory_leave_record(3, 0, "nested");
        let encoded_enter = ManifestSpoolWriter::encode_record(&enter).unwrap();
        let encoded_file = ManifestSpoolWriter::encode_record(&file).unwrap();
        let records = vec![enter, file, leave];

        let transfer = super::prepare_manifest_transfer(
            &records,
            &ManifestBoundaryV3 {
                manifest_bytes_persisted: 0,
                last_record_seq: None,
                last_work_ordinal: None,
                sealed: false,
            },
            encoded_enter.len() + encoded_file.len(),
            16,
        )
        .unwrap();

        assert_eq!(transfer.chunks.len(), 2);
        assert_eq!(transfer.chunks[0].last_record_seq, Some(2));
        assert_eq!(transfer.chunks[0].last_work_ordinal, Some(1));
        assert_eq!(transfer.chunks[1].last_record_seq, Some(3));
        assert_eq!(transfer.chunks[1].last_work_ordinal, Some(1));
        assert_eq!(transfer.final_work_ordinal, Some(1));
    }

    #[test]
    fn send_manifest_v3_rejects_unsealed_ack_for_manifest_seal() {
        let source = TestDir::new();
        let records = vec![
            manifest_file_record(1, 1, "alpha.txt"),
            manifest_file_record(2, 2, "beta.txt"),
        ];
        write_sealed_manifest_spool(source.path(), "dump-v3-manifest-bad-seal", &records);
        let encoded_first = ManifestSpoolWriter::encode_record(&records[0]).unwrap();
        let encoded_second = ManifestSpoolWriter::encode_record(&records[1]).unwrap();
        let base_manifest = ManifestBoundaryV3 {
            manifest_bytes_persisted: 0,
            last_record_seq: None,
            last_work_ordinal: None,
            sealed: false,
        };
        let mut stream =
            ScriptedStream::new(encode_message_v3(&V3Message::ManifestAck(ManifestAck {
                committed_manifest: ManifestBoundaryV3 {
                    manifest_bytes_persisted: 123,
                    last_record_seq: Some(2),
                    last_work_ordinal: Some(2),
                    sealed: false,
                },
            })));
        let options = SendOptions {
            manifest_chunk_bytes: encoded_first.len() + encoded_second.len(),
            manifest_ack_records: 16,
            manifest_ack_bytes: 1024 * 1024,
            ..sample_send_options()
        };

        let error = super::send_manifest_v3(
            &mut stream,
            &crate::sender::SenderSource::ordinary_path(source.path().to_path_buf()),
            "dump-v3-manifest-bad-seal",
            &base_manifest,
            &options,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("manifest seal must be acknowledged with a sealed manifest boundary")
        );
        assert_eq!(
            decode_messages_v3(&stream.written),
            vec![
                V3Message::ManifestStart(ManifestStart {
                    base_manifest: base_manifest.clone(),
                }),
                V3Message::ManifestChunk(ManifestChunk {
                    bytes: [encoded_first, encoded_second].concat(),
                    last_record_seq: Some(2),
                    last_work_ordinal: Some(2),
                }),
                V3Message::ManifestSeal(ManifestSeal {
                    final_record_seq: Some(2),
                    final_work_ordinal: Some(2),
                }),
            ]
        );
    }

    #[test]
    fn manifest_transfer_state_v3_stops_after_durable_boundary_and_finishes_later() {
        let records = vec![
            manifest_file_record(1, 1, "alpha.txt"),
            manifest_file_record(2, 2, "beta.txt"),
            manifest_file_record(3, 3, "gamma.txt"),
        ];
        let transfer = super::prepare_manifest_transfer(
            &records,
            &ManifestBoundaryV3 {
                manifest_bytes_persisted: 0,
                last_record_seq: None,
                last_work_ordinal: None,
                sealed: false,
            },
            4096,
            1,
        )
        .unwrap();
        let mut state = super::ManifestTransferStateV3::new(
            ManifestBoundaryV3 {
                manifest_bytes_persisted: 0,
                last_record_seq: None,
                last_work_ordinal: None,
                sealed: false,
            },
            transfer,
        );
        let options = SendOptions {
            manifest_ack_records: 2,
            manifest_ack_bytes: 1024 * 1024,
            ..sample_send_options()
        };
        let mut timings = super::V3SessionTimings::default();
        let mut stream = ScriptedStream::new(
            [
                encode_message_v3(&V3Message::ManifestAck(ManifestAck {
                    committed_manifest: ManifestBoundaryV3 {
                        manifest_bytes_persisted: 128,
                        last_record_seq: Some(2),
                        last_work_ordinal: Some(2),
                        sealed: false,
                    },
                })),
                encode_message_v3(&V3Message::ManifestAck(ManifestAck {
                    committed_manifest: ManifestBoundaryV3 {
                        manifest_bytes_persisted: 192,
                        last_record_seq: Some(3),
                        last_work_ordinal: Some(3),
                        sealed: true,
                    },
                })),
            ]
            .concat(),
        );

        state
            .advance_until_cover(&mut stream, 2, &options, &mut timings)
            .unwrap();

        let early_messages = decode_messages_v3(&stream.written);
        assert_eq!(early_messages.len(), 3);
        assert!(matches!(early_messages[0], V3Message::ManifestStart(_)));
        assert!(matches!(early_messages[1], V3Message::ManifestChunk(_)));
        assert!(matches!(early_messages[2], V3Message::ManifestChunk(_)));
        assert_eq!(state.committed.last_work_ordinal, Some(2));
        assert!(!state.committed.sealed);

        state.finish(&mut stream, &options, &mut timings).unwrap();

        let messages = decode_messages_v3(&stream.written);
        assert_eq!(messages.len(), 5);
        assert!(matches!(messages[3], V3Message::ManifestChunk(_)));
        assert_eq!(
            messages[4],
            V3Message::ManifestSeal(ManifestSeal {
                final_record_seq: Some(3),
                final_work_ordinal: Some(3),
            })
        );
        assert_eq!(state.committed.last_work_ordinal, Some(3));
        assert!(state.committed.sealed);
        assert!(timings.manifest_transfer_started.is_some());
        assert!(timings.manifest_transfer_finished.is_some());
    }

    #[test]
    fn send_streaming_frame_v3_stays_within_durable_manifest_window() {
        let source = TestDir::new();
        fs::write(source.path().join("alpha.txt"), b"alpha").unwrap();
        fs::write(source.path().join("beta.txt"), b"beta").unwrap();
        let resume_state = ResumeState {
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            archive_len: 0,
        };
        let walker = PendingFileWalker::new_with_manifest(
            source.path().to_path_buf(),
            "dump-v3-frame-window",
            &resume_state,
            None,
        )
        .unwrap();
        let mut pending = PendingFrameFiles::new(walker);
        let mut readers = super::new_reader_pool(&sample_send_options(), None);
        let mut retry_journal = super::StreamRetryJournal::default();
        let mut timings = super::V3SessionTimings::default();
        let mut stream = ScriptedStream::new(encode_message_v3(&V3Message::Ack(AckV3 {
            frame_no: 1,
            last_work_ordinal: 1,
            archive_len: 64,
        })));

        let frame = super::send_streaming_frame_v3(
            &mut stream,
            &mut pending,
            1,
            super::V3FrameWindow {
                max_work_ordinal: 1,
            },
            &sample_send_options(),
            &mut readers,
            None,
            &mut retry_journal,
            &mut timings,
        )
        .unwrap();

        let super::StreamingFrameAttemptV3::Committed(frame) = frame else {
            panic!("expected committed v3 frame");
        };
        assert_eq!(frame.outbound.first_work_ordinal, 1);
        assert_eq!(frame.outbound.last_work_ordinal, 1);
        let _ =
            super::prefetch_streaming_files(&mut pending, &mut readers, &retry_journal).unwrap();
        assert_eq!(pending.next_expected().map(|file| file.index), Some(2));
        let messages = decode_messages_v3(&stream.written);
        assert!(matches!(messages[0], V3Message::FrameStart(_)));
        let Some(V3Message::FrameEnd(frame_end)) = messages.last() else {
            panic!("expected final frame end");
        };
        assert_eq!(frame_end.last_work_ordinal, 1);
        readers.finish();
    }

    #[test]
    fn send_issue_journal_v3_batches_typed_entries_without_losing_order() {
        let large_error = "x".repeat(2048);
        let file_issues = (0_u64..40)
            .map(|index| super::SendIssue {
                file_index: index + 1,
                file_path: format!("file-{index:02}.txt"),
                attempts: 2,
                reason: if index % 2 == 0 {
                    super::SendIssueReason::OpenFailed
                } else {
                    super::SendIssueReason::UnstableAfterRetries
                },
                last_error: format!("{large_error}-{index}"),
            })
            .collect::<Vec<_>>();
        let discovery_issues = vec![
            super::SendDiscoveryIssue {
                record_seq: Some(91),
                path: "fifo.sock".to_owned(),
                scope: super::SendDiscoveryIssueScope::Entry,
                reason: super::SendDiscoveryIssueReason::UnsupportedEntry {
                    kind: "fifo".to_owned(),
                },
            },
            super::SendDiscoveryIssue {
                record_seq: Some(92),
                path: "sender-state".to_owned(),
                scope: super::SendDiscoveryIssueScope::SenderState,
                reason: super::SendDiscoveryIssueReason::SenderState {
                    event: "manifest_reset".to_owned(),
                    detail: "source invalidated".to_owned(),
                },
            },
        ];
        let mut stream = ScriptedStream::new(Vec::new());

        let chunks_sent =
            super::send_issue_journal_v3(&mut stream, &file_issues, &discovery_issues).unwrap();

        let messages = decode_messages_v3(&stream.written);
        assert_eq!(messages.len(), chunks_sent);
        assert!(chunks_sent > 1);
        let entries = messages
            .into_iter()
            .flat_map(|message| match message {
                V3Message::IssueChunk(chunk) => chunk.entries,
                other => panic!("unexpected message: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), file_issues.len() + discovery_issues.len());
        assert_eq!(entries[0], super::typed_issue_entry_v3(&file_issues[0]));
        assert_eq!(
            entries[file_issues.len() - 1],
            super::typed_issue_entry_v3(&file_issues[file_issues.len() - 1])
        );
        assert_eq!(
            entries[file_issues.len()],
            super::typed_discovery_issue_entry_v3(&discovery_issues[0])
        );
        assert_eq!(
            entries.last().unwrap(),
            &super::typed_discovery_issue_entry_v3(&discovery_issues[1])
        );
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
    fn send_frame_v3_writes_start_chunks_end_and_returns_ack() {
        let frame = sample_frame_v3();
        let expected_ack = AckV3 {
            frame_no: frame.frame_no,
            last_work_ordinal: frame.last_work_ordinal,
            archive_len: 1234,
        };
        let mut stream =
            ScriptedStream::new(encode_message_v3(&V3Message::Ack(expected_ack.clone())));

        let ack =
            send_frame_v3(&mut stream, &frame, [b"abc".as_slice(), b"def".as_slice()]).unwrap();

        assert_eq!(ack, expected_ack);
        let messages = decode_messages_v3(&stream.written);
        assert!(matches!(messages[0], V3Message::FrameStart(_)));
        assert_eq!(messages.len(), 4);
        assert!(matches!(messages[1], V3Message::FrameChunk(_)));
        assert!(matches!(messages[2], V3Message::FrameChunk(_)));
        assert!(matches!(messages[3], V3Message::FrameEnd(_)));
    }

    #[test]
    fn send_frame_v3_supports_zero_chunk_skip_only_progress() {
        let frame = OutboundFrameV3 {
            frame_no: 1,
            first_work_ordinal: 7,
            last_work_ordinal: 7,
            level: 3,
            threads: 1,
        };
        let mut stream = ScriptedStream::new(encode_message_v3(&V3Message::Ack(AckV3 {
            frame_no: 1,
            last_work_ordinal: 7,
            archive_len: 55,
        })));

        let ack = send_frame_v3(&mut stream, &frame, std::iter::empty::<&[u8]>()).unwrap();

        assert_eq!(ack.archive_len, 55);
        assert_eq!(
            decode_messages_v3(&stream.written),
            vec![
                V3Message::FrameStart(FrameStartV3 {
                    frame_no: 1,
                    first_work_ordinal: 7,
                    level: 3,
                    threads: 1,
                }),
                V3Message::FrameEnd(FrameEndV3 {
                    frame_no: 1,
                    last_work_ordinal: 7,
                    compressed_len: 0,
                    compressed_xxh3_64: Xxh3Default::new().digest(),
                }),
            ]
        );
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
    fn send_finish_v3_writes_finish_and_accepts_finish_ack() {
        let mut stream = ScriptedStream::new(encode_message_v3(&V3Message::FinishAck));

        let finish = send_finish_v3(&mut stream, 4, Some(12)).unwrap();

        assert_eq!(finish.final_frame_no, 4);
        assert_eq!(finish.final_work_ordinal, Some(12));
        assert_eq!(
            decode_messages_v3(&stream.written),
            vec![V3Message::Finish(finish)]
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
    fn send_dump_reports_discovery_warnings_without_uploading_file_issue_journal() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("broken")).unwrap();
        fs::create_dir_all(temp.path().join("healthy")).unwrap();
        fs::write(temp.path().join("healthy").join("ok.txt"), b"ok").unwrap();

        let dump_id = format!(
            "dump-discovery-warning-{}",
            temp.path().file_name().unwrap().to_string_lossy()
        );
        let paths = write_sealed_manifest_spool(
            temp.path(),
            &dump_id,
            &[
                ManifestRecord::new(
                    ManifestRecordSeq(1),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(2),
                    ManifestRecordKind::SubtreeSkip,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::SubtreeSkip {
                        reason: ManifestSubtreeSkipReason::ReadDirFailed {
                            error: "manifest read_dir failure".into(),
                        },
                    },
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(3),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "broken",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(4),
                    ManifestRecordKind::DirectoryEnter,
                    0,
                    "healthy",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(5),
                    ManifestRecordKind::File,
                    1,
                    "ok.txt",
                    Some(ManifestWorkOrdinal(1)),
                    ManifestRecordPayload::None,
                )
                .unwrap(),
                ManifestRecord::new(
                    ManifestRecordSeq(6),
                    ManifestRecordKind::DirectoryLeave,
                    0,
                    "healthy",
                    None,
                    ManifestRecordPayload::None,
                )
                .unwrap(),
            ],
        );

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
                last_file_path: "healthy/ok.txt".to_owned(),
                archive_len: 123,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
        };

        let report = send_dump(&mut stream, temp.path().to_path_buf(), &dump_id, &options).unwrap();

        assert_eq!(report.status, SendStatus::CompletedWithWarnings);
        assert_eq!(report.files_total_discovered, 1);
        assert_eq!(report.files_sent_clean, 1);
        assert_eq!(report.files_sent_best_effort, 0);
        assert_eq!(report.files_skipped_open_failed, 0);
        assert_eq!(report.files_skipped_unstable_after_retries, 0);
        assert_eq!(report.issue_journal.len(), 0);
        assert_eq!(report.discovery_issues_total, 1);
        assert_eq!(report.subtrees_skipped, 1);
        assert_eq!(report.manifest_resets_total, 0);
        assert_eq!(report.discovery_issue_journal.len(), 1);
        assert_eq!(report.discovery_issue_journal[0].record_seq, Some(2));
        assert_eq!(report.discovery_issue_journal[0].path, "broken");
        assert_eq!(
            report.discovery_issue_journal[0].scope,
            super::SendDiscoveryIssueScope::Subtree
        );
        match &report.discovery_issue_journal[0].reason {
            super::SendDiscoveryIssueReason::ReadDirFailed { error } => {
                assert!(error.contains("manifest read_dir failure"));
            }
            other => panic!("unexpected discovery issue reason: {other:?}"),
        }
        assert_eq!(report.sent_frames, 1);
        assert_eq!(report.final_frame_no, 1);
        assert_eq!(report.final_file_index, 1);
        assert_eq!(report.final_file_path, "healthy/ok.txt");
        assert_eq!(
            report.final_archive_bytes,
            123 + super::final_archive_footer_bytes().unwrap()
        );

        let messages = decode_messages(&stream.written);
        assert!(
            !messages
                .iter()
                .any(|message| matches!(message, Message::IssueJournalChunk(_)))
        );
        assert!(!paths.state_dir.exists());
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
            manifest_chunk_bytes: 256 * 1024,
            manifest_ack_records: 4096,
            manifest_ack_bytes: 1024 * 1024,
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
                    ..sample_send_options()
                },
                "threads must be at least 1",
            ),
            (
                SendOptions {
                    frame_target_bytes: 0,
                    ..sample_send_options()
                },
                "frame_target_bytes must be at least 1",
            ),
            (
                SendOptions {
                    min_readers: 0,
                    ..sample_send_options()
                },
                "min_readers must be at least 1",
            ),
            (
                SendOptions {
                    max_readers: 0,
                    ..sample_send_options()
                },
                "max_readers must be at least 1",
            ),
            (
                SendOptions {
                    min_readers: 2,
                    max_readers: 1,
                    ..sample_send_options()
                },
                "min_readers must not be greater than max_readers",
            ),
            (
                SendOptions {
                    read_buffer_bytes: 0,
                    ..sample_send_options()
                },
                "read_buffer_bytes must be at least 1",
            ),
            (
                SendOptions {
                    manifest_chunk_bytes: 0,
                    ..sample_send_options()
                },
                "manifest_chunk_bytes must be at least 1",
            ),
            (
                SendOptions {
                    manifest_ack_records: 0,
                    ..sample_send_options()
                },
                "manifest_ack_records must be at least 1",
            ),
            (
                SendOptions {
                    manifest_ack_bytes: 0,
                    ..sample_send_options()
                },
                "manifest_ack_bytes must be at least 1",
            ),
            (
                SendOptions {
                    manifest_chunk_bytes: 1024,
                    manifest_ack_bytes: 512,
                    ..sample_send_options()
                },
                "manifest_ack_bytes must not be smaller than manifest_chunk_bytes",
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
