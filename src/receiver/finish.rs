use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use log::debug;

use crate::network::protocol::{self, Finish, IssueCode, IssueEntry, IssueScope, Message};

use super::journal::recover_partial_archive;
use super::shared::{
    ReceiverTempFileKind, append_temp_file_payload, remove_file_path, rename_path,
};
use super::{ReceiverProfile, ReceiverState};

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn receive_finish_message<T>(
    stream: &mut T,
    state: &ReceiverState,
    finish: Finish,
    profile: &mut ReceiverProfile,
) -> Result<Finish>
where
    T: Read + Write,
{
    let finish_started = Instant::now();
    validate_finish(&state.journal, &finish).or_else(|error| {
        let message = error.to_string();
        protocol::write_message(
            stream,
            &Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        )?;
        stream
            .flush()
            .context("failed to flush v3 finish validation error")?;
        Err(error)
    })?;

    let archive_finalize_started = Instant::now();
    if let Err(error) = finalize_archive(state) {
        let message = error.to_string();
        let _ = protocol::write_message(
            stream,
            &Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        );
        let _ = stream.flush();
        return Err(error.context("failed to finalize receiver archive"));
    }
    profile.record_archive_finalize(archive_finalize_started.elapsed());

    let issue_finalize_started = Instant::now();
    if let Err(error) = finalize_issue_journal(state) {
        let message = error.to_string();
        let _ = protocol::write_message(
            stream,
            &Message::Error(protocol::ErrorMessage {
                message: message.clone(),
            }),
        );
        let _ = stream.flush();
        return Err(error.context("failed to finalize receiver typed issue journal"));
    }
    profile.record_issue_finalize(issue_finalize_started.elapsed());

    let ack_started = Instant::now();
    protocol::write_message(stream, &Message::FinishAck)?;
    stream
        .flush()
        .context("failed to flush v3 finish ack to peer")?;
    profile.record_finish_ack_flush(ack_started.elapsed());
    profile.record_finish(finish_started.elapsed());
    debug!(
        "receiver v3 finish acknowledged: dump_id={} final_frame_no={} final_work_ordinal={:?} elapsed_ms={}",
        state.paths.dump_id(),
        finish.final_frame_no,
        finish.final_work_ordinal,
        finish_started.elapsed().as_millis()
    );

    Ok(finish)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn append_issue_journal_chunk(
    state: &ReceiverState,
    entries: &[IssueEntry],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    let issue_journal_temp_path = state.paths.issue_journal_temp_path();
    let current_len = match fs::metadata(&issue_journal_temp_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "failed to inspect receiver typed issue journal temp file {}",
                    issue_journal_temp_path.display()
                )
            });
        }
    };
    let needs_header = current_len == 0;
    let mut payload = Vec::new();
    if needs_header {
        payload.extend_from_slice(
            b"scope\trecord_seq\twork_ordinal\tattempts\tcode\tpath\tdetail\tcontext\n",
        );
    }
    for entry in entries {
        writeln!(
            payload,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            issue_scope(entry.scope),
            entry
                .record_seq
                .map(|value| value.to_string())
                .unwrap_or_default(),
            entry
                .work_ordinal
                .map(|value| value.to_string())
                .unwrap_or_default(),
            entry.attempts,
            issue_code(entry.code),
            escape_issue_field(&entry.path),
            escape_issue_field(&entry.detail),
            escape_issue_field(&entry.context)
        )
        .with_context(|| {
            format!(
                "failed to buffer receiver typed issue journal entry {}",
                issue_journal_temp_path.display()
            )
        })?;
    }
    append_temp_file_payload(
        &issue_journal_temp_path,
        &payload,
        ReceiverTempFileKind::IssueJournalTemp,
    )?;
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn finalize_issue_journal(state: &ReceiverState) -> Result<()> {
    let issue_journal_temp_path = state.paths.issue_journal_temp_path();
    let issue_journal_path = state.paths.issue_journal_path();
    if !issue_journal_temp_path.exists() {
        if issue_journal_path.exists() {
            remove_file_path(issue_journal_path).with_context(|| {
                format!(
                    "failed to remove stale receiver typed issue journal {}",
                    issue_journal_path.display()
                )
            })?;
        }
        return Ok(());
    }
    if issue_journal_path.exists() {
        remove_file_path(issue_journal_path).with_context(|| {
            format!(
                "failed to remove stale receiver typed issue journal {}",
                issue_journal_path.display()
            )
        })?;
    }
    rename_path(&issue_journal_temp_path, issue_journal_path).with_context(|| {
        format!(
            "failed to rename receiver typed issue journal {} to {}",
            issue_journal_temp_path.display(),
            issue_journal_path.display()
        )
    })
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn discard_issue_journal_temp(state: &ReceiverState) -> Result<()> {
    let issue_journal_temp_path = state.paths.issue_journal_temp_path();
    if issue_journal_temp_path.exists() {
        remove_file_path(&issue_journal_temp_path).with_context(|| {
            format!(
                "failed to remove receiver typed issue journal temp file {}",
                issue_journal_temp_path.display()
            )
        })?;
    }
    Ok(())
}

fn issue_scope(scope: IssueScope) -> &'static str {
    match scope {
        IssueScope::File => "file",
        IssueScope::DiscoveryEntry => "discovery_entry",
        IssueScope::DiscoverySubtree => "discovery_subtree",
        IssueScope::SenderState => "sender_state",
    }
}

fn issue_code(code: IssueCode) -> &'static str {
    match code {
        IssueCode::OpenFailed => "open_failed",
        IssueCode::UnstableAfterRetries => "unstable_after_retries",
        IssueCode::UnsupportedEntry => "unsupported_entry",
        IssueCode::UnreadableSymlinkTarget => "unreadable_symlink_target",
        IssueCode::VirtualFilesystem => "virtual_filesystem",
        IssueCode::Io => "io",
        IssueCode::SenderStateEvent => "sender_state_event",
        IssueCode::ReadDirFailed => "read_dir_failed",
        IssueCode::SourceInvalidated => "source_invalidated",
    }
}

fn escape_issue_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_finish(journal: &super::ReceiverJournal, finish: &Finish) -> Result<()> {
    if finish.final_frame_no != journal.last_committed_frame {
        bail!(
            "v3 finish frame number mismatch: expected {} got {}",
            journal.last_committed_frame,
            finish.final_frame_no
        );
    }
    if finish.final_work_ordinal != journal.last_committed_work_ordinal {
        bail!(
            "v3 finish work ordinal mismatch: expected {:?} got {:?}",
            journal.last_committed_work_ordinal,
            finish.final_work_ordinal
        );
    }

    Ok(())
}

fn finalize_archive(state: &ReceiverState) -> Result<()> {
    let finalize_started = Instant::now();
    let partial_archive_path = state.paths.partial_archive_path();
    let final_archive_path = state.paths.final_archive_path();
    let journal_path = state.paths.journal_path();

    if final_archive_path.exists() {
        bail!(
            "final archive already exists and would be overwritten: {}",
            final_archive_path.display()
        );
    }

    let file = OpenOptions::new()
        .append(true)
        .open(partial_archive_path)
        .with_context(|| {
            format!(
                "failed to open partial archive for finalization {}",
                partial_archive_path.display()
            )
        })?;
    let mut encoder = zstd::stream::write::Encoder::new(file, 0)
        .context("failed to create zstd encoder for tar end blocks")?;
    encoder
        .write_all(&[0_u8; 1024])
        .context("failed to append tar end blocks into final zstd frame")?;
    let file = encoder
        .finish()
        .context("failed to finish zstd frame for tar end blocks")?;
    file.sync_all().with_context(|| {
        format!(
            "failed to sync finalized receiver archive {}",
            partial_archive_path.display()
        )
    })?;

    rename_path(partial_archive_path, final_archive_path).with_context(|| {
        format!(
            "failed to rename finalized archive {} to {}",
            partial_archive_path.display(),
            final_archive_path.display()
        )
    })?;

    if journal_path.exists()
        && let Err(error) = remove_file_path(journal_path)
    {
        rename_path(final_archive_path, partial_archive_path).with_context(|| {
            format!(
                "failed to roll finalized archive back from {} to {}",
                final_archive_path.display(),
                partial_archive_path.display()
            )
        })?;
        recover_partial_archive(partial_archive_path, state.journal.archive_len)?;
        return Err(error).with_context(|| {
            format!(
                "failed to remove receiver journal after finalization {}",
                journal_path.display()
            )
        });
    }

    debug!(
        "receiver archive finalized: dump_id={} final_archive={} elapsed_ms={}",
        state.paths.dump_id(),
        final_archive_path.display(),
        finalize_started.elapsed().as_millis()
    );

    Ok(())
}
