use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiverProfile {
    pub total_duration: Duration,
    pub manifest_stage_bytes: u64,
    pub manifest_stage_duration: Duration,
    pub manifest_commit_bytes: u64,
    pub manifest_commit_duration: Duration,
    pub manifest_acks: u64,
    pub manifest_ack_flush_duration: Duration,
    pub frame_bytes: u64,
    pub frames_committed: u64,
    pub frame_duration: Duration,
    pub frame_write_duration: Duration,
    pub frame_sync_duration: Duration,
    pub frame_journal_persist_duration: Duration,
    pub frame_ack_flush_duration: Duration,
    pub issue_tail_chunks: u64,
    pub issue_tail_duration: Duration,
    pub finish_duration: Duration,
    pub archive_finalize_duration: Duration,
    pub issue_finalize_duration: Duration,
    pub finish_ack_flush_duration: Duration,
}

impl ReceiverProfile {
    pub(crate) fn record_total(&mut self, duration: Duration) {
        self.total_duration = duration;
    }

    pub(crate) fn record_manifest_stage(&mut self, bytes: u64, duration: Duration) {
        self.manifest_stage_bytes = self.manifest_stage_bytes.saturating_add(bytes);
        self.manifest_stage_duration += duration;
    }

    pub(crate) fn record_manifest_commit(&mut self, bytes: u64, duration: Duration) {
        self.manifest_commit_bytes = self.manifest_commit_bytes.saturating_add(bytes);
        self.manifest_commit_duration += duration;
    }

    pub(crate) fn record_manifest_ack_flush(&mut self, duration: Duration) {
        self.manifest_acks = self.manifest_acks.saturating_add(1);
        self.manifest_ack_flush_duration += duration;
    }

    pub(crate) fn record_frame_write(&mut self, duration: Duration) {
        self.frame_write_duration += duration;
    }

    pub(crate) fn record_frame_sync(&mut self, duration: Duration) {
        self.frame_sync_duration += duration;
    }

    pub(crate) fn record_frame_journal_persist(&mut self, duration: Duration) {
        self.frame_journal_persist_duration += duration;
    }

    pub(crate) fn record_frame_ack_flush(&mut self, duration: Duration) {
        self.frame_ack_flush_duration += duration;
    }

    pub(crate) fn record_frame_commit(&mut self, bytes: u64, duration: Duration) {
        self.frame_bytes = self.frame_bytes.saturating_add(bytes);
        self.frames_committed = self.frames_committed.saturating_add(1);
        self.frame_duration += duration;
    }

    pub(crate) fn record_issue_tail(&mut self, chunks: u64, duration: Duration) {
        self.issue_tail_chunks = self.issue_tail_chunks.saturating_add(chunks);
        self.issue_tail_duration += duration;
    }

    pub(crate) fn record_archive_finalize(&mut self, duration: Duration) {
        self.archive_finalize_duration += duration;
    }

    pub(crate) fn record_issue_finalize(&mut self, duration: Duration) {
        self.issue_finalize_duration += duration;
    }

    pub(crate) fn record_finish_ack_flush(&mut self, duration: Duration) {
        self.finish_ack_flush_duration += duration;
    }

    pub(crate) fn record_finish(&mut self, duration: Duration) {
        self.finish_duration += duration;
    }
}
