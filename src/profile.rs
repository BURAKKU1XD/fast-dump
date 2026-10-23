use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StageProfile {
    pub bytes: u64,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SenderProfile {
    pub scan: StageProfile,
    pub read: StageProfile,
    pub tar: StageProfile,
    pub compress: StageProfile,
    pub send: StageProfile,
    pub ack_wait: StageProfile,
    pub total: StageProfile,
    pub time_to_first_bundle: Duration,
    pub discovered_entries: u64,
    pub bundled_entries: u64,
    pub terminal_skips: u64,
    pub bundles_sent: u64,
    pub bundle_aborts: u64,
    pub bundle_rebuilds: u64,
    pub flush_count: u64,
}

impl SenderProfile {
    pub(crate) fn record_send(&mut self, bytes: u64, duration: Duration) {
        self.send.bytes = self.send.bytes.saturating_add(bytes);
        self.send.duration += duration;
    }

    pub(crate) fn record_ack_wait(&mut self, duration: Duration) {
        self.ack_wait.bytes = self.ack_wait.bytes.saturating_add(1);
        self.ack_wait.duration += duration;
    }

    pub(crate) fn record_total(&mut self, bytes: u64, duration: Duration) {
        self.total.bytes = bytes;
        self.total.duration = duration;
    }

    pub(crate) fn record_flush(&mut self) {
        self.flush_count = self.flush_count.saturating_add(1);
    }

    pub(crate) fn record_time_to_first_bundle(&mut self, duration: Duration) {
        if self.time_to_first_bundle.is_zero() {
            self.time_to_first_bundle = duration;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReceiverProfile {
    pub recovery: StageProfile,
    pub skip_catalog: StageProfile,
    pub bundle_write: StageProfile,
    pub bundle_write_io: StageProfile,
    pub bundle_periodic_sync: StageProfile,
    pub bundle_sync: StageProfile,
    pub bundle_sync_skipped: u64,
    pub wal_append: StageProfile,
    pub bundle_commit: StageProfile,
    pub bundle_ack_flush: StageProfile,
    pub total_duration: Duration,
    pub finalize_duration: Duration,
    pub archive_finalize_duration: Duration,
    pub issue_finalize_duration: Duration,
    pub finish_ack_flush_duration: Duration,
    pub committed_bundles: u64,
}

impl ReceiverProfile {
    pub(crate) fn record_total(&mut self, duration: Duration) {
        self.total_duration = duration;
    }

    pub(crate) fn record_recovery(&mut self, bytes: u64, duration: Duration) {
        self.recovery.bytes = bytes;
        self.recovery.duration = duration;
    }

    pub(crate) fn record_skip_catalog(&mut self, entries: u64, duration: Duration) {
        self.skip_catalog.bytes = self.skip_catalog.bytes.saturating_add(entries);
        self.skip_catalog.duration += duration;
    }

    pub(crate) fn record_bundle_write(&mut self, bytes: u64, duration: Duration) {
        self.bundle_write.bytes = self.bundle_write.bytes.saturating_add(bytes);
        self.bundle_write.duration += duration;
    }

    pub(crate) fn record_bundle_sync(&mut self, duration: Duration) {
        self.bundle_sync.bytes = self.bundle_sync.bytes.saturating_add(1);
        self.bundle_sync.duration += duration;
    }

    pub(crate) fn record_bundle_write_details(
        &mut self,
        bytes: u64,
        write_duration: Duration,
        periodic_sync_duration: Duration,
        periodic_sync_count: u64,
    ) {
        // bundle_write сохраняет прежний aggregate, новые поля разделяют write
        // и промежуточный sync без изменения существующих метрик и stdout.
        self.bundle_write_io.bytes = self.bundle_write_io.bytes.saturating_add(bytes);
        self.bundle_write_io.duration += write_duration;
        self.bundle_periodic_sync.bytes = self
            .bundle_periodic_sync
            .bytes
            .saturating_add(periodic_sync_count);
        self.bundle_periodic_sync.duration += periodic_sync_duration;
    }

    pub(crate) fn record_wal_append(&mut self, bytes: u64, duration: Duration) {
        self.wal_append.bytes = self.wal_append.bytes.saturating_add(bytes);
        self.wal_append.duration += duration;
    }

    pub(crate) fn record_bundle_commit_batch(
        &mut self,
        bytes: u64,
        duration: Duration,
        bundles: u64,
    ) {
        self.bundle_commit.bytes = self.bundle_commit.bytes.saturating_add(bytes);
        self.bundle_commit.duration += duration;
        self.committed_bundles = self.committed_bundles.saturating_add(bundles);
    }

    pub(crate) fn record_bundle_ack_flush(&mut self, duration: Duration) {
        self.bundle_ack_flush.bytes = self.bundle_ack_flush.bytes.saturating_add(1);
        self.bundle_ack_flush.duration += duration;
    }

    pub(crate) fn record_finalize(
        &mut self,
        total_duration: Duration,
        archive_finalize_duration: Duration,
        issue_finalize_duration: Duration,
    ) {
        self.finalize_duration += total_duration;
        self.archive_finalize_duration += archive_finalize_duration;
        self.issue_finalize_duration += issue_finalize_duration;
    }

    pub(crate) fn record_finish_ack_flush(&mut self, duration: Duration) {
        self.finish_ack_flush_duration += duration;
    }
}
