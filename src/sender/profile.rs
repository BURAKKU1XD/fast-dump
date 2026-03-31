use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StageProfile {
    pub bytes: u64,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileReadProfile {
    pub file_index: u64,
    pub path: String,
    pub bytes: u64,
    pub read_buffers: u64,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PipelineProfile {
    pub read: StageProfile,
    pub compress: StageProfile,
    pub send: StageProfile,
    pub total: StageProfile,
    pub files_read: u64,
    pub read_buffers: u64,
    pub file_reads: Vec<FileReadProfile>,
}

#[derive(Debug, Clone)]
pub(crate) struct PipelineProfileCollector {
    started: Instant,
    inner: Arc<Mutex<PipelineProfileState>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct PipelineProfileState {
    profile: PipelineProfile,
    read_spans: Vec<TimeSpan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeSpan {
    start: Duration,
    end: Duration,
}

impl Default for PipelineProfileCollector {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            inner: Arc::new(Mutex::new(PipelineProfileState::default())),
        }
    }
}

impl PipelineProfileCollector {
    // Профиль чтения хранит и агрегаты, и отдельные file-level измерения,
    // чтобы бенчмарк мог посчитать percentile и по stage, и по файлам.
    pub(crate) fn record_file_read(
        &self,
        file_index: u64,
        path: &str,
        bytes: u64,
        read_buffers: u64,
        started_at: Instant,
        finished_at: Instant,
    ) {
        let mut state = self.inner.lock().unwrap();
        let duration = finished_at.saturating_duration_since(started_at);
        state.profile.read.bytes = state.profile.read.bytes.saturating_add(bytes);
        state.profile.files_read = state.profile.files_read.saturating_add(1);
        state.profile.read_buffers = state.profile.read_buffers.saturating_add(read_buffers);
        state.profile.file_reads.push(FileReadProfile {
            file_index,
            path: path.to_owned(),
            bytes,
            read_buffers,
            duration,
        });
        state.read_spans.push(TimeSpan {
            start: started_at.saturating_duration_since(self.started),
            end: finished_at.saturating_duration_since(self.started),
        });
    }

    pub(crate) fn record_compress(&self, bytes: u64, duration: Duration) {
        let mut state = self.inner.lock().unwrap();
        state.profile.compress.bytes = state.profile.compress.bytes.saturating_add(bytes);
        state.profile.compress.duration += duration;
    }

    pub(crate) fn record_send(&self, bytes: u64, duration: Duration) {
        let mut state = self.inner.lock().unwrap();
        state.profile.send.bytes = state.profile.send.bytes.saturating_add(bytes);
        state.profile.send.duration += duration;
    }

    pub(crate) fn record_total(&self, bytes: u64, duration: Duration) {
        let mut state = self.inner.lock().unwrap();
        state.profile.total.bytes = bytes;
        state.profile.total.duration = duration;
    }

    pub(crate) fn finish(&self) -> PipelineProfile {
        let mut state = self.inner.lock().unwrap().clone();
        state
            .profile
            .file_reads
            .sort_by(|left, right| left.file_index.cmp(&right.file_index));
        state.profile.read.duration = merged_span_duration(&mut state.read_spans);
        state.profile
    }
}

fn merged_span_duration(spans: &mut [TimeSpan]) -> Duration {
    if spans.is_empty() {
        return Duration::ZERO;
    }

    spans.sort_by(|left, right| left.start.cmp(&right.start).then(left.end.cmp(&right.end)));
    let mut total = Duration::ZERO;
    let mut current = spans[0];
    for span in spans.iter().copied().skip(1) {
        if span.start <= current.end {
            if span.end > current.end {
                current.end = span.end;
            }
            continue;
        }

        total += current.end.saturating_sub(current.start);
        current = span;
    }

    total + current.end.saturating_sub(current.start)
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{PipelineProfile, PipelineProfileCollector};

    #[test]
    fn collector_finish_returns_default_profile_without_reads() {
        let collector = PipelineProfileCollector::default();

        let profile = collector.finish();

        assert_eq!(profile, PipelineProfile::default());
    }

    #[test]
    fn collector_merges_overlapping_read_spans() {
        let collector = PipelineProfileCollector::default();
        let first_started = Instant::now();
        thread::sleep(Duration::from_millis(5));
        let second_started = Instant::now();
        thread::sleep(Duration::from_millis(5));
        let finished = Instant::now();

        collector.record_file_read(1, "alpha", 10, 2, first_started, finished);
        collector.record_file_read(2, "beta", 10, 2, second_started, finished);

        let profile = collector.finish();
        assert_eq!(profile.files_read, 2);
        assert_eq!(profile.read_buffers, 4);
        assert_eq!(profile.read.bytes, 20);
        assert!(profile.read.duration >= finished.saturating_duration_since(first_started));
        assert!(profile.read.duration < Duration::from_millis(25));
    }

    #[test]
    fn collector_sorts_file_reads_and_accumulates_stage_totals() {
        let collector = PipelineProfileCollector::default();
        let first_started = collector.started + Duration::from_millis(30);
        let first_finished = first_started + Duration::from_millis(10);
        let second_started = collector.started + Duration::from_millis(5);
        let second_finished = second_started + Duration::from_millis(15);

        collector.record_file_read(2, "beta.bin", 7, 3, first_started, first_finished);
        collector.record_file_read(1, "alpha.bin", 5, 2, second_started, second_finished);
        collector.record_compress(9, Duration::from_millis(4));
        collector.record_compress(1, Duration::from_millis(2));
        collector.record_send(4, Duration::from_millis(3));
        collector.record_send(6, Duration::from_millis(5));
        collector.record_total(123, Duration::from_millis(50));

        let profile = collector.finish();

        assert_eq!(
            profile
                .file_reads
                .iter()
                .map(|entry| entry.file_index)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(profile.file_reads[0].path, "alpha.bin");
        assert_eq!(profile.file_reads[0].duration, Duration::from_millis(15));
        assert_eq!(profile.file_reads[1].path, "beta.bin");
        assert_eq!(profile.file_reads[1].duration, Duration::from_millis(10));
        assert_eq!(profile.read.bytes, 12);
        assert_eq!(profile.read_buffers, 5);
        assert_eq!(profile.files_read, 2);
        assert_eq!(profile.read.duration, Duration::from_millis(25));
        assert_eq!(profile.compress.bytes, 10);
        assert_eq!(profile.compress.duration, Duration::from_millis(6));
        assert_eq!(profile.send.bytes, 10);
        assert_eq!(profile.send.duration, Duration::from_millis(8));
        assert_eq!(profile.total.bytes, 123);
        assert_eq!(profile.total.duration, Duration::from_millis(50));
    }
}
