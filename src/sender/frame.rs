use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use log::debug;
use tar::{EntryType, Header};

use crate::network::protocol::{self, ResumeState};

use super::{
    FileDisposition, FileReadOutcome, FileSkipReason, InputFile, PendingFileWalker,
    PipelineProfile, PipelineProfileCollector, ReadEvent, ReaderPool,
};

const DEFAULT_READ_BUFFER_BYTES: usize = 1024 * 1024;
const TAR_HEADER_BYTES: u64 = 512;

// Этот тип описывает уже собранный zstd-frame на стороне sender.
// Позже сюда будет привязан tar/zstd pipeline, а пока helper получает
// metadata кадра и последовательность уже готовых chunk-ов.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundFrame {
    pub frame_no: u64,
    pub first_file_index: u64,
    pub first_file_path: String,
    pub last_file_index: u64,
    pub last_file_path: String,
    pub level: i32,
    pub threads: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedFrame {
    pub outbound: OutboundFrame,
    pub chunks: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedDump {
    pub frames: Vec<PreparedFrame>,
    pub final_frame_no: u64,
    pub final_file_index: u64,
    pub final_file_path: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct FrameBuildStats {
    pub(crate) files_sent_clean: usize,
    pub(crate) files_sent_best_effort: usize,
    pub(crate) files_skipped_open_failed: usize,
    pub(crate) files_skipped_unstable_after_retries: usize,
    pub(crate) retries_total: u64,
    pub(crate) bytes_sent_raw: u64,
    pub(crate) bytes_sent_best_effort_raw: u64,
    pub(crate) tar_bytes_sent: u64,
}

impl FrameBuildStats {
    pub(crate) fn note_snapshot(
        &mut self,
        disposition: FileDisposition,
        size_at_open: u64,
        retries: u32,
    ) {
        self.retries_total = self.retries_total.saturating_add(retries as u64);
        self.bytes_sent_raw = self.bytes_sent_raw.saturating_add(size_at_open);
        self.tar_bytes_sent = self
            .tar_bytes_sent
            .saturating_add(tar_entry_bytes(size_at_open));
        match disposition {
            FileDisposition::Clean => {
                self.files_sent_clean += 1;
            }
            FileDisposition::BestEffort => {
                self.files_sent_best_effort += 1;
                self.bytes_sent_best_effort_raw =
                    self.bytes_sent_best_effort_raw.saturating_add(size_at_open);
            }
        }
    }

    pub(crate) fn note_skip(&mut self, reason: FileSkipReason, retries: u32) {
        self.retries_total = self.retries_total.saturating_add(retries as u64);
        match reason {
            FileSkipReason::OpenFailed => {
                self.files_skipped_open_failed += 1;
            }
            FileSkipReason::UnstableAfterRetries => {
                self.files_skipped_unstable_after_retries += 1;
            }
        }
    }
}

pub(crate) struct PreparedFrameBuild {
    pub(crate) frame: PreparedFrame,
    pub(crate) stats: FrameBuildStats,
}

#[derive(Clone, Copy)]
pub(crate) struct FrameBuildConfig<'a> {
    pub(crate) level: i32,
    pub(crate) threads: u32,
    pub(crate) frame_target_bytes: u64,
    pub(crate) profile: Option<&'a PipelineProfileCollector>,
}

pub(crate) struct PendingFrameFiles {
    walker: PendingFileWalker,
    queued: VecDeque<InputFile>,
    exhausted: bool,
}

impl PendingFrameFiles {
    pub(crate) fn new(walker: PendingFileWalker) -> Self {
        Self {
            walker,
            queued: VecDeque::new(),
            exhausted: false,
        }
    }

    // Очередь pending-файлов держит только уже запланированное окно.
    // Остальные пути остаются на стороне walker и открываются по мере надобности.
    pub(crate) fn prefetch(&mut self, readers: &mut ReaderPool) -> Result<(usize, bool)> {
        let target = readers.prefetch_target();
        while self.queued.len() < target && !self.exhausted {
            match self.walker.next_pending_file()? {
                Some(file) => {
                    readers.schedule(file.clone())?;
                    self.queued.push_back(file);
                }
                None => {
                    self.exhausted = true;
                }
            }
        }
        Ok((self.queued.len(), !self.exhausted))
    }

    pub(crate) fn next_expected(&self) -> Option<&InputFile> {
        self.queued.front()
    }

    pub(crate) fn consume_expected(&mut self) -> Option<InputFile> {
        self.queued.pop_front()
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.exhausted && self.queued.is_empty()
    }

    pub(crate) fn final_boundary(&self) -> Result<(u64, String)> {
        if !self.is_drained() {
            anyhow::bail!("sender pending frame queue is not drained yet");
        }
        self.walker.final_boundary()
    }

    pub(crate) fn last_discovered_boundary(&self) -> (u64, String) {
        self.walker.last_discovered_boundary()
    }
}

struct ChunkCollector {
    chunk_limit: usize,
    current: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    total_written: u64,
}

impl ChunkCollector {
    fn new(chunk_limit: usize) -> Self {
        Self {
            chunk_limit,
            current: Vec::with_capacity(chunk_limit),
            chunks: Vec::new(),
            total_written: 0,
        }
    }

    fn total_written(&self) -> u64 {
        self.total_written
    }

    fn finish(mut self) -> Vec<Vec<u8>> {
        if !self.current.is_empty() {
            self.chunks.push(std::mem::take(&mut self.current));
        }
        self.chunks
    }
}

impl Write for ChunkCollector {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        let original_len = buf.len();
        while !buf.is_empty() {
            if self.current.len() == self.chunk_limit {
                self.chunks.push(std::mem::take(&mut self.current));
                self.current = Vec::with_capacity(self.chunk_limit);
            }
            let remaining = self.chunk_limit - self.current.len();
            let take = remaining.min(buf.len());
            self.current.extend_from_slice(&buf[..take]);
            self.total_written += take as u64;
            buf = &buf[take..];
        }
        Ok(original_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Этот helper остаётся compatibility path для тестов и диагностики:
// он собирает готовые zstd-frame-ы в память, но теперь использует ту же
// streaming discovery-модель, что и production sender path.
pub fn prepare_dump(
    source: impl Into<PathBuf>,
    resume_state: &ResumeState,
    level: i32,
    threads: u32,
    frame_target_bytes: u64,
) -> Result<PreparedDump> {
    prepare_dump_impl(
        source,
        resume_state,
        level,
        threads,
        frame_target_bytes,
        None,
    )
}

// Этот вариант нужен бенчмаркам и профилировке sender-пайплайна:
// он возвращает тот же dump, но вместе с детализацией по стадиям.
pub fn prepare_dump_profile(
    source: impl Into<PathBuf>,
    resume_state: &ResumeState,
    level: i32,
    threads: u32,
    frame_target_bytes: u64,
) -> Result<(PreparedDump, PipelineProfile)> {
    let profile = PipelineProfileCollector::default();
    let dump = prepare_dump_impl(
        source,
        resume_state,
        level,
        threads,
        frame_target_bytes,
        Some(&profile),
    )?;
    Ok((dump, profile.finish()))
}

fn prepare_dump_impl(
    source: impl Into<PathBuf>,
    resume_state: &ResumeState,
    level: i32,
    threads: u32,
    frame_target_bytes: u64,
    profile: Option<&PipelineProfileCollector>,
) -> Result<PreparedDump> {
    let prepare_started = Instant::now();
    let source = source.into();
    let mut sent_raw_bytes = 0_u64;
    let walker = PendingFileWalker::new(source.clone(), resume_state)?;
    let mut pending = PendingFrameFiles::new(walker);
    debug!(
        "sender prepare_dump started: source={} level={} threads={} frame_target_bytes={}",
        source.display(),
        level,
        threads,
        frame_target_bytes
    );

    let mut frames = Vec::new();
    let mut next_frame_no = resume_state.last_committed_frame;
    let mut readers = ReaderPool::new(1, 1, 1, DEFAULT_READ_BUFFER_BYTES, profile.cloned());
    loop {
        let Some(build) = prepare_buffered_frame(
            &mut pending,
            next_frame_no.saturating_add(1),
            &mut readers,
            FrameBuildConfig {
                level,
                threads,
                frame_target_bytes,
                profile,
            },
        )?
        else {
            break;
        };
        sent_raw_bytes = sent_raw_bytes.saturating_add(build.stats.bytes_sent_raw);
        next_frame_no = build.frame.outbound.frame_no;
        frames.push(build.frame);
    }
    readers.finish();
    let (final_file_index, final_file_path) = pending.final_boundary()?;
    if let Some(profile) = profile {
        profile.record_total(sent_raw_bytes, prepare_started.elapsed());
    }
    debug!(
        "sender prepare_dump completed: frames={} final_frame_no={} final_file_index={} elapsed_ms={}",
        frames.len(),
        next_frame_no,
        final_file_index,
        prepare_started.elapsed().as_millis()
    );

    Ok(PreparedDump {
        frames,
        final_frame_no: next_frame_no,
        final_file_index,
        final_file_path,
    })
}

fn next_buffered_file(pending: &PendingFrameFiles) -> Result<InputFile> {
    pending
        .next_expected()
        .cloned()
        .context("buffered sender frame lost its scheduled file before processing")
}

fn consume_buffered_file(pending: &mut PendingFrameFiles) -> Result<InputFile> {
    pending
        .consume_expected()
        .context("buffered sender frame lost its scheduled file after processing")
}

fn prepare_buffered_frame(
    pending: &mut PendingFrameFiles,
    frame_no: u64,
    readers: &mut ReaderPool,
    config: FrameBuildConfig<'_>,
) -> Result<Option<PreparedFrameBuild>> {
    let _ = pending.prefetch(readers)?;
    let Some(first) = pending.next_expected().cloned() else {
        return Ok(None);
    };

    let build_started = Instant::now();
    let mut stats = FrameBuildStats::default();
    let mut encoder = zstd::stream::write::Encoder::new(
        ChunkCollector::new(protocol::MAX_FRAME_CHUNK_BYTES),
        config.level,
    )
    .context("failed to create zstd encoder for sender frame")?;
    encoder
        .multithread(config.threads)
        .context("failed to configure zstd worker threads for sender frame")?;

    let last = loop {
        let file = next_buffered_file(pending)?;
        match readers.next_for_with_scheduler(file.index, |pool| pending.prefetch(pool))? {
            ReadEvent::Ready(FileReadOutcome::Snapshot(snapshot)) => {
                let header_started = Instant::now();
                append_tar_header(&mut encoder, &snapshot.file.rel_path, snapshot.size_at_open)?;
                if let Some(profile) = config.profile {
                    profile.record_compress(TAR_HEADER_BYTES, header_started.elapsed());
                }
                stream_staged_snapshot(&mut encoder, &snapshot, config.profile)?;
                let padding_bytes = tar_padding_bytes(snapshot.size_at_open) as u64;
                let padding_started = Instant::now();
                append_tar_padding(&mut encoder, &snapshot.file.rel_path, snapshot.size_at_open)?;
                if let Some(profile) = config.profile {
                    profile.record_compress(padding_bytes, padding_started.elapsed());
                }
                let flush_started = Instant::now();
                encoder
                    .flush()
                    .context("failed to flush zstd encoder at sender file boundary")?;
                if let Some(profile) = config.profile {
                    profile.record_compress(0, flush_started.elapsed());
                }
                stats.note_snapshot(
                    snapshot.disposition,
                    snapshot.size_at_open,
                    snapshot.retries,
                );
            }
            ReadEvent::Ready(FileReadOutcome::Skip(skipped)) => {
                stats.note_skip(skipped.reason, skipped.retries);
            }
            ReadEvent::Fatal { message, .. } => anyhow::bail!("{message}"),
        }
        let last = consume_buffered_file(pending)?;
        let _ = pending.prefetch(readers)?;

        if pending.next_expected().is_none()
            || encoder.get_ref().total_written() >= config.frame_target_bytes
        {
            break last;
        }
    };

    let finish_started = Instant::now();
    let collector = encoder
        .finish()
        .context("failed to finish sender zstd frame")?;
    if let Some(profile) = config.profile {
        profile.record_compress(0, finish_started.elapsed());
    }
    let chunks = collector.finish();
    let compressed_bytes = chunks.iter().map(|chunk| chunk.len() as u64).sum::<u64>();
    debug!(
        "sender prepare_frame completed: frame_no={} first_file={} last_file={} compressed_bytes={} chunks={} elapsed_ms={}",
        frame_no,
        first.rel_path,
        last.rel_path,
        compressed_bytes,
        chunks.len(),
        build_started.elapsed().as_millis()
    );
    Ok(Some(PreparedFrameBuild {
        frame: PreparedFrame {
            outbound: OutboundFrame {
                frame_no,
                first_file_index: first.index,
                first_file_path: first.rel_path,
                last_file_index: last.index,
                last_file_path: last.rel_path,
                level: config.level,
                threads: config.threads,
            },
            chunks,
        },
        stats,
    }))
}

pub(crate) fn tar_entry_bytes(size_at_open: u64) -> u64 {
    TAR_HEADER_BYTES + size_at_open + tar_padding_bytes(size_at_open) as u64
}

fn tar_padding_bytes(size_at_open: u64) -> usize {
    ((512 - (size_at_open % 512)) % 512) as usize
}

pub(crate) fn append_tar_header<W>(writer: &mut W, rel_path: &str, size_at_open: u64) -> Result<()>
where
    W: Write,
{
    let mut header = Header::new_gnu();
    header
        .set_path(rel_path)
        .with_context(|| format!("failed to encode tar path {rel_path}"))?;
    header.set_entry_type(EntryType::Regular);
    header.set_size(size_at_open);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    writer
        .write_all(header.as_bytes())
        .with_context(|| format!("failed to write tar header for {rel_path}"))?;

    Ok(())
}

pub(crate) fn append_tar_padding<W>(writer: &mut W, rel_path: &str, size_at_open: u64) -> Result<()>
where
    W: Write,
{
    let padding = tar_padding_bytes(size_at_open);
    if padding != 0 {
        let zeros = [0_u8; 512];
        writer
            .write_all(&zeros[..padding])
            .with_context(|| format!("failed to write tar padding for {rel_path}"))?;
    }

    Ok(())
}

pub(crate) fn stream_staged_snapshot<W>(
    writer: &mut W,
    snapshot: &super::FileSnapshot,
    profile: Option<&PipelineProfileCollector>,
) -> Result<()>
where
    W: Write,
{
    let mut staged = std::fs::File::open(&snapshot.staged_path).with_context(|| {
        format!(
            "failed to reopen staged sender snapshot {} for {}",
            snapshot.staged_path.display(),
            snapshot.file.rel_path
        )
    })?;
    let mut buffer = vec![0_u8; DEFAULT_READ_BUFFER_BYTES];
    loop {
        let read = staged.read(&mut buffer).with_context(|| {
            format!(
                "failed to read staged sender snapshot {} for {}",
                snapshot.staged_path.display(),
                snapshot.file.rel_path
            )
        })?;
        if read == 0 {
            break;
        }
        let write_started = Instant::now();
        writer.write_all(&buffer[..read]).with_context(|| {
            format!(
                "failed to stream sender file {}",
                snapshot.file.abs_path.display()
            )
        })?;
        if let Some(profile) = profile {
            profile.record_compress(read as u64, write_started.elapsed());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{prepare_dump, tar_entry_bytes};
    use crate::network::protocol::{self, ResumeState};
    use crate::sender::InputFile;

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "fast-dump-sender-frame-tests-{}-{}",
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

    struct DeterministicBytes {
        state: u64,
    }

    impl DeterministicBytes {
        fn new(seed: u64) -> Self {
            Self { state: seed.max(1) }
        }

        fn next_u64(&mut self) -> u64 {
            let mut value = self.state;
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            self.state = value;
            value
        }

        fn fill(&mut self, buffer: &mut [u8]) {
            for chunk in buffer.chunks_mut(8) {
                let bytes = self.next_u64().to_le_bytes();
                chunk.copy_from_slice(&bytes[..chunk.len()]);
            }
        }
    }

    fn write_random_file(path: &Path, size: usize, seed: u64) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut generator = DeterministicBytes::new(seed);
        let mut bytes = vec![0_u8; size];
        generator.fill(&mut bytes);
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn prepare_dump_walks_files_in_deterministic_order_and_splits_frames() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("b")).unwrap();
        fs::create_dir_all(temp.path().join("a")).unwrap();
        fs::write(temp.path().join("b").join("second.txt"), b"222").unwrap();
        fs::write(temp.path().join("a").join("first.txt"), b"111").unwrap();
        fs::write(temp.path().join("root.txt"), b"333").unwrap();

        let dump = prepare_dump(
            temp.path().to_path_buf(),
            &ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            },
            3,
            1,
            1,
        )
        .unwrap();

        assert_eq!(dump.frames.len(), 3);
        assert_eq!(dump.frames[0].outbound.first_file_path, "a/first.txt");
        assert_eq!(dump.frames[0].outbound.last_file_path, "a/first.txt");
        assert_eq!(dump.frames[1].outbound.first_file_path, "b/second.txt");
        assert_eq!(dump.frames[2].outbound.first_file_path, "root.txt");
        assert_eq!(dump.final_frame_no, 3);
        assert_eq!(dump.final_file_index, 3);
        assert_eq!(dump.final_file_path, "root.txt");
    }

    #[test]
    fn prepare_dump_skips_committed_prefix_on_resume() {
        let temp = TestDir::new();
        fs::create_dir_all(temp.path().join("dir")).unwrap();
        fs::write(temp.path().join("dir").join("one.txt"), b"111").unwrap();
        fs::write(temp.path().join("dir").join("two.txt"), b"222").unwrap();

        let dump = prepare_dump(
            temp.path().to_path_buf(),
            &ResumeState {
                last_committed_frame: 1,
                last_committed_file_index: 1,
                last_committed_file_path: "dir/one.txt".to_owned(),
                archive_len: 123,
            },
            3,
            1,
            1024 * 1024,
        )
        .unwrap();

        assert_eq!(dump.frames.len(), 1);
        assert_eq!(dump.frames[0].outbound.frame_no, 2);
        assert_eq!(dump.frames[0].outbound.first_file_index, 2);
        assert_eq!(dump.frames[0].outbound.first_file_path, "dir/two.txt");
        assert_eq!(dump.final_frame_no, 2);
        assert_eq!(dump.final_file_index, 2);
        assert_eq!(dump.final_file_path, "dir/two.txt");
    }

    #[test]
    fn prepare_dump_supports_empty_source_directory() {
        let temp = TestDir::new();

        let dump = prepare_dump(
            temp.path().to_path_buf(),
            &ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            },
            3,
            1,
            1024 * 1024,
        )
        .unwrap();

        assert!(dump.frames.is_empty());
        assert_eq!(dump.final_frame_no, 0);
        assert_eq!(dump.final_file_index, 0);
        assert_eq!(dump.final_file_path, "");
    }

    #[test]
    fn tar_entry_bytes_matches_header_payload_and_padding() {
        for (size, expected_bytes) in [
            (0_u64, 512_u64),
            (10, 1024),
            (511, 1024),
            (512, 1024),
            (513, 1536),
            (1024, 1536),
        ] {
            assert_eq!(tar_entry_bytes(size), expected_bytes, "size={size}");
        }
    }

    #[test]
    fn prepare_dump_splits_only_after_large_file_when_target_is_crossed_mid_file() {
        let source_dir = TestDir::new();
        let payload_path = source_dir.path().join("payload.bin");
        write_random_file(&payload_path, 256 * 1024, 11);
        fs::write(source_dir.path().join("tail.txt"), b"tail").unwrap();
        let payload = InputFile {
            index: 1,
            abs_path: payload_path,
            rel_path: "payload.bin".to_owned(),
        };
        let mut encoder = zstd::stream::write::Encoder::new(
            super::ChunkCollector::new(protocol::MAX_FRAME_CHUNK_BYTES),
            3,
        )
        .unwrap();
        encoder.multithread(1).unwrap();
        super::append_tar_header(&mut encoder, &payload.rel_path, 256 * 1024).unwrap();
        encoder
            .write_all(&fs::read(&payload.abs_path).unwrap())
            .unwrap();
        super::append_tar_padding(&mut encoder, &payload.rel_path, 256 * 1024).unwrap();
        encoder.flush().unwrap();
        let payload_boundary_bytes = encoder.get_ref().total_written();

        let below_target = prepare_dump(
            source_dir.path().to_path_buf(),
            &ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            },
            3,
            1,
            payload_boundary_bytes - 1,
        )
        .unwrap();
        assert_eq!(below_target.frames.len(), 2);
        assert_eq!(
            below_target.frames[0].outbound.first_file_path,
            "payload.bin"
        );
        assert_eq!(
            below_target.frames[0].outbound.last_file_path,
            "payload.bin"
        );
        assert_eq!(below_target.frames[1].outbound.first_file_path, "tail.txt");

        let exact_target = prepare_dump(
            source_dir.path().to_path_buf(),
            &ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            },
            3,
            1,
            payload_boundary_bytes,
        )
        .unwrap();
        assert_eq!(exact_target.frames.len(), 2);
        assert_eq!(
            exact_target.frames[0].outbound.last_file_path,
            "payload.bin"
        );
        assert_eq!(exact_target.frames[1].outbound.first_file_path, "tail.txt");

        let above_target = prepare_dump(
            source_dir.path().to_path_buf(),
            &ResumeState {
                last_committed_frame: 0,
                last_committed_file_index: 0,
                last_committed_file_path: String::new(),
                archive_len: 0,
            },
            3,
            1,
            payload_boundary_bytes + 1,
        )
        .unwrap();
        assert_eq!(above_target.frames.len(), 1);
        assert_eq!(
            above_target.frames[0].outbound.first_file_path,
            "payload.bin"
        );
        assert_eq!(above_target.frames[0].outbound.last_file_path, "tail.txt");
    }
}
