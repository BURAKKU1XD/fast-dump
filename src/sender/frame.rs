use std::borrow::Cow;
use std::collections::VecDeque;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::str;
use std::time::Instant;

use anyhow::{Context, Result};
use log::debug;
use tar::{EntryType, Header};

use super::readers::{IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES, start_long_read_watchdog};
use super::source::{self, SourceDiscoveryIssue, SourceDiscoveryIssueSummary};
use super::{
    DirectFileStream, FileDisposition, FileSkipReason, InputFile, InputFileKind, PendingFileWalker,
    PipelineProfileCollector, ReaderPool, SnapshotStorage,
};

const TAR_HEADER_BYTES: u64 = 512;
const DEFAULT_MAX_FILES_PER_FRAME: usize = 4096;
pub(crate) const DEFAULT_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;
const GNU_LONG_LINK_PATH: &str = "././@LongLink";
const LARGE_FILE_SINGLE_FRAME_THRESHOLD_ENV: &str =
    "FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES";

#[derive(Debug, Clone)]
pub(crate) struct RetryableStreamFailure {
    pub(crate) file: InputFile,
}

impl std::fmt::Display for RetryableStreamFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "retryable sender stream failure for {}",
            self.file.abs_path().display()
        )
    }
}

impl std::error::Error for RetryableStreamFailure {}

// Этот тип описывает уже собранный zstd-frame на стороне sender.
// Позже сюда будет привязан tar/zstd pipeline, а пока helper получает
// metadata кадра и последовательность уже готовых chunk-ов.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundFrame {
    pub frame_no: u64,
    pub first_work_ordinal: u64,
    pub last_work_ordinal: u64,
    pub level: i32,
    pub threads: u32,
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
        file: &InputFile,
        disposition: FileDisposition,
        size_at_open: u64,
        retries: u32,
    ) {
        self.retries_total = self.retries_total.saturating_add(retries as u64);
        self.bytes_sent_raw = self.bytes_sent_raw.saturating_add(size_at_open);
        self.tar_bytes_sent = self
            .tar_bytes_sent
            .saturating_add(tar_entry_bytes_for_file(file, size_at_open));
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FramePolicy {
    pub(crate) soft_target_bytes: u64,
    pub(crate) max_files_per_frame: usize,
    pub(crate) large_file_single_frame_threshold: u64,
    pub(crate) isolate_retried_files: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameProgress {
    pub(crate) tar_bytes: u64,
    pub(crate) files_with_payload: usize,
}

impl FrameProgress {
    pub(crate) fn note_payload_file(&mut self, file: &InputFile, size_at_open: u64) {
        self.tar_bytes = self
            .tar_bytes
            .saturating_add(tar_entry_bytes_for_file(file, size_at_open));
        self.files_with_payload = self.files_with_payload.saturating_add(1);
    }
}

impl FramePolicy {
    pub(crate) fn from_frame_target_bytes(frame_target_bytes: u64) -> Self {
        Self {
            soft_target_bytes: frame_target_bytes.max(1),
            max_files_per_frame: DEFAULT_MAX_FILES_PER_FRAME,
            // Держим direct-stream порог и dedicated-frame порог отдельно:
            // medium direct-stream файлы можно оставлять в общем frame,
            // а truly large файлы по-прежнему изолировать ради rollback scope.
            large_file_single_frame_threshold: resolve_large_file_single_frame_threshold_bytes(
                env_override_large_file_single_frame_threshold_bytes(),
            ),
            isolate_retried_files: true,
        }
    }

    pub(crate) fn should_close_before_next(
        &self,
        progress: FrameProgress,
        next_file: &InputFile,
        next_file_retries: u32,
    ) -> bool {
        if progress.files_with_payload == 0 {
            return false;
        }

        if self.isolate_retried_files && next_file_retries > 0 {
            return true;
        }

        self.is_probably_large(next_file)
    }

    pub(crate) fn should_close_after_current(
        &self,
        progress: FrameProgress,
        current_size_at_open: u64,
        current_retries: u32,
        has_more_files: bool,
    ) -> bool {
        if !has_more_files {
            return true;
        }

        if progress.files_with_payload >= self.max_files_per_frame {
            return true;
        }

        if progress.tar_bytes >= self.soft_target_bytes {
            return true;
        }

        // Держим рискованный файл в отдельном frame, если он уже оказался первым
        // payload в кадре. Так мы сокращаем объём повторной отправки после abort.
        if progress.files_with_payload == 1
            && current_size_at_open >= self.large_file_single_frame_threshold
        {
            return true;
        }

        self.isolate_retried_files && progress.files_with_payload == 1 && current_retries > 0
    }

    pub(crate) fn is_probably_large(&self, file: &InputFile) -> bool {
        // Это эвристика только для выбора границы frame. Корректность tar header
        // и retry-семантика по-прежнему опираются на metadata открытого handle.
        if !file.is_regular() {
            return false;
        }

        fs::metadata(file.abs_path())
            .map(|metadata| {
                metadata.is_file() && metadata.len() >= self.large_file_single_frame_threshold
            })
            .unwrap_or(false)
    }
}

fn env_override_large_file_single_frame_threshold_bytes() -> Option<u64> {
    let raw = env::var(LARGE_FILE_SINGLE_FRAME_THRESHOLD_ENV).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    match trimmed.parse::<u64>() {
        Ok(value) => Some(value),
        Err(error) => {
            debug!(
                "ignoring invalid {}={:?}: {}",
                LARGE_FILE_SINGLE_FRAME_THRESHOLD_ENV, raw, error
            );
            None
        }
    }
}

fn resolve_large_file_single_frame_threshold_bytes(explicit_threshold: Option<u64>) -> u64 {
    explicit_threshold
        .unwrap_or(DEFAULT_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES)
        .max(IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES.saturating_add(1))
}

#[derive(Clone)]
pub(crate) struct PendingFrameFiles {
    walker: PendingFileWalker,
    queued: VecDeque<InputFile>,
    scheduled_prefix: usize,
    exhausted: bool,
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::collections::{BTreeMap, VecDeque};
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    static STREAM_FAILURES: OnceLock<Mutex<BTreeMap<String, VecDeque<u64>>>> = OnceLock::new();

    fn stream_failures() -> &'static Mutex<BTreeMap<String, VecDeque<u64>>> {
        STREAM_FAILURES.get_or_init(|| Mutex::new(BTreeMap::new()))
    }

    fn path_key(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    #[allow(dead_code)]
    pub(crate) fn fail_stream_after_next_write(path: &Path, after_bytes: u64) {
        stream_failures()
            .lock()
            .unwrap()
            .entry(path_key(path))
            .or_default()
            .push_back(after_bytes.max(1));
    }

    pub(crate) fn note_streamed_bytes(path: &Path, bytes: u64) -> bool {
        let mut failures = stream_failures().lock().unwrap();
        let Some(queue) = failures.get_mut(&path_key(path)) else {
            return false;
        };
        let Some(remaining) = queue.front_mut() else {
            return false;
        };
        if bytes >= *remaining {
            queue.pop_front();
            if queue.is_empty() {
                failures.remove(&path_key(path));
            }
            return true;
        }
        *remaining -= bytes;
        false
    }

    #[allow(dead_code)]
    pub(crate) fn clear() {
        stream_failures().lock().unwrap().clear();
    }
}

impl PendingFrameFiles {
    pub(crate) fn new(walker: PendingFileWalker) -> Self {
        Self {
            walker,
            queued: VecDeque::new(),
            scheduled_prefix: 0,
            exhausted: false,
        }
    }

    // Очередь pending-файлов держит только уже запланированное окно.
    // Остальные пути остаются на стороне walker и открываются по мере надобности.
    pub(crate) fn prefetch_with<F>(
        &mut self,
        readers: &mut ReaderPool,
        mut schedule: F,
    ) -> Result<(usize, bool)>
    where
        F: FnMut(&mut ReaderPool, InputFile) -> Result<()>,
    {
        let target = readers.prefetch_target();
        self.prefetch_with_target(target, |file| schedule(readers, file))
    }

    pub(crate) fn prefetch_with_target<F>(
        &mut self,
        target: usize,
        mut schedule: F,
    ) -> Result<(usize, bool)>
    where
        F: FnMut(InputFile) -> Result<()>,
    {
        let schedule_target = target.min(self.queued.len());
        while self.scheduled_prefix < schedule_target {
            let file = self
                .queued
                .get(self.scheduled_prefix)
                .cloned()
                .context("sender pending queue lost a scheduled prefix file")?;
            schedule(file)?;
            self.scheduled_prefix = self.scheduled_prefix.saturating_add(1);
        }

        while self.queued.len() < target && !self.exhausted {
            match self.walker.next_pending_file()? {
                Some(file) => {
                    schedule(file.clone())?;
                    self.queued.push_back(file);
                    self.scheduled_prefix = self.scheduled_prefix.saturating_add(1);
                }
                None => {
                    self.exhausted = true;
                }
            }
        }
        Ok((self.queued.len(), !self.exhausted))
    }

    pub(crate) fn discover_ahead(&mut self, target: usize) -> Result<(usize, bool)> {
        while self.queued.len() < target && !self.exhausted {
            match self.walker.next_pending_file()? {
                Some(file) => self.queued.push_back(file),
                None => {
                    self.exhausted = true;
                }
            }
        }
        Ok((self.queued.len(), !self.exhausted))
    }

    pub(crate) fn grow_discovery_window(&mut self, additional: usize) -> Result<(usize, bool)> {
        let target = self.queued.len().saturating_add(additional.max(1));
        self.discover_ahead(target)
    }

    pub(crate) fn next_expected(&self) -> Option<&InputFile> {
        self.queued.front()
    }

    pub(crate) fn consume_expected(&mut self) -> Option<InputFile> {
        let file = self.queued.pop_front()?;
        self.scheduled_prefix = self.scheduled_prefix.saturating_sub(1);
        Some(file)
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

    pub(crate) fn cleanup_after_finish(&self) -> Result<()> {
        if !self.is_drained() {
            anyhow::bail!("sender pending frame queue is not drained yet");
        }
        self.walker.cleanup_after_finish()
    }

    pub(crate) fn last_discovered_boundary(&self) -> (u64, String) {
        self.walker.last_discovered_boundary()
    }

    pub(crate) fn discovery_issues(&self) -> Vec<SourceDiscoveryIssue> {
        self.walker.discovery_issues()
    }

    pub(crate) fn discovery_issue_summary(&self) -> SourceDiscoveryIssueSummary {
        self.walker.discovery_issue_summary()
    }

    pub(crate) fn pop_manifest_chunk(
        &mut self,
        force_flush: bool,
    ) -> Result<Option<source::manifest::ManifestChunkPlan>> {
        self.walker.pop_manifest_chunk(force_flush)
    }

    pub(crate) fn produced_manifest_work_ordinal(&self) -> u64 {
        self.walker.produced_manifest_work_ordinal()
    }

    pub(crate) fn queued_manifest_target_work_ordinal(&self) -> Option<u64> {
        self.queued.back().map(|file| file.index)
    }

    pub(crate) fn queued_manifest_target_work_ordinal_with_limit(
        &self,
        max_files: usize,
    ) -> Option<u64> {
        self.queued
            .iter()
            .take(max_files.max(1))
            .next_back()
            .map(|file| file.index)
    }

    pub(crate) fn manifest_exhausted(&self) -> bool {
        self.walker.manifest_exhausted()
    }

    pub(crate) fn produced_manifest_boundary(&self) -> (Option<u64>, Option<u64>) {
        self.walker.produced_manifest_boundary()
    }

    pub(crate) fn final_manifest_boundary(&self) -> Result<(Option<u64>, Option<u64>)> {
        self.walker.final_manifest_boundary()
    }
}

// Этот helper остаётся буферизованным путём для тестов и диагностики:
// он собирает готовые zstd-frame-ы в память, но теперь использует ту же
// streaming discovery-модель, что и production sender path.
pub(crate) fn tar_entry_bytes(rel_path: &str, size_at_open: u64) -> u64 {
    tar_header_bytes(rel_path) + size_at_open + tar_padding_bytes(size_at_open) as u64
}

pub(crate) fn tar_header_bytes(rel_path: &str) -> u64 {
    TAR_HEADER_BYTES + tar_long_path_entry_bytes(rel_path)
}

pub(crate) fn tar_padding_bytes(size_at_open: u64) -> usize {
    ((512 - (size_at_open % 512)) % 512) as usize
}

fn tar_long_path_entry_bytes(rel_path: &str) -> u64 {
    if !path_requires_gnu_long_link(rel_path) {
        return 0;
    }

    let payload_len = rel_path.len() as u64 + 1;
    TAR_HEADER_BYTES + payload_len + tar_padding_bytes(payload_len) as u64
}

fn path_requires_gnu_long_link(rel_path: &str) -> bool {
    let mut header = Header::new_gnu();
    header.set_path(rel_path).is_err()
}

pub(crate) fn append_tar_header<W>(writer: &mut W, rel_path: &str, size_at_open: u64) -> Result<()>
where
    W: Write,
{
    let mut header = Header::new_gnu();
    prepare_path_header_for_file(writer, &mut header, rel_path)?;
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

fn tar_entry_payload_bytes_for_file(file: &InputFile, size_at_open: u64) -> u64 {
    match file.kind {
        InputFileKind::Regular => size_at_open + tar_padding_bytes(size_at_open) as u64,
        InputFileKind::Symlink { .. } => 0,
    }
}

fn tar_long_link_entry_bytes_for_file(file: &InputFile) -> u64 {
    let Some(target) = file.link_target() else {
        return 0;
    };
    if !link_target_requires_gnu_long_link(target) {
        return 0;
    }

    match path_to_tar_bytes(target) {
        Ok(bytes) => {
            let payload_len = bytes.len() as u64 + 1;
            TAR_HEADER_BYTES + payload_len + tar_padding_bytes(payload_len) as u64
        }
        Err(_) => 0,
    }
}

pub(crate) fn tar_entry_bytes_for_file(file: &InputFile, size_at_open: u64) -> u64 {
    match file.kind {
        InputFileKind::Regular => tar_entry_bytes(file.rel_path(), size_at_open),
        InputFileKind::Symlink { .. } => {
            tar_header_bytes_for_file(file) + tar_entry_payload_bytes_for_file(file, size_at_open)
        }
    }
}

pub(crate) fn tar_header_bytes_for_file(file: &InputFile) -> u64 {
    match file.kind {
        InputFileKind::Regular => tar_header_bytes(file.rel_path()),
        InputFileKind::Symlink { .. } => {
            TAR_HEADER_BYTES
                + tar_long_path_entry_bytes(file.rel_path())
                + tar_long_link_entry_bytes_for_file(file)
        }
    }
}

fn link_target_requires_gnu_long_link(target: &Path) -> bool {
    let mut header = Header::new_gnu();
    match header.set_link_name(target) {
        Ok(()) => false,
        Err(_) => match path_to_tar_bytes(target) {
            Ok(bytes) => bytes.len() >= header.as_old().linkname.len(),
            Err(_) => false,
        },
    }
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn path_to_tar_bytes(path: &Path) -> std::io::Result<Cow<'_, [u8]>> {
    use std::os::unix::ffi::OsStrExt;

    Ok(Cow::Borrowed(path.as_os_str().as_bytes()))
}

#[cfg(any(windows, target_arch = "wasm32"))]
fn path_to_tar_bytes(path: &Path) -> std::io::Result<Cow<'_, [u8]>> {
    let value = path.as_os_str().to_str().ok_or_else(|| {
        std::io::Error::other(format!("path {} is not valid Unicode", path.display()))
    })?;
    if value.contains('\\') {
        let mut normalized = value.as_bytes().to_vec();
        for byte in &mut normalized {
            if *byte == b'\\' {
                *byte = b'/';
            }
        }
        Ok(Cow::Owned(normalized))
    } else {
        Ok(Cow::Borrowed(value.as_bytes()))
    }
}

fn truncated_utf8_prefix(bytes: &[u8], max_len: usize) -> Result<&str> {
    let prefix = &bytes[..bytes.len().min(max_len)];
    match str::from_utf8(prefix) {
        Ok(value) => Ok(value),
        Err(error) => str::from_utf8(&prefix[..error.valid_up_to()])
            .context("failed to truncate GNU long tar path to a valid utf-8 prefix"),
    }
}

fn append_gnu_long_data_entry<W>(
    writer: &mut W,
    data: &[u8],
    record_type: u8,
    context_path: &str,
    context_label: &str,
) -> Result<()>
where
    W: Write,
{
    let payload_len = data.len() as u64 + 1;
    let mut header = Header::new_gnu();
    header
        .set_path(GNU_LONG_LINK_PATH)
        .context("failed to encode GNU long-link tar header path")?;
    header.set_entry_type(EntryType::new(record_type));
    header.set_size(payload_len);
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    writer.write_all(header.as_bytes()).with_context(|| {
        format!("failed to write GNU long-link tar header for {context_label} {context_path}")
    })?;
    writer.write_all(data).with_context(|| {
        format!("failed to write GNU long-link tar payload for {context_label} {context_path}")
    })?;
    writer.write_all(&[0]).with_context(|| {
        format!("failed to terminate GNU long-link tar payload for {context_label} {context_path}")
    })?;

    let padding = tar_padding_bytes(payload_len);
    if padding != 0 {
        let zeros = [0_u8; 512];
        writer.write_all(&zeros[..padding]).with_context(|| {
            format!("failed to write GNU long-link tar padding for {context_label} {context_path}")
        })?;
    }

    Ok(())
}

fn prepare_path_header_for_file<W>(
    writer: &mut W,
    header: &mut Header,
    rel_path: &str,
) -> Result<()>
where
    W: Write,
{
    if let Err(error) = header.set_path(rel_path) {
        let path_bytes = path_to_tar_bytes(Path::new(rel_path))
            .with_context(|| format!("failed to encode tar path {rel_path}"))?;
        if path_bytes.len() < header.as_old().name.len() {
            return Err(error).with_context(|| format!("failed to encode tar path {rel_path}"));
        }

        // Для длинных путей пишем GNU LongLink рядом с обычным entry header,
        // потому что вручную сериализуем tar без `tar::Builder`.
        append_gnu_long_data_entry(writer, &path_bytes, b'L', rel_path, "path")?;
        let truncated = truncated_utf8_prefix(&path_bytes, header.as_old().name.len())?;
        header
            .set_path(truncated)
            .context("failed to encode truncated tar path after GNU long-link extension")?;
    }

    Ok(())
}

fn prepare_link_name_for_file<W>(
    writer: &mut W,
    header: &mut Header,
    target: &Path,
    rel_path: &str,
) -> Result<()>
where
    W: Write,
{
    if let Err(error) = header.set_link_name(target) {
        let link_bytes = path_to_tar_bytes(target).with_context(|| {
            format!(
                "failed to encode tar symlink target {} for {}",
                target.display(),
                rel_path
            )
        })?;
        if link_bytes.len() < header.as_old().linkname.len() {
            return Err(error).with_context(|| {
                format!(
                    "failed to encode tar symlink target {} for {}",
                    target.display(),
                    rel_path
                )
            });
        }

        // Для длинного link target tar ждёт отдельную GNU long-link запись
        // перед symlink header, а в самом header оставляем короткий префикс.
        append_gnu_long_data_entry(writer, &link_bytes, b'K', rel_path, "link target")?;
        let truncated = truncated_utf8_prefix(&link_bytes, header.as_old().linkname.len())?;
        header.set_link_name(truncated).context(
            "failed to encode truncated tar symlink target after GNU long-link extension",
        )?;
    }

    Ok(())
}

pub(crate) fn append_tar_file_header<W>(
    writer: &mut W,
    file: &InputFile,
    size_at_open: u64,
) -> Result<()>
where
    W: Write,
{
    if file.is_regular() {
        return append_tar_header(writer, file.rel_path(), size_at_open);
    }

    let mut header = Header::new_gnu();
    prepare_path_header_for_file(writer, &mut header, file.rel_path())?;

    match &file.kind {
        InputFileKind::Regular => {
            header.set_entry_type(EntryType::Regular);
            header.set_size(size_at_open);
            header.set_mode(0o644);
        }
        InputFileKind::Symlink { target } => {
            prepare_link_name_for_file(writer, &mut header, target, file.rel_path())?;
            header.set_entry_type(EntryType::new(b'2'));
            header.set_size(0);
            header.set_mode(0o777);
        }
    }

    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    writer
        .write_all(header.as_bytes())
        .with_context(|| format!("failed to write tar header for {}", file.rel_path()))?;

    Ok(())
}

pub(crate) fn append_tar_file_padding<W>(
    writer: &mut W,
    file: &InputFile,
    size_at_open: u64,
) -> Result<()>
where
    W: Write,
{
    if file.is_regular() {
        return append_tar_padding(writer, file.rel_path(), size_at_open);
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
    match &snapshot.storage {
        SnapshotStorage::Memory(bytes) => {
            let write_started = Instant::now();
            writer.write_all(bytes).with_context(|| {
                format!(
                    "failed to stream sender file {}",
                    snapshot.file.abs_path().display()
                )
            })?;
            #[cfg(test)]
            if test_hooks::note_streamed_bytes(snapshot.file.abs_path(), bytes.len() as u64) {
                return Err(RetryableStreamFailure {
                    file: snapshot.file.clone(),
                }
                .into());
            }
            if let Some(profile) = profile {
                profile.record_compress(bytes.len() as u64, write_started.elapsed());
            }
        }
    }

    Ok(())
}

pub(crate) fn stream_direct_file<W>(
    writer: &mut W,
    direct: &mut DirectFileStream,
    profile: Option<&PipelineProfileCollector>,
) -> Result<()>
where
    W: Write,
{
    // Прямой поток больших файлов дольше всего удерживает reader worker, поэтому
    // отдельный watchdog помогает увидеть path даже если read() заблокировался.
    let _watchdog = start_long_read_watchdog(
        direct.file.abs_path(),
        "direct-stream",
        direct.retries.saturating_add(1),
    );
    let mut buffer = vec![0_u8; direct.read_buffer_bytes.max(1)];
    let mut remaining = direct.size_at_open;
    let mut exposed_bytes = 0_u64;
    let mut read_buffers = 0_u64;

    while remaining > 0 {
        if let Some(limit) = direct.forced_short_read_limit
            && exposed_bytes >= limit
        {
            return Err(RetryableStreamFailure {
                file: direct.file.clone(),
            }
            .into());
        }

        let mut read_limit = buffer.len().min(remaining as usize);
        if let Some(limit) = direct.forced_short_read_limit {
            read_limit = read_limit.min(limit.saturating_sub(exposed_bytes) as usize);
        }

        let read = direct
            .input
            .read(&mut buffer[..read_limit])
            .with_context(|| {
                format!(
                    "failed to read sender file {}",
                    direct.file.abs_path().display()
                )
            })?;
        if read == 0 {
            return Err(RetryableStreamFailure {
                file: direct.file.clone(),
            }
            .into());
        }

        let write_started = Instant::now();
        writer.write_all(&buffer[..read]).with_context(|| {
            format!(
                "failed to stream sender file {}",
                direct.file.abs_path().display()
            )
        })?;
        #[cfg(test)]
        if test_hooks::note_streamed_bytes(direct.file.abs_path(), read as u64) {
            return Err(RetryableStreamFailure {
                file: direct.file.clone(),
            }
            .into());
        }
        if let Some(profile) = profile {
            profile.record_compress(read as u64, write_started.elapsed());
        }

        remaining -= read as u64;
        exposed_bytes += read as u64;
        read_buffers += 1;
    }

    if let Some(profile) = profile {
        profile.record_file_read(
            direct.file.index,
            direct.file.rel_path(),
            direct.size_at_open,
            read_buffers,
            direct.read_started,
            Instant::now(),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;
    use std::io::Cursor;
    use std::path::{Path, PathBuf};
    use std::time::{Instant, SystemTime, UNIX_EPOCH};
    use tar::{Archive, EntryType};

    use super::{
        IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES, append_tar_file_header, append_tar_file_padding,
        resolve_large_file_single_frame_threshold_bytes, stream_direct_file,
        tar_entry_bytes_for_file,
    };
    use crate::sender::{DirectFileStream, FileDisposition, InputFile, PipelineProfileCollector};

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
    fn tar_entry_bytes_matches_header_payload_and_padding() {
        let file = InputFile::regular(1, PathBuf::from("file.bin"), "file.bin".to_owned());
        for (size, expected_bytes) in [
            (0_u64, 512_u64),
            (10, 1024),
            (511, 1024),
            (512, 1024),
            (513, 1536),
            (1024, 1536),
        ] {
            assert_eq!(
                tar_entry_bytes_for_file(&file, size),
                expected_bytes,
                "size={size}"
            );
        }
    }

    #[test]
    fn tar_entry_bytes_accounts_for_gnu_long_path_extension() {
        let rel_path = format!("root/{}/payload.bin", "deep".repeat(30));
        let file = InputFile::regular(1, PathBuf::from("payload.bin"), rel_path.clone());

        assert_eq!(tar_entry_bytes_for_file(&file, 0), 1536);
    }

    #[test]
    fn append_tar_header_supports_long_paths_via_gnu_long_link() {
        let rel_path = format!("root/{}/payload.bin", "deep".repeat(30));
        let file = InputFile::regular(1, PathBuf::from("payload.bin"), rel_path.clone());
        let mut archive_bytes = Vec::new();

        append_tar_file_header(&mut archive_bytes, &file, 0).unwrap();
        append_tar_file_padding(&mut archive_bytes, &file, 0).unwrap();
        archive_bytes.extend_from_slice(&[0_u8; 1024]);

        let mut archive = Archive::new(Cursor::new(archive_bytes));
        let entries = archive
            .entries()
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                entry.path().unwrap().to_string_lossy().into_owned()
            })
            .collect::<Vec<_>>();

        assert_eq!(entries, vec![rel_path]);
    }

    #[test]
    fn append_tar_header_writes_symlink_entries() {
        let file = InputFile::symlink(
            1,
            PathBuf::from("link.txt"),
            "link.txt".to_owned(),
            PathBuf::from("../target.txt"),
        );
        let mut archive_bytes = Vec::new();

        append_tar_file_header(&mut archive_bytes, &file, 0).unwrap();
        append_tar_file_padding(&mut archive_bytes, &file, 0).unwrap();
        archive_bytes.extend_from_slice(&[0_u8; 1024]);

        let mut archive = Archive::new(Cursor::new(archive_bytes));
        let mut entries = archive.entries().unwrap();
        let entry = entries.next().unwrap().unwrap();

        assert_eq!(entry.path().unwrap().to_string_lossy(), "link.txt");
        assert_eq!(
            entry.link_name().unwrap().unwrap().to_string_lossy(),
            "../target.txt"
        );
        assert_eq!(entry.header().entry_type(), EntryType::new(b'2'));
    }

    #[test]
    fn frame_policy_clamps_override_to_direct_stream_cutover() {
        assert_eq!(
            resolve_large_file_single_frame_threshold_bytes(Some(1)),
            IN_MEMORY_SNAPSHOT_THRESHOLD_BYTES.saturating_add(1)
        );
        assert_eq!(
            resolve_large_file_single_frame_threshold_bytes(Some(8 * 1024 * 1024)),
            8 * 1024 * 1024
        );
    }

    #[test]
    fn stream_direct_file_records_read_profile_and_uses_configured_buffer() {
        let source_dir = TestDir::new();
        let payload_path = source_dir.path().join("direct.bin");
        write_random_file(&payload_path, 4096, 17);
        let profile = PipelineProfileCollector::default();
        let mut writer = Vec::new();
        let mut direct = DirectFileStream {
            file: InputFile::regular(7, payload_path.clone(), "direct.bin".to_owned()),
            input: File::open(&payload_path).unwrap(),
            size_at_open: 4096,
            forced_short_read_limit: None,
            disposition: FileDisposition::Clean,
            retries: 0,
            read_started: Instant::now(),
            read_buffer_bytes: 1024,
        };

        stream_direct_file(&mut writer, &mut direct, Some(&profile)).unwrap();

        let profile = profile.finish();
        assert_eq!(writer.len(), 4096);
        assert_eq!(profile.files_read, 1);
        assert_eq!(profile.read.bytes, 4096);
        assert_eq!(profile.read_buffers, 4);
        assert_eq!(profile.file_reads.len(), 1);
        assert_eq!(profile.file_reads[0].file_index, 7);
        assert_eq!(profile.file_reads[0].bytes, 4096);
        assert_eq!(profile.file_reads[0].read_buffers, 4);
    }
}
