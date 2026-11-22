use std::io::{self, Read, Write};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use tar::{Builder, EntryType, Header};

use crate::network::protocol::{BundleEntryDescriptor, BundleEntryKind};
use crate::progress::SourceFileGuard;

use super::compress::{BundleCompressor, CompressionOutput, DirectEncoder};
#[cfg(test)]
use super::read::MaterializedEntry;

#[cfg(test)]
use crate::network::protocol::{SkipReason, SkippedEntry};

#[cfg(test)]
use super::read::StreamedRegularFile;

#[cfg(test)]
pub(crate) struct StreamedBundleBuild {
    pub(crate) compressed_payload: Vec<u8>,
    pub(crate) tar_payload_bytes: u64,
    pub(crate) raw_read_bytes: u64,
    pub(crate) read_duration: Duration,
    pub(crate) compress_duration: Duration,
}

#[cfg(test)]
pub(crate) enum StreamedBundleOutcome {
    Built(StreamedBundleBuild),
    Skipped {
        skipped: SkippedEntry,
        raw_read_bytes: u64,
        read_duration: Duration,
    },
}

pub(crate) struct CountingWriter<W> {
    inner: W,
    bytes_written: u64,
}

impl<W> CountingWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub(crate) fn into_inner(self) -> W {
        self.inner
    }
}

impl<W> Write for CountingWriter<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes_written = self.bytes_written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) struct ExactFileReader<'a> {
    file: &'a mut std::fs::File,
    abs_path: &'a std::path::Path,
    remaining: u64,
    forced_short_remaining: Option<u64>,
    bytes_read: u64,
    read_duration: Duration,
}

impl<'a> ExactFileReader<'a> {
    pub(crate) fn new(
        file: &'a mut std::fs::File,
        abs_path: &'a std::path::Path,
        size_at_open: u64,
        forced_short_limit: Option<u64>,
    ) -> Self {
        Self {
            file,
            abs_path,
            remaining: size_at_open,
            forced_short_remaining: forced_short_limit,
            bytes_read: 0,
            read_duration: Duration::ZERO,
        }
    }

    pub(crate) fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    pub(crate) fn read_duration(&self) -> Duration {
        self.read_duration
    }
}

impl Read for ExactFileReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        if matches!(self.forced_short_remaining, Some(0)) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "v4 streamed bundle forced a short read",
            ));
        }

        let mut limit = buf.len() as u64;
        limit = limit.min(self.remaining);
        if let Some(forced_limit) = self.forced_short_remaining {
            limit = limit.min(forced_limit);
        }

        let started = Instant::now();
        let read = self.file.read(&mut buf[..limit as usize])?;
        self.read_duration += started.elapsed();

        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "short read while streaming v4 bundle entry {}",
                    self.abs_path.display()
                ),
            ));
        }

        let read = read as u64;
        self.remaining = self.remaining.saturating_sub(read);
        self.bytes_read = self.bytes_read.saturating_add(read);
        if let Some(forced_limit) = &mut self.forced_short_remaining {
            *forced_limit = forced_limit.saturating_sub(read);
        }
        Ok(read as usize)
    }
}

#[cfg(test)]
pub(crate) fn build_compressed_bundle(
    entries: &[MaterializedEntry],
    compression_level: i32,
    compression_threads: u32,
) -> Result<(Vec<u8>, u64)> {
    let mut compressor = BundleCompressor::new(compression_level, compression_threads)?;
    build_compressed_bundle_with(entries, &mut compressor)
}

#[cfg(test)]
pub(crate) fn build_compressed_bundle_with(
    entries: &[MaterializedEntry],
    compressor: &mut BundleCompressor,
) -> Result<(Vec<u8>, u64)> {
    let mut bundle = BufferedBundleWriter::new(compressor, Vec::new())?;
    for entry in entries {
        bundle.append(entry)?;
    }
    bundle.finish()
}

#[cfg(test)]
pub(crate) fn build_compressed_bundle_into(
    entries: &mut [MaterializedEntry],
    compressor: &mut BundleCompressor,
    mut output: Vec<u8>,
) -> Result<(Vec<u8>, u64)> {
    output.clear();
    let mut bundle = BufferedBundleWriter::new(compressor, output)?;
    for entry in entries {
        bundle.append(entry)?;
        // Write уже потребил bytes или скопировал их во внутренние буферы.
        // Освобождаем исходный payload до роста compressed output и finish;
        // descriptors остаются для BundlePlan. Shared slab живёт до последнего slice.
        entry.contents = Vec::new().into();
    }
    bundle.finish()
}

pub(crate) struct BufferedBundleWriter<'a, O: CompressionOutput> {
    writer: CountingWriter<io::BufWriter<DirectEncoder<'a, O>>>,
}

impl<'a, O: CompressionOutput> BufferedBundleWriter<'a, O> {
    pub(crate) fn new(compressor: &'a mut BundleCompressor, output: O) -> Result<Self> {
        let encoder = compressor.direct_encoder(output)?;
        // Объединяем мелкие tar-записи до входа в zstd; большие срезы BufWriter
        // передаёт напрямую, не копируя их во входной буфер.
        Ok(Self {
            writer: CountingWriter::new(io::BufWriter::with_capacity(128 * 1024, encoder)),
        })
    }

    #[cfg(test)]
    fn append(&mut self, entry: &MaterializedEntry) -> Result<()> {
        append_buffered_entry(&mut self.writer, entry)
    }

    pub(crate) fn append_payload(
        &mut self,
        descriptor: &BundleEntryDescriptor,
        mode: u32,
        contents: &[u8],
        progress: Option<&SourceFileGuard<'_>>,
    ) -> Result<()> {
        append_materialized_entry(&mut self.writer, descriptor, mode, contents, progress)
    }

    pub(crate) fn finish(self) -> Result<(O, u64)> {
        let tar_payload_bytes = self.writer.bytes_written();
        // into_inner дописывает входной буфер без flush самого zstd: frame
        // завершаем один раз, после всех tar-заголовков, тел и выравнивания.
        let encoder = self
            .writer
            .into_inner()
            .into_inner()
            .map_err(|error| error.into_error())
            .context("failed to flush buffered v4 tar payload")?;
        let compressed_payload = encoder
            .finish()
            .context("failed to finalize v4 bundle compression")?;
        Ok((compressed_payload, tar_payload_bytes))
    }
}

#[cfg(test)]
pub(crate) fn build_streamed_regular_bundle_payload(
    entry: &mut StreamedRegularFile,
    compression_level: i32,
    compression_threads: u32,
) -> Result<StreamedBundleOutcome> {
    let mut total_read_bytes = 0_u64;
    let mut total_read_duration = Duration::ZERO;
    let mut compressor = BundleCompressor::new(compression_level, compression_threads)?;

    let attempt_limit = entry.remaining_attempts();
    for attempt in 1..=attempt_limit {
        if attempt > 1 {
            let reason = match entry.reopen_for_retry() {
                Ok(super::read::ReopenDecision::Opened) => None,
                Ok(super::read::ReopenDecision::Unsupported(_)) => {
                    Some(SkipReason::UnsupportedEntry)
                }
                Err(_) if attempt == attempt_limit => Some(SkipReason::OpenFailed),
                Err(_) => continue,
            };
            if let Some(reason) = reason {
                return Ok(StreamedBundleOutcome::Skipped {
                    skipped: SkippedEntry {
                        work_ordinal: entry.descriptor.work_ordinal,
                        relative_path: entry.descriptor.relative_path.clone(),
                        reason,
                    },
                    raw_read_bytes: total_read_bytes,
                    read_duration: total_read_duration,
                });
            }
        }

        let build_started = Instant::now();
        let encoder = compressor.encoder(Vec::new())?;
        let mut writer = CountingWriter::new(encoder);
        let forced_short_limit = entry.take_forced_short_read_limit();
        let size_at_open = entry.descriptor.size_at_open;
        let mode = entry.mode;
        let relative_path = entry.descriptor.relative_path.clone();

        let append_result = {
            let mut reader = ExactFileReader::new(
                &mut entry.file,
                &entry.abs_path,
                size_at_open,
                forced_short_limit,
            );
            let append_result = append_streamed_regular_entry(
                &mut writer,
                relative_path.as_str(),
                mode,
                size_at_open,
                &mut reader,
            );
            total_read_bytes = total_read_bytes.saturating_add(reader.bytes_read);
            total_read_duration += reader.read_duration;
            append_result
        };

        match append_result {
            Ok(()) => {
                let tar_payload_bytes = writer.bytes_written();
                let compressed_payload = writer
                    .into_inner()
                    .finish()
                    .context("failed to finalize v4 streamed bundle compression")?;
                return Ok(StreamedBundleOutcome::Built(StreamedBundleBuild {
                    compressed_payload,
                    tar_payload_bytes,
                    raw_read_bytes: total_read_bytes,
                    read_duration: total_read_duration,
                    compress_duration: build_started.elapsed(),
                }));
            }
            Err(_) if attempt == attempt_limit => {
                return Ok(StreamedBundleOutcome::Skipped {
                    skipped: SkippedEntry {
                        work_ordinal: entry.descriptor.work_ordinal,
                        relative_path: entry.descriptor.relative_path.clone(),
                        reason: SkipReason::UnstableAfterRetries,
                    },
                    raw_read_bytes: total_read_bytes,
                    read_duration: total_read_duration,
                });
            }
            Err(_) => {
                entry.descriptor.best_effort = true;
            }
        }
    }

    Ok(StreamedBundleOutcome::Skipped {
        skipped: SkippedEntry {
            work_ordinal: entry.descriptor.work_ordinal,
            relative_path: entry.descriptor.relative_path.clone(),
            reason: SkipReason::UnstableAfterRetries,
        },
        raw_read_bytes: total_read_bytes,
        read_duration: total_read_duration,
    })
}

#[cfg(test)]
fn append_buffered_entry<W>(writer: &mut W, entry: &MaterializedEntry) -> Result<()>
where
    W: Write,
{
    append_materialized_entry(writer, &entry.descriptor, entry.mode, &entry.contents, None)
}

fn append_materialized_entry<W: Write>(
    writer: &mut W,
    descriptor: &BundleEntryDescriptor,
    mode: u32,
    contents: &[u8],
    progress: Option<&SourceFileGuard<'_>>,
) -> Result<()> {
    match &descriptor.kind {
        BundleEntryKind::Regular => {
            ensure!(
                contents.len() as u64 == descriptor.size_at_open,
                "v4 buffered tar entry size mismatch for {}",
                descriptor.relative_path
            );
            let mut header = new_entry_header(mode, EntryType::Regular, descriptor.size_at_open);
            append_entry_header(writer, &mut header, &descriptor.relative_path, None)?;
            // Данные уже в памяти: передаём срез кодировщику без промежуточного
            // буфера io::copy и динамических вызовов Read на каждом блоке.
            write_source_payload(writer, contents, progress).with_context(|| {
                format!("failed to encode v4 tar entry {}", descriptor.relative_path)
            })?;
            pad_zeroes(writer, descriptor.size_at_open)?;
        }
        BundleEntryKind::Symlink { target } => {
            let mut header = new_entry_header(mode, EntryType::Symlink, 0);
            append_entry_header(writer, &mut header, &descriptor.relative_path, Some(target))?;
        }
    }

    Ok(())
}

#[cfg(test)]
pub(crate) fn append_streamed_regular_entry<W, R>(
    writer: &mut W,
    relative_path: &str,
    mode: u32,
    size_at_open: u64,
    reader: &mut R,
) -> Result<()>
where
    W: Write,
    R: Read,
{
    let mut scratch = vec![0_u8; STREAM_READ_BYTES];
    append_streamed_regular_entry_with_buffer(
        writer,
        relative_path,
        mode,
        size_at_open,
        reader,
        &mut scratch,
        None,
    )
}

pub(crate) const STREAM_READ_BYTES: usize = 1024 * 1024;

pub(crate) fn append_streamed_regular_entry_with_buffer<W: Write, R: Read>(
    writer: &mut W,
    relative_path: &str,
    mode: u32,
    size_at_open: u64,
    reader: &mut R,
    scratch: &mut [u8],
    progress: Option<&SourceFileGuard<'_>>,
) -> Result<()> {
    anyhow::ensure!(
        !scratch.is_empty(),
        "streamed read buffer must not be empty"
    );
    let mut header = new_entry_header(mode, EntryType::Regular, size_at_open);
    append_entry_header(writer, &mut header, relative_path, None)?;
    copy_stream(reader, writer, relative_path, scratch, progress)?;
    pad_zeroes(writer, size_at_open)?;
    Ok(())
}

fn new_entry_header(mode: u32, entry_type: EntryType, size: u64) -> Header {
    // tar форматирует числа через временный String. Неизменяемые поля кодируем
    // один раз; каждый файл получает собственную копию всех 512 bytes заголовка.
    static TEMPLATE: LazyLock<Header> = LazyLock::new(|| {
        let mut header = Header::new_gnu();
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header
    });
    let mut header = LazyLock::force(&TEMPLATE).clone();
    write_octal(&mut header.as_old_mut().mode, u64::from(mode));
    if size < (1_u64 << 33) {
        write_octal(&mut header.as_old_mut().size, size);
    } else {
        // За пределами 11 octal-разрядов сохраняем GNU base-256 кодирование tar.
        header.set_size(size);
    }
    header.set_entry_type(entry_type);
    header
}

fn write_octal(field: &mut [u8], mut value: u64) {
    let (terminator, digits) = field.split_last_mut().expect("tar numeric field is empty");
    *terminator = 0;
    // Как tar::Header, заполняем ведущими нулями и сохраняем младшие разряды,
    // даже если вызывающий передал mode с битами вне ширины стандартного поля.
    for digit in digits.iter_mut().rev() {
        *digit = b'0' + (value & 7) as u8;
        value >>= 3;
    }
}

fn set_entry_checksum(header: &mut Header) {
    // При подсчёте checksum её собственное поле считается восемью пробелами.
    header.as_old_mut().cksum.fill(b' ');
    let checksum = header_checksum(header.as_bytes());
    write_octal(&mut header.as_old_mut().cksum, checksum);
}

fn header_checksum(bytes: &[u8; 512]) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // Проверка учитывает поддержку AVX2 процессором и ОС.
            return unsafe { header_checksum_avx2(bytes) };
        }
        // SSE2 входит в базовый набор инструкций x86-64.
        unsafe { header_checksum_sse2(bytes) }
    }
    #[cfg(not(target_arch = "x86_64"))]
    header_checksum_scalar(bytes)
}

#[cfg(any(test, not(target_arch = "x86_64")))]
fn header_checksum_scalar(bytes: &[u8; 512]) -> u64 {
    bytes.iter().map(|&byte| u64::from(byte)).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn header_checksum_avx2(bytes: &[u8; 512]) -> u64 {
    use std::arch::x86_64::*;

    let zero = _mm256_setzero_si256();
    let mut sums = zero;
    for chunk in bytes.chunks_exact(32) {
        // loadu допускает любое выравнивание; каждый chunk содержит ровно 32 байта.
        let block = unsafe { _mm256_loadu_si256(chunk.as_ptr().cast()) };
        sums = _mm256_add_epi64(sums, _mm256_sad_epu8(block, zero));
    }
    let mut lanes = [0_u64; 4];
    // В массиве достаточно места для всех четырёх 64-битных сумм.
    unsafe { _mm256_storeu_si256(lanes.as_mut_ptr().cast(), sums) };
    lanes.into_iter().sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn header_checksum_sse2(bytes: &[u8; 512]) -> u64 {
    use std::arch::x86_64::*;

    let zero = _mm_setzero_si128();
    let mut sums = zero;
    for chunk in bytes.chunks_exact(16) {
        // loadu читает только текущие 16 байт и не требует выравнивания Header.
        let block = unsafe { _mm_loadu_si128(chunk.as_ptr().cast()) };
        sums = _mm_add_epi64(sums, _mm_sad_epu8(block, zero));
    }
    let mut lanes = [0_u64; 2];
    // В массиве достаточно места для обеих 64-битных сумм.
    unsafe { _mm_storeu_si128(lanes.as_mut_ptr().cast(), sums) };
    lanes.into_iter().sum()
}

fn append_entry_header<W: Write>(
    writer: &mut W,
    header: &mut Header,
    relative_path: &str,
    link_target: Option<&str>,
) -> Result<()> {
    if header.set_path(relative_path).is_ok()
        && link_target.is_none_or(|target| header.set_link_name(target).is_ok())
    {
        set_entry_checksum(header);
        writer
            .write_all(header.as_bytes())
            .with_context(|| format!("failed to write v4 tar header for {relative_path}"))?;
        return Ok(());
    }

    append_extended_header(writer, header, relative_path, link_target)
}

#[cold]
fn append_extended_header<W: Write>(
    writer: &mut W,
    header: &mut Header,
    relative_path: &str,
    link_target: Option<&str>,
) -> Result<()> {
    let mut builder = Builder::new(DetachableWriter(Some(writer)));
    let result = match link_target {
        Some(target) => builder.append_link(header, relative_path, target),
        None => builder.append_data(header, relative_path, io::empty()),
    };
    // Builder проверяет пути и пишет GNU-расширения, но его Drop также пишет
    // конец tar. Отсоединяем поток даже при ошибке: завершающие блоки добавит
    // только receiver после последнего commit, иначе пропадут следующие файлы.
    builder.get_mut().0 = None;
    result.with_context(|| format!("failed to write extended v4 tar header for {relative_path}"))
}

struct DetachableWriter<'a, W>(Option<&'a mut W>);

impl<W: Write> Write for DetachableWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.0 {
            Some(writer) => writer.write(buf),
            None => Ok(buf.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.0 {
            Some(writer) => writer.flush(),
            None => Ok(()),
        }
    }
}

fn copy_stream<R, W>(
    reader: &mut R,
    writer: &mut W,
    relative_path: &str,
    buffer: &mut [u8],
    progress: Option<&SourceFileGuard<'_>>,
) -> Result<()>
where
    R: Read,
    W: Write,
{
    loop {
        let result = match reader.read(buffer) {
            // Сигнал не обрывает файл и не расходует попытку live-file чтения.
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result,
        };
        let read = result.with_context(|| {
            format!("failed to read streamed v4 tar payload for {relative_path}")
        })?;
        if read == 0 {
            break;
        }
        write_source_payload(writer, &buffer[..read], progress).with_context(|| {
            format!("failed to write streamed v4 tar payload for {relative_path}")
        })?;
    }
    Ok(())
}

fn write_source_payload<W: Write>(
    writer: &mut W,
    mut bytes: &[u8],
    progress: Option<&SourceFileGuard<'_>>,
) -> io::Result<()> {
    let Some(progress) = progress else {
        return writer.write_all(bytes);
    };
    // Считаем только принятый payload, включая успешный префикс перед ошибкой.
    // Заголовки/padding идут мимо; read-ahead не означает обработку компрессором.
    while !bytes.is_empty() {
        let end = bytes.len().min(STREAM_READ_BYTES);
        match writer.write(&bytes[..end]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                progress.processed(written);
                bytes = &bytes[written..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn pad_zeroes<W>(writer: &mut W, len: u64) -> Result<()>
where
    W: Write,
{
    let remaining = 512_u64.wrapping_sub(len % 512) % 512;
    if remaining == 0 {
        return Ok(());
    }
    let padding = [0_u8; 512];
    writer
        .write_all(&padding[..remaining as usize])
        .context("failed to write v4 tar padding")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::network::protocol::BundleEntryDescriptor;

    use super::*;

    #[test]
    fn observed_payload_counts_partial_writes_retries_and_write_zero() {
        struct FailingWriter(usize);
        impl Write for FailingWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.0 == 5 {
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                let written = bytes.len().min(3).min(5 - self.0);
                self.0 += written;
                Ok(written)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let progress = crate::progress::ProgressHub::new().session("sender".into(), "dump", "peer");
        {
            let source = progress.source_file("file", 9);
            let error = write_source_payload(&mut FailingWriter(0), b"123456789", Some(&source))
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            assert_eq!(progress.snapshot().current_file.unwrap().processed_bytes, 5);
        }
        {
            let source = progress.source_file("file", 9);
            write_source_payload(&mut Vec::new(), b"123456789", Some(&source)).unwrap();
            assert_eq!(progress.snapshot().current_file.unwrap().processed_bytes, 9);
        }
        let source = progress.source_file("empty-writer", 1);
        let mut empty = &mut [][..];
        assert_eq!(
            write_source_payload(&mut empty, b"x", Some(&source))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WriteZero
        );
        assert_eq!(progress.snapshot().raw_processed_bytes, 14);
    }

    #[test]
    fn source_accounting_matches_materialized_and_streamed_payload_only() {
        let progress = crate::progress::ProgressHub::new().session("sender".into(), "dump", "peer");
        let mut entry = regular("regular", vec![7; 17]);
        let mut materialized = Vec::new();
        {
            let source = progress.source_file("regular", 17);
            append_materialized_entry(
                &mut materialized,
                &entry.descriptor,
                entry.mode,
                &entry.contents,
                Some(&source),
            )
            .unwrap();
            assert_eq!(
                progress.snapshot().current_file.unwrap().processed_bytes,
                17
            );
        }
        let mut streamed = Vec::new();
        {
            let source = progress.source_file("regular", 17);
            append_streamed_regular_entry_with_buffer(
                &mut streamed,
                "regular",
                entry.mode,
                17,
                &mut Cursor::new(&entry.contents),
                &mut [0; 5],
                Some(&source),
            )
            .unwrap();
            assert_eq!(
                progress.snapshot().current_file.unwrap().processed_bytes,
                17
            );
        }
        assert_eq!(materialized, streamed);
        assert!(streamed.len() > 17);
        entry.descriptor.kind = BundleEntryKind::Symlink {
            target: "target".into(),
        };
        entry.descriptor.size_at_open = 0;
        entry.contents = Vec::new().into();
        {
            let source = progress.source_file("symlink", 0);
            append_materialized_entry(
                &mut Vec::new(),
                &entry.descriptor,
                entry.mode,
                &entry.contents,
                Some(&source),
            )
            .unwrap();
        }
        entry.descriptor.kind = BundleEntryKind::Regular;
        {
            let source = progress.source_file("empty", 0);
            append_materialized_entry(
                &mut Vec::new(),
                &entry.descriptor,
                entry.mode,
                &entry.contents,
                Some(&source),
            )
            .unwrap();
        }
        assert_eq!(progress.snapshot().raw_processed_bytes, 34);
        assert!(progress.snapshot().current_file.is_none());
    }

    #[test]
    fn observed_materialized_payload_has_bounded_progress_intervals() {
        #[derive(Default)]
        struct LargestWrite(usize);
        impl Write for LargestWrite {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 = self.0.max(bytes.len());
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let bytes = vec![0; STREAM_READ_BYTES * 3 + 17];
        let progress = crate::progress::ProgressHub::new().session("sender".into(), "dump", "peer");
        let source = progress.source_file("large", bytes.len() as u64);
        let mut observed = LargestWrite::default();
        write_source_payload(&mut observed, &bytes, Some(&source)).unwrap();
        assert_eq!(observed.0, STREAM_READ_BYTES);
        assert_eq!(progress.snapshot().raw_processed_bytes, bytes.len() as u64);
        let mut unobserved = LargestWrite::default();
        write_source_payload(&mut unobserved, &bytes, None).unwrap();
        assert_eq!(unobserved.0, bytes.len());
    }

    #[test]
    fn streamed_copy_retries_interrupted_read_without_losing_progress() {
        struct InterruptedReader {
            inner: Cursor<&'static [u8]>,
            calls: usize,
        }

        impl Read for InterruptedReader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.calls += 1;
                if self.calls == 2 {
                    return Err(io::Error::from(io::ErrorKind::Interrupted));
                }
                let len = buf.len().min(3);
                self.inner.read(&mut buf[..len])
            }
        }

        let expected: &[u8] = b"payload after an interrupted read";
        let mut reader = InterruptedReader {
            inner: Cursor::new(expected),
            calls: 0,
        };
        let mut output = Vec::new();
        copy_stream(&mut reader, &mut output, "file", &mut [0; 16], None).unwrap();
        assert_eq!(output, expected);
        assert!(reader.calls > 2);
    }

    fn regular(path: &str, contents: Vec<u8>) -> MaterializedEntry {
        MaterializedEntry {
            descriptor: BundleEntryDescriptor {
                work_ordinal: 1,
                relative_path: path.to_owned(),
                kind: BundleEntryKind::Regular,
                size_at_open: contents.len() as u64,
                best_effort: false,
            },
            mode: 0o640,
            contents: contents.into(),
        }
    }

    fn symlink(path: &str, target: &str) -> MaterializedEntry {
        let mut entry = regular(path, Vec::new());
        entry.descriptor.kind = BundleEntryKind::Symlink {
            target: target.to_owned(),
        };
        entry
    }

    fn reference_header(mode: u32, entry_type: EntryType, size: u64) -> Header {
        let mut header = Header::new_gnu();
        header.set_mode(mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_entry_type(entry_type);
        header.set_size(size);
        header
    }

    fn assert_archive_entries(compressed: &[u8], expected: &[MaterializedEntry]) {
        let decoded = zstd::stream::decode_all(compressed).unwrap();
        let mut archive = tar::Archive::new(decoded.as_slice());
        let mut actual = archive.entries().unwrap();
        for expected in expected {
            let mut entry = actual.next().expect("missing tar entry").unwrap();
            assert_eq!(
                entry.path().unwrap().to_string_lossy(),
                expected.descriptor.relative_path
            );
            assert_eq!(entry.size(), expected.descriptor.size_at_open);
            assert_eq!(entry.header().mode().unwrap(), expected.mode);
            match &expected.descriptor.kind {
                BundleEntryKind::Regular => assert!(entry.header().entry_type().is_file()),
                BundleEntryKind::Symlink { target } => {
                    assert!(entry.header().entry_type().is_symlink());
                    assert_eq!(
                        entry.link_name().unwrap().unwrap().to_string_lossy(),
                        *target
                    );
                }
            }
            let mut contents = Vec::new();
            entry.read_to_end(&mut contents).unwrap();
            assert_eq!(contents.as_slice(), expected.contents.as_slice());
        }
        assert!(actual.next().is_none());
    }

    #[test]
    fn buffered_fragments_preserve_long_paths_links_and_padding() {
        let long_path = format!("nested/{}/data.bin", "данные".repeat(22));
        let long_target = format!("../{}/target.bin", "target/".repeat(24));
        let mut entries = vec![
            regular("empty", Vec::new()),
            regular("before-boundary", vec![1; 511]),
            regular("at-boundary", vec![2; 512]),
            regular("after-boundary", vec![3; 513]),
            regular(&long_path, b"long-path payload".to_vec()),
            symlink("short-link", "../at-boundary"),
            symlink(&format!("{long_path}.link"), &long_target),
        ];
        let (mut compressed, tar_bytes) = build_compressed_bundle(&entries, 1, 1).unwrap();
        assert_eq!(
            zstd::stream::decode_all(compressed.as_slice())
                .unwrap()
                .len() as u64,
            tar_bytes
        );
        let tail = regular("tail", b"must survive the previous frame".to_vec());
        let (tail_frame, _) = build_compressed_bundle(std::slice::from_ref(&tail), 1, 1).unwrap();
        compressed.extend_from_slice(&tail_frame);
        compressed.extend_from_slice(
            &crate::sender::compress::compress_tar_termination_frame(1).unwrap(),
        );
        entries.push(tail);
        assert_archive_entries(&compressed, &entries);
    }

    #[test]
    fn streamed_long_path_does_not_terminate_following_entries() {
        let long_path = format!("{}/streamed.bin", "directory/".repeat(16));
        let streamed = regular(&long_path, vec![0xa5; 32 * 1024 + 7]);
        let mut compressor = BundleCompressor::new(1, 1).unwrap();
        let encoder = compressor.encoder(Vec::new()).unwrap();
        let mut writer = CountingWriter::new(encoder);
        append_streamed_regular_entry(
            &mut writer,
            &streamed.descriptor.relative_path,
            streamed.mode,
            streamed.descriptor.size_at_open,
            &mut Cursor::new(&streamed.contents),
        )
        .unwrap();
        let mut compressed = writer.into_inner().finish().unwrap();
        let tail = regular("tail.bin", b"complete dump".to_vec());
        let (tail_frame, _) = build_compressed_bundle(std::slice::from_ref(&tail), 1, 1).unwrap();
        compressed.extend_from_slice(&tail_frame);
        compressed.extend_from_slice(
            &crate::sender::compress::compress_tar_termination_frame(1).unwrap(),
        );
        assert_archive_entries(&compressed, &[streamed, tail]);
    }

    #[test]
    fn buffered_entry_rejects_inconsistent_size() {
        let mut entry = regular("truncated.bin", vec![1, 2]);
        entry.descriptor.size_at_open = 3;
        assert!(build_compressed_bundle(&[entry], 1, 1).is_err());
    }

    #[test]
    fn consumed_payload_release_preserves_tar_bytes_and_descriptors() {
        for threads in [1, 4] {
            let long_path = format!("{}/large.bin", "directory/".repeat(16));
            let mut entries = vec![
                regular("small", vec![0xa5; 513]),
                regular(&long_path, vec![0x5a; 256 * 1024 + 7]),
                symlink("link", &long_path),
                regular("tail", b"retained after releasing source bytes".to_vec()),
            ];
            let descriptors = entries
                .iter()
                .map(|entry| entry.descriptor.clone())
                .collect::<Vec<_>>();
            let mut compressor = BundleCompressor::new(3, threads).unwrap();
            let (reference, reference_tar_bytes) =
                build_compressed_bundle_with(&entries, &mut compressor).unwrap();
            let (compressed, tar_bytes) =
                build_compressed_bundle_into(&mut entries, &mut compressor, Vec::new()).unwrap();
            assert_eq!(tar_bytes, reference_tar_bytes);
            assert_eq!(
                zstd::stream::decode_all(compressed.as_slice()).unwrap(),
                zstd::stream::decode_all(reference.as_slice()).unwrap()
            );
            for (entry, descriptor) in entries.iter().zip(descriptors) {
                assert_eq!(entry.descriptor, descriptor);
                assert!(entry.contents.is_empty());
            }
        }
    }

    #[test]
    fn shared_payload_is_released_before_later_entry_fails() {
        use std::sync::Arc;

        use crate::sender::read::{EntryContents, ReadDecision, materialize_batch};
        use crate::sender::scan::{ScannedEntry, ScannedEntryKind};
        use crate::tests::TestDir;

        let dir = TestDir::new();
        let batch = std::array::from_fn(|index| {
            let relative_path = index.to_string();
            let abs_path = dir.path().join(&relative_path);
            std::fs::write(&abs_path, [index as u8; 333]).unwrap();
            Some(ScannedEntry {
                work_ordinal: index as u64 + 1,
                abs_path,
                relative_path,
                kind: ScannedEntryKind::Regular,
            })
        });
        let mut entries = materialize_batch(batch, 1024)
            .into_iter()
            .map(|result| match result.unwrap().unwrap() {
                ReadDecision::Accepted(entry) => entry,
                _ => panic!("fixture entry was not prefetched"),
            })
            .collect::<Vec<_>>();
        let EntryContents::Shared { bytes, .. } = &entries[0].contents else {
            panic!("fixture must use a shared slab");
        };
        let weak = Arc::downgrade(bytes);
        assert_eq!(weak.strong_count(), entries.len());
        let mut invalid = regular("invalid", vec![1, 2]);
        invalid.descriptor.size_at_open = 3;
        entries.push(invalid);
        let mut compressor = BundleCompressor::new(1, 4).unwrap();
        assert!(build_compressed_bundle_into(&mut entries, &mut compressor, Vec::new()).is_err());
        // Frame не завершён из-за следующего entry, но уже потреблённый slab
        // освобождён, даже пока его descriptors остаются в entries.
        assert!(weak.upgrade().is_none());
        assert_eq!(entries.last().unwrap().contents, [1, 2]);
    }

    #[test]
    fn threaded_compression_preserves_complete_tar_bytes() {
        let long_path = format!("{}/large.bin", "nested/".repeat(20));
        let contents = (0..8 * 1024 * 1024 + 513)
            .map(|offset| (offset % 251) as u8)
            .collect();
        let entries = vec![
            regular(&long_path, contents),
            symlink("large-link", &long_path),
            regular(
                "tail.bin",
                b"all files survive threaded compression".to_vec(),
            ),
        ];
        let (synchronous, synchronous_tar_bytes) = build_compressed_bundle(&entries, 3, 1).unwrap();
        let (mut threaded, threaded_tar_bytes) = build_compressed_bundle(&entries, 3, 4).unwrap();
        assert_eq!(threaded_tar_bytes, synchronous_tar_bytes);
        assert_eq!(
            zstd::stream::decode_all(threaded.as_slice()).unwrap(),
            zstd::stream::decode_all(synchronous.as_slice()).unwrap()
        );
        threaded.extend_from_slice(
            &crate::sender::compress::compress_tar_termination_frame(3).unwrap(),
        );
        assert_archive_entries(&threaded, &entries);
    }

    #[test]
    fn header_checksum_backends_match_for_all_byte_values_and_alignments() {
        let mut storage = [0_u8; 512 + 31];
        for value in 0..=255 {
            storage.fill(value);
            for offset in 0..32 {
                let bytes = storage[offset..offset + 512].try_into().unwrap();
                assert_header_checksum_backends(bytes, 512 * u64::from(value));
            }
        }
        for (index, byte) in storage.iter_mut().enumerate() {
            *byte = ((index * 131 + index / 7) % 256) as u8;
        }
        for offset in 0..32 {
            let bytes = storage[offset..offset + 512].try_into().unwrap();
            assert_header_checksum_backends(bytes, header_checksum_scalar(bytes));
        }
    }

    fn assert_header_checksum_backends(bytes: &[u8; 512], expected: u64) {
        assert_eq!(header_checksum_scalar(bytes), expected);
        assert_eq!(header_checksum(bytes), expected);
        #[cfg(target_arch = "x86_64")]
        {
            // SSE2 гарантирован на x86-64; AVX2 вызываем только после проверки CPU/ОС.
            assert_eq!(unsafe { header_checksum_sse2(bytes) }, expected);
            if std::arch::is_x86_feature_detected!("avx2") {
                assert_eq!(unsafe { header_checksum_avx2(bytes) }, expected);
            }
        }
    }

    #[test]
    #[ignore = "manual release-mode tar checksum throughput diagnostic"]
    fn header_checksum_throughput_comparison() {
        use std::hint::black_box;

        let bytes = std::array::from_fn(|index| (index % 251) as u8);
        let measure = |checksum: fn(&[u8; 512]) -> u64| {
            let started = Instant::now();
            for _ in 0..1_000_000 {
                black_box(checksum(black_box(&bytes)));
            }
            started.elapsed()
        };
        for round in 0..8 {
            let (before, after) = if round % 2 == 0 {
                (measure(header_checksum_scalar), measure(header_checksum))
            } else {
                let after = measure(header_checksum);
                (measure(header_checksum_scalar), after)
            };
            eprintln!(
                "tar_checksum round={round} legacy_ns={} dispatched_ns={}",
                before.as_nanos(),
                after.as_nanos()
            );
        }
    }

    #[test]
    fn numeric_headers_match_tar_crate_at_octal_and_binary_boundaries() {
        let modes = [
            0,
            1,
            0o644,
            0o7777,
            0o100644,
            0o177777,
            (1 << 21) - 1,
            1 << 21,
            (1 << 24) - 1,
            1 << 31,
            u32::MAX,
        ];
        let sizes = [
            0,
            1,
            511,
            512,
            513,
            (1 << 21) - 1,
            1 << 21,
            (1 << 24) - 1,
            1 << 24,
            (1 << 32) - 1,
            1 << 32,
            (1 << 33) - 1,
            1 << 33,
            (1 << 33) + 1,
            u64::MAX,
        ];
        for entry_type in [EntryType::Regular, EntryType::Symlink] {
            for mode in modes {
                for size in sizes {
                    let mut expected = reference_header(mode, entry_type, size);
                    let mut actual = new_entry_header(mode, entry_type, size);
                    assert_eq!(actual.as_bytes(), expected.as_bytes());
                    assert_eq!(actual.entry_size().unwrap(), size);
                    actual.set_path("nested/данные.bin").unwrap();
                    expected.set_path("nested/данные.bin").unwrap();
                    if entry_type.is_symlink() {
                        actual.set_link_name("../target.bin").unwrap();
                        expected.set_link_name("../target.bin").unwrap();
                    }
                    // Старое содержимое checksum не должно влиять на новое значение.
                    actual.as_old_mut().cksum.fill(0xa5);
                    expected.as_old_mut().cksum.fill(0x5a);
                    set_entry_checksum(&mut actual);
                    expected.set_cksum();
                    assert_eq!(
                        actual.as_bytes(),
                        expected.as_bytes(),
                        "mode={mode:#o}, size={size}, type={entry_type:?}"
                    );
                    set_entry_checksum(&mut actual);
                    assert_eq!(actual.as_bytes(), expected.as_bytes());
                }
            }
        }
    }

    #[test]
    fn emitted_headers_match_tar_builder_for_paths_and_links() {
        let paths = [
            "short.bin".to_owned(),
            "nested/./данные.bin".to_owned(),
            "a".repeat(100),
            "a".repeat(101),
            format!("{}é", "a".repeat(99)),
            format!("nested/{}/tail.bin", "данные/".repeat(30)),
        ];
        let targets = [
            None,
            Some("../target.bin".to_owned()),
            Some("b".repeat(100)),
            Some("b".repeat(101)),
            Some(format!("../{}/target.bin", "длинная-ссылка/".repeat(20))),
        ];
        for path in paths {
            for target in &targets {
                let entry_type = if target.is_some() {
                    EntryType::Symlink
                } else {
                    EntryType::Regular
                };
                for size in [0, 513, 1_u64 << 33, u64::MAX] {
                    let size = if target.is_some() { 0 } else { size };
                    let mut actual_header = new_entry_header(u32::MAX, entry_type, size);
                    let mut actual = Vec::new();
                    append_entry_header(&mut actual, &mut actual_header, &path, target.as_deref())
                        .unwrap();

                    let mut expected_header = reference_header(u32::MAX, entry_type, size);
                    let mut builder = Builder::new(Vec::new());
                    if let Some(target) = target {
                        builder
                            .append_link(&mut expected_header, &path, target)
                            .unwrap();
                    } else {
                        builder
                            .append_data(&mut expected_header, &path, io::empty())
                            .unwrap();
                    }
                    let mut expected = builder.into_inner().unwrap();
                    // Сравниваем fragment без двух финальных блоков полного tar.
                    assert!(
                        expected[expected.len() - 1024..]
                            .iter()
                            .all(|&byte| byte == 0)
                    );
                    expected.truncate(expected.len() - 1024);
                    assert_eq!(actual, expected, "path={path:?}, target={target:?}");
                }
            }
        }
    }

    #[test]
    fn optimized_tar_entries_match_builder_bytes_and_streamed_output() {
        let long_path = format!("nested/{}/data.bin", "данные/".repeat(20));
        let long_target = format!("../{}/target.bin", "цель/".repeat(30));
        let mut entries = vec![
            regular("empty", Vec::new()),
            regular("before-boundary", vec![1; 511]),
            regular("at-boundary", vec![2; 512]),
            regular("after-boundary", vec![3; 513]),
            regular(&long_path, vec![4; 4097]),
            symlink("short-link", "../at-boundary"),
            symlink("long-target", &long_target),
            symlink(&format!("{long_path}.link"), &long_target),
            regular("tail.bin", vec![5; 17]),
        ];
        entries[0].mode = 0;
        entries[2].mode = 0o100644;
        entries[4].mode = u32::MAX;
        let mut expected = Builder::new(Vec::new());
        let mut buffered = Vec::new();
        let mut streamed = Vec::new();
        for entry in &entries {
            append_buffered_entry(&mut buffered, entry).unwrap();
            match &entry.descriptor.kind {
                BundleEntryKind::Regular => {
                    let mut header = reference_header(
                        entry.mode,
                        EntryType::Regular,
                        entry.descriptor.size_at_open,
                    );
                    expected
                        .append_data(
                            &mut header,
                            &entry.descriptor.relative_path,
                            entry.contents.as_slice(),
                        )
                        .unwrap();
                    append_streamed_regular_entry(
                        &mut streamed,
                        &entry.descriptor.relative_path,
                        entry.mode,
                        entry.descriptor.size_at_open,
                        &mut Cursor::new(&entry.contents),
                    )
                    .unwrap();
                }
                BundleEntryKind::Symlink { target } => {
                    let mut header = reference_header(entry.mode, EntryType::Symlink, 0);
                    expected
                        .append_link(&mut header, &entry.descriptor.relative_path, target)
                        .unwrap();
                    append_buffered_entry(&mut streamed, entry).unwrap();
                }
            }
        }
        buffered.extend_from_slice(&[0; 1024]);
        streamed.extend_from_slice(&[0; 1024]);
        let expected = expected.into_inner().unwrap();
        assert_eq!(buffered, expected);
        assert_eq!(streamed, expected);
    }

    #[test]
    #[ignore = "compression-only A/B microbenchmark; run in release mode with --nocapture"]
    fn buffered_tar_compression_microbench() {
        use std::hint::black_box;

        const FILES: usize = 40_000;
        const REPEATS: usize = 5;
        const THREADS: u32 = 4;

        fn direct(
            entries: &[MaterializedEntry],
            compressor: &mut BundleCompressor,
        ) -> Result<(Vec<u8>, u64)> {
            // Обе стороны используют один encoder и повторное использование CCtx:
            // здесь измеряем только эффект буферизации tar writes.
            let encoder = compressor.direct_encoder(Vec::new())?;
            let mut writer = CountingWriter::new(encoder);
            for entry in entries {
                append_buffered_entry(&mut writer, entry)?;
            }
            let tar_bytes = writer.bytes_written();
            Ok((writer.into_inner().finish()?, tar_bytes))
        }

        fn measure(
            buffered: bool,
            entries: &[MaterializedEntry],
            compressor: &mut BundleCompressor,
        ) -> ((Vec<u8>, u64), Duration) {
            let started = Instant::now();
            let result = if buffered {
                build_compressed_bundle_with(black_box(entries), black_box(compressor))
            } else {
                direct(black_box(entries), black_box(compressor))
            }
            .unwrap();
            (black_box(result), started.elapsed())
        }

        fn verify(compressed: &[u8], expected: &[u8]) {
            let mut decoder = zstd::stream::read::Decoder::new(compressed).unwrap();
            let mut buffer = vec![0_u8; 128 * 1024];
            for chunk in expected.chunks(buffer.len()) {
                decoder.read_exact(&mut buffer[..chunk.len()]).unwrap();
                assert_eq!(&buffer[..chunk.len()], chunk);
            }
            assert_eq!(decoder.read(&mut buffer[..1]).unwrap(), 0);
        }

        // Та же смесь размеров и детерминированных случайных bytes, что у small fixture.
        // Подготовка данных и проверка полного tar находятся вне измеряемого интервала.
        let mut entries = Vec::with_capacity(FILES);
        for index in 0..FILES {
            let mut contents = vec![0_u8; 512 * (1 + index % 8)];
            let mut state = index as u64 + 1;
            for chunk in contents.chunks_mut(8) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
            }
            let mut path = String::new();
            let mut cursor = index;
            for level in 0..1 + index % 7 {
                let bucket = cursor % 23;
                let segment = match level % 3 {
                    0 => format!("shared-{bucket:02}"),
                    1 => format!("tier-{:02}", bucket % 11),
                    _ => format!("branch-{:02}", bucket % 17),
                };
                path.push_str(&segment);
                path.push('/');
                cursor = cursor / 23 + level + 1;
            }
            path.push_str(&format!("file-{index:05}.bin"));
            let mut entry = regular(&path, contents);
            entry.descriptor.work_ordinal = index as u64 + 1;
            entries.push(entry);
        }
        entries.sort_unstable_by(|left, right| {
            left.descriptor
                .relative_path
                .cmp(&right.descriptor.relative_path)
        });
        let raw_bytes: usize = entries.iter().map(|entry| entry.contents.len()).sum();
        let mut expected = Vec::with_capacity(raw_bytes + FILES * 512);
        for entry in &entries {
            append_buffered_entry(&mut expected, entry).unwrap();
        }

        let mut compressor = BundleCompressor::new(3, THREADS).unwrap();
        // Оба пути прогревают один context; в timed-парах меняем порядок запуска.
        for buffered in [false, true] {
            let ((compressed, tar_bytes), _) = measure(buffered, &entries, &mut compressor);
            assert_eq!(tar_bytes, expected.len() as u64);
            verify(&compressed, &expected);
        }

        let mut direct_times = [Duration::ZERO; REPEATS];
        let mut buffered_times = [Duration::ZERO; REPEATS];
        for repeat in 0..REPEATS {
            let buffered_first = repeat % 2 != 0;
            let (first, first_time) = measure(buffered_first, &entries, &mut compressor);
            let (second, second_time) = measure(!buffered_first, &entries, &mut compressor);
            let (direct, buffered, direct_time, buffered_time) = if buffered_first {
                (second, first, second_time, first_time)
            } else {
                (first, second, first_time, second_time)
            };
            assert_eq!(direct.1, expected.len() as u64);
            assert_eq!(buffered.1, expected.len() as u64);
            verify(&direct.0, &expected);
            verify(&buffered.0, &expected);
            direct_times[repeat] = direct_time;
            buffered_times[repeat] = buffered_time;
            eprintln!(
                "[tar-buffer] repeat={} threads={THREADS} buffered_first={buffered_first} direct_ms={:.3} buffered_ms={:.3} raw_bytes={raw_bytes} tar_bytes={} direct_output_bytes={} buffered_output_bytes={} compressed_bytes_equal={} decoded_bytes_equal=true",
                repeat + 1,
                direct_time.as_secs_f64() * 1000.0,
                buffered_time.as_secs_f64() * 1000.0,
                expected.len(),
                direct.0.len(),
                buffered.0.len(),
                direct.0 == buffered.0,
            );
        }
        direct_times.sort_unstable();
        buffered_times.sort_unstable();
        eprintln!(
            "[tar-buffer] summary repeats={REPEATS} direct_mean_ms={:.3} buffered_mean_ms={:.3} direct_median_ms={:.3} buffered_median_ms={:.3}",
            direct_times.iter().sum::<Duration>().as_secs_f64() * 1000.0 / REPEATS as f64,
            buffered_times.iter().sum::<Duration>().as_secs_f64() * 1000.0 / REPEATS as f64,
            direct_times[REPEATS / 2].as_secs_f64() * 1000.0,
            buffered_times[REPEATS / 2].as_secs_f64() * 1000.0,
        );
    }
}
