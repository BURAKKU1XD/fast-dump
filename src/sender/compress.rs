use std::io::{self, Write};

use anyhow::{Context, Result, ensure};
#[cfg(test)]
use zstd::stream::write::Encoder;
use zstd::zstd_safe::zstd_sys::ZSTD_EndDirective;
use zstd::zstd_safe::{CCtx, CParameter, InBuffer, OutBuffer, ResetDirective};

pub(crate) struct BundleCompressor {
    context: CCtx<'static>,
    compression_level: i32,
    compression_threads: u32,
    unfinished: bool,
}

impl BundleCompressor {
    pub(crate) fn new(compression_level: i32, compression_threads: u32) -> Result<Self> {
        ensure!(
            compression_threads > 0,
            "v4 compression threads must be positive"
        );
        let mut context = CCtx::try_create().context("failed to allocate v4 bundle compressor")?;
        context
            .set_parameter(CParameter::CompressionLevel(compression_level))
            .map_err(zstd_error)
            .context("failed to configure v4 compression level")?;
        // При одном потоке сохраняем синхронный путь без очередей zstd;
        // несколько работников сжимают блоки одного frame параллельно.
        let workers = if compression_threads > 1 {
            compression_threads
        } else {
            0
        };
        context
            .set_parameter(CParameter::NbWorkers(workers))
            .map_err(zstd_error)
            .context("failed to configure v4 compression threads")?;
        Ok(Self {
            context,
            compression_level,
            compression_threads,
            unfinished: false,
        })
    }

    fn reset(&mut self) -> Result<()> {
        if self.unfinished {
            // У zstd 1.5.7 SessionOnly после отмены может оставить jobReady,
            // хотя descriptors jobs уже сброшены. Новый CCtx исключает запуск
            // worker с пустым descriptor; Drop старого сначала завершает workers.
            *self = Self::new(self.compression_level, self.compression_threads)?;
            return Ok(());
        }
        // Завершённые frames безопасно сохраняют workspace и native workers.
        self.context
            .reset(ResetDirective::SessionOnly)
            .map_err(zstd_error)
            .context("failed to reset v4 bundle compressor")?;
        Ok(())
    }

    pub(crate) fn direct_encoder<O: CompressionOutput>(
        &mut self,
        output: O,
    ) -> Result<DirectEncoder<'_, O>> {
        self.reset()?;
        self.unfinished = true;
        Ok(DirectEncoder {
            context: &mut self.context,
            unfinished: &mut self.unfinished,
            output,
        })
    }

    #[cfg(test)]
    pub(crate) fn encoder<W: Write>(&mut self, writer: W) -> Result<Encoder<'_, W>> {
        self.reset()?;
        // Только test oracle: внешний Encoder не сообщает нам об успешном finish.
        self.unfinished = true;
        Ok(Encoder::with_context(writer, &mut self.context))
    }
}

// Output владеет конечным Vec или чанком очереди. Zstd пишет прямо в его
// свободную ёмкость, без промежуточного zio::Writer и последующего memcpy.
pub(crate) trait CompressionOutput {
    fn compress(
        &mut self,
        context: &mut CCtx<'static>,
        input: &mut InBuffer<'_>,
        directive: ZSTD_EndDirective,
    ) -> io::Result<usize>;

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl CompressionOutput for Vec<u8> {
    fn compress(
        &mut self,
        context: &mut CCtx<'static>,
        input: &mut InBuffer<'_>,
        directive: ZSTD_EndDirective,
    ) -> io::Result<usize> {
        if self.len() == self.capacity() {
            self.try_reserve(32 * 1024).map_err(io::Error::other)?;
        }
        compress_into_vec(context, input, directive, self, usize::MAX)
    }
}

pub(crate) fn compress_into_vec(
    context: &mut CCtx<'static>,
    input: &mut InBuffer<'_>,
    directive: ZSTD_EndDirective,
    bytes: &mut Vec<u8>,
    limit: usize,
) -> io::Result<usize> {
    let start = bytes.len();
    if start >= limit || start == bytes.capacity() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "compressed output buffer has no space",
        ));
    }
    if bytes.capacity() <= limit {
        // OutBuffer обновляет длину Vec только на инициализированный zstd префикс.
        // Обычные Vec и чанки 4 MiB не требуют предварительного обнуления.
        let mut output = OutBuffer::around_pos(bytes, start);
        return context
            .compress_stream2(&mut output, input, directive)
            .map_err(zstd_error);
    }

    // Allocator вправе дать ёмкость больше запрошенной. Редкий fallback ограничивает
    // zstd размером wire-чанка через безопасный slice, не вводя свой unsafe WriteBuf.
    bytes.resize(limit, 0);
    let (result, written) = {
        let mut output = OutBuffer::around_pos(&mut bytes[..limit], start);
        let result = context.compress_stream2(&mut output, input, directive);
        (result, output.pos())
    };
    bytes.truncate(written);
    result.map_err(zstd_error)
}

pub(crate) struct DirectEncoder<'a, O> {
    context: &'a mut CCtx<'static>,
    unfinished: &'a mut bool,
    output: O,
}

impl<O: CompressionOutput> DirectEncoder<'_, O> {
    pub(crate) fn finish(mut self) -> io::Result<O> {
        let mut input = InBuffer::around(&[]);
        // End может требовать несколько выходных буферов, особенно с zstd workers.
        while self
            .output
            .compress(self.context, &mut input, ZSTD_EndDirective::ZSTD_e_end)?
            != 0
        {}
        *self.unfinished = false;
        Ok(self.output)
    }
}

impl<O: CompressionOutput> Write for DirectEncoder<'_, O> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let mut input = InBuffer::around(bytes);
        loop {
            self.output
                .compress(self.context, &mut input, ZSTD_EndDirective::ZSTD_e_continue)?;
            if input.pos != 0 {
                // После потребления input возвращаемся сразу: возможная ошибка
                // отправки полного чанка относится уже к следующему вызову Write.
                return Ok(input.pos);
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut input = InBuffer::around(&[]);
        while self
            .output
            .compress(self.context, &mut input, ZSTD_EndDirective::ZSTD_e_flush)?
            != 0
        {}
        self.output.flush()
    }
}

fn zstd_error(code: usize) -> io::Error {
    io::Error::other(zstd::zstd_safe::get_error_name(code))
}

pub(crate) fn compress_bundle(bytes: &[u8], compression_level: i32) -> Result<Vec<u8>> {
    zstd::bulk::compress(bytes, compression_level)
        .with_context(|| "failed to compress v4 bundle payload")
}

pub(crate) fn compress_tar_termination_frame(compression_level: i32) -> Result<Vec<u8>> {
    compress_bundle(&[0_u8; 1024], compression_level)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reused_context_produces_independent_frames() {
        for threads in [1, 4] {
            let mut compressor = BundleCompressor::new(3, threads).unwrap();
            let mut concatenated_frames = Vec::new();
            let mut concatenated_payloads = Vec::new();
            for payload in [vec![0xa5; 1024 * 1024 + 7], Vec::new(), vec![0x5a; 513]] {
                let mut encoder = compressor.direct_encoder(Vec::new()).unwrap();
                encoder.write_all(&payload).unwrap();
                let compressed = encoder.finish().unwrap();
                assert_eq!(
                    zstd::stream::decode_all(compressed.as_slice()).unwrap(),
                    payload
                );
                concatenated_frames.extend_from_slice(&compressed);
                concatenated_payloads.extend_from_slice(&payload);
            }
            assert_eq!(
                zstd::stream::decode_all(concatenated_frames.as_slice()).unwrap(),
                concatenated_payloads
            );
        }
    }

    #[test]
    fn reused_context_discards_unfinished_frame_before_retry() {
        let abandoned_payload = vec![0xa5; 8 * 1024 * 1024 + 7];
        let retry_payload = vec![0x5a; 1024 * 1024 + 513];
        for threads in [1, 4] {
            let mut compressor = BundleCompressor::new(3, threads).unwrap();
            let mut unfinished = compressor.direct_encoder(Vec::new()).unwrap();
            unfinished.write_all(&abandoned_payload).unwrap();
            drop(unfinished);

            let mut retry = compressor.direct_encoder(Vec::new()).unwrap();
            retry.write_all(&retry_payload).unwrap();
            let compressed = retry.finish().unwrap();
            assert_eq!(
                zstd::stream::decode_all(compressed.as_slice()).unwrap(),
                retry_payload
            );
        }
    }

    fn payload(size: usize) -> Vec<u8> {
        let mut state = 0xd9f4_1a83_26b5_7ec1_u64;
        let mut bytes = vec![0; size];
        for chunk in bytes.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        bytes
    }

    #[test]
    fn direct_output_matches_reference_frames_with_flush_and_context_reuse() {
        let bytes = payload(9 * 1024 * 1024 + 37);
        for threads in [1, 4] {
            let mut compressor = BundleCompressor::new(3, threads).unwrap();
            for flush in [false, true] {
                let write = |encoder: &mut dyn Write| {
                    assert_eq!(encoder.write(&[]).unwrap(), 0);
                    for (index, chunk) in bytes.chunks(128 * 1024 + 11).enumerate() {
                        encoder.write_all(chunk).unwrap();
                        if flush && index == 3 {
                            encoder.flush().unwrap();
                        }
                    }
                };
                let mut reference = compressor.encoder(Vec::new()).unwrap();
                write(&mut reference);
                let expected = reference.finish().unwrap();
                let mut direct = compressor.direct_encoder(Vec::with_capacity(1)).unwrap();
                write(&mut direct);
                let actual = direct.finish().unwrap();
                assert!(actual == expected, "direct output changed zstd frame bytes");
                assert_eq!(zstd::stream::decode_all(actual.as_slice()).unwrap(), bytes);
            }
        }
    }

    struct TinyOutput {
        pending: Vec<u8>,
        bytes: Vec<u8>,
        limit: usize,
        calls: usize,
        fail_after: Option<usize>,
    }

    impl TinyOutput {
        fn new(limit: usize, capacity: usize) -> Self {
            Self {
                pending: Vec::with_capacity(capacity),
                bytes: Vec::new(),
                limit,
                calls: 0,
                fail_after: None,
            }
        }

        fn drain(&mut self) {
            assert!(self.pending.len() <= self.limit);
            self.bytes.extend_from_slice(&self.pending);
            self.pending.clear();
        }
    }

    impl CompressionOutput for TinyOutput {
        fn compress(
            &mut self,
            context: &mut CCtx<'static>,
            input: &mut InBuffer<'_>,
            directive: ZSTD_EndDirective,
        ) -> io::Result<usize> {
            if self.fail_after.is_some_and(|limit| self.calls >= limit) {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "output stopped"));
            }
            self.calls += 1;
            if self.pending.len() == self.limit {
                self.drain();
            }
            compress_into_vec(context, input, directive, &mut self.pending, self.limit)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.drain();
            Ok(())
        }
    }

    #[test]
    fn tiny_output_buffers_finish_and_preserve_bytes_without_stalling() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let bytes = payload(4099);
            for threads in [1, 4] {
                let mut compressor = BundleCompressor::new(3, threads).unwrap();
                for (limit, capacity) in [(1, 1), (7, 7), (7, 31)] {
                    let mut encoder = compressor
                        .direct_encoder(TinyOutput::new(limit, capacity))
                        .unwrap();
                    encoder.write_all(&bytes[..17]).unwrap();
                    encoder.flush().unwrap();
                    encoder.write_all(&bytes[17..]).unwrap();
                    let mut output = encoder.finish().unwrap();
                    output.drain();
                    assert!(output.calls > 2);
                    assert_eq!(
                        zstd::stream::decode_all(output.bytes.as_slice()).unwrap(),
                        bytes
                    );
                }
            }
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(15))
            .expect("direct compression stalled with short output buffers");
        worker.join().unwrap();
    }

    #[test]
    fn output_error_propagates_and_context_can_start_a_clean_retry() {
        for threads in [1, 4] {
            let mut compressor = BundleCompressor::new(3, threads).unwrap();
            let mut output = TinyOutput::new(7, 7);
            output.fail_after = Some(2);
            let mut encoder = compressor.direct_encoder(output).unwrap();
            let result = encoder
                .write_all(&payload(4099))
                .and_then(|()| encoder.finish().map(|_| ()));
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
            let mut retry = compressor.direct_encoder(Vec::new()).unwrap();
            retry.write_all(b"complete retry").unwrap();
            let bytes = retry.finish().unwrap();
            assert_eq!(
                zstd::stream::decode_all(bytes.as_slice()).unwrap(),
                b"complete retry"
            );
        }
    }

    #[test]
    fn concurrent_abandoned_worker_frames_retry_with_independent_valid_output() {
        struct InterruptedOutput {
            bytes: Vec<u8>,
            produced: usize,
        }

        impl CompressionOutput for InterruptedOutput {
            fn compress(
                &mut self,
                context: &mut CCtx<'static>,
                input: &mut InBuffer<'_>,
                directive: ZSTD_EndDirective,
            ) -> io::Result<usize> {
                if self.produced >= 4 * 1024 * 1024 {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "cancel frame"));
                }
                self.bytes.clear();
                let result =
                    compress_into_vec(context, input, directive, &mut self.bytes, usize::MAX);
                self.produced += self.bytes.len();
                result
            }
        }

        let bytes = payload(32 * 1024 * 1024 + 17);
        let start = std::sync::Barrier::new(4);
        // Несколько сессий нагружают native pools: отмена может попасть между
        // подготовкой job и его передачей занятому worker (jobReady в zstd).
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let bytes = &bytes;
                let start = &start;
                scope.spawn(move || {
                    let mut compressor = BundleCompressor::new(1, 4).unwrap();
                    start.wait();
                    for round in 0..8 {
                        let error = {
                            let output = InterruptedOutput {
                                bytes: Vec::with_capacity(4 * 1024 * 1024),
                                produced: 0,
                            };
                            let mut encoder = compressor.direct_encoder(output).unwrap();
                            encoder
                                .write_all(bytes)
                                .and_then(|()| encoder.finish().map(|_| ()))
                                .unwrap_err()
                        };
                        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
                        let expected = vec![round; 256 * 1024 + 17];
                        let mut retry = compressor.direct_encoder(Vec::new()).unwrap();
                        retry.write_all(&expected).unwrap();
                        let frame = retry.finish().unwrap();
                        assert_eq!(
                            zstd::stream::decode_all(frame.as_slice()).unwrap(),
                            expected
                        );
                    }
                });
            }
        });
    }
}
