use std::io::{self, Write};

use anyhow::{Context, Result, ensure};
use zstd::stream::write::Encoder;
use zstd::zstd_safe::{CCtx, CParameter, ResetDirective};

pub(crate) struct BundleCompressor {
    context: CCtx<'static>,
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
        Ok(Self { context })
    }

    pub(crate) fn encoder<W: Write>(&mut self, writer: W) -> Result<Encoder<'_, W>> {
        // Новый frame независим от предыдущего, включая прерванную попытку.
        // Сохраняем настройки, рабочие буферы и потоки между бандлами.
        self.context
            .reset(ResetDirective::SessionOnly)
            .map_err(zstd_error)
            .context("failed to reset v4 bundle compressor")?;
        Ok(Encoder::with_context(writer, &mut self.context))
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
                let mut encoder = compressor.encoder(Vec::new()).unwrap();
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
            let mut unfinished = compressor.encoder(Vec::new()).unwrap();
            unfinished.write_all(&abandoned_payload).unwrap();
            drop(unfinished);

            let mut retry = compressor.encoder(Vec::new()).unwrap();
            retry.write_all(&retry_payload).unwrap();
            let compressed = retry.finish().unwrap();
            assert_eq!(
                zstd::stream::decode_all(compressed.as_slice()).unwrap(),
                retry_payload
            );
        }
    }
}
