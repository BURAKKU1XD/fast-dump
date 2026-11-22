use std::hash::Hasher;

use twox_hash::xxhash3_64::{DEFAULT_SECRET_LENGTH, RawHasher, SecretBuffer};

// Статический стандартный secret не требует heap-аллокации на каждый bundle.
// Реализация выбирает AVX2/SSE2/NEON во время выполнения, сохраняя XXH3-64 v4.
pub(crate) struct Xxh3(RawHasher<&'static [u8; DEFAULT_SECRET_LENGTH]>);

impl Xxh3 {
    pub(crate) fn new() -> Self {
        Self(RawHasher::new(SecretBuffer::default()))
    }

    #[inline]
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }

    #[inline]
    pub(crate) fn digest(&self) -> u64 {
        self.0.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_checksum_matches_v4_for_lengths_alignments_and_chunk_boundaries() {
        let bytes: Vec<_> = (0..16 * 1024 + 32)
            .map(|index| ((index * 131 + index / 251) % 256) as u8)
            .collect();
        let lengths =
            (0..=260).chain([511, 512, 513, 1023, 1024, 1025, 4095, 4096, 4097, 16 * 1024]);
        for len in lengths {
            for offset in [0, 1, 7, 15, 31] {
                let input = &bytes[offset..offset + len];
                let expected = xxhash_rust::xxh3::xxh3_64(input);
                for chunk_size in [1, 7, 63, 64, 65, 255, 256, 257, 4096, 16384] {
                    let mut checksum = Xxh3::new();
                    checksum.update(&[]);
                    for chunk in input.chunks(chunk_size) {
                        checksum.update(chunk);
                        checksum.update(&[]);
                    }
                    assert_eq!(
                        checksum.digest(),
                        expected,
                        "len={len}, offset={offset}, chunk={chunk_size}"
                    );
                    assert_eq!(checksum.digest(), expected);
                    // Чтение digest не завершает состояние: следующие update допустимы.
                    checksum.update(b"tail");
                    let mut reference = xxhash_rust::xxh3::Xxh3Default::new();
                    reference.update(input);
                    reference.update(b"tail");
                    assert_eq!(checksum.digest(), reference.digest());
                }
            }
        }
    }

    #[test]
    fn streaming_checksum_matches_v4_across_network_chunks() {
        let bytes: Vec<_> = (0..4 * 1024 * 1024 + 513)
            .map(|index| (index % 251) as u8)
            .collect();
        for chunk_size in [65535, 1024 * 1024, 4 * 1024 * 1024] {
            let mut checksum = Xxh3::new();
            let mut reference = xxhash_rust::xxh3::Xxh3Default::new();
            for chunk in bytes.chunks(chunk_size) {
                checksum.update(chunk);
                reference.update(chunk);
                assert_eq!(checksum.digest(), reference.digest());
            }
            assert_eq!(checksum.digest(), xxhash_rust::xxh3::xxh3_64(&bytes));
        }
    }

    #[test]
    #[ignore = "manual release-mode checksum throughput diagnostic"]
    fn checksum_throughput_comparison() {
        use std::hint::black_box;
        use std::time::Instant;

        let bytes: Vec<_> = (0..4 * 1024 * 1024)
            .map(|index| (index % 251) as u8)
            .collect();
        for round in 0..8 {
            let legacy = || {
                let started = Instant::now();
                for _ in 0..256 {
                    let mut checksum = xxhash_rust::xxh3::Xxh3Default::new();
                    checksum.update(black_box(&bytes));
                    black_box(checksum.digest());
                }
                started.elapsed()
            };
            let dispatched = || {
                let started = Instant::now();
                for _ in 0..256 {
                    let mut checksum = Xxh3::new();
                    checksum.update(black_box(&bytes));
                    black_box(checksum.digest());
                }
                started.elapsed()
            };
            let (before, after) = if round % 2 == 0 {
                (legacy(), dispatched())
            } else {
                let after = dispatched();
                (legacy(), after)
            };
            eprintln!(
                "checksum round={round} legacy_ns={} dispatched_ns={}",
                before.as_nanos(),
                after.as_nanos()
            );
        }
    }
}
