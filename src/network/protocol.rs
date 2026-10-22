pub(crate) mod wire;

pub(crate) fn validate_dump_id(dump_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!dump_id.is_empty(), "dump_id must not be empty");
    // Один переносимый компонент: receiver добавляет суффиксы файлов состояния,
    // поэтому ни разделители/ADS, ни Windows device names здесь недопустимы.
    anyhow::ensure!(
        !matches!(dump_id, "." | "..")
            && !dump_id.ends_with('.')
            && !dump_id.ends_with(' ')
            && !dump_id.chars().any(|character| {
                character.is_ascii_control()
                    || matches!(
                        character,
                        '/' | '\\' | '<' | '>' | ':' | '"' | '|' | '?' | '*'
                    )
            }),
        "dump_id must be one portable file-name component"
    );
    let stem = dump_id
        .split('.')
        .next()
        .unwrap()
        .trim_end_matches(' ')
        .to_ascii_uppercase();
    let reserved_device = matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || stem
        .strip_prefix("COM")
        .or_else(|| stem.strip_prefix("LPT"))
        .is_some_and(|number| {
            matches!(
                number,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        });
    anyhow::ensure!(!reserved_device, "dump_id must not name a Windows device");
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BundleEntryKind {
    Regular,
    Symlink { target: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleEntryDescriptor {
    pub(crate) work_ordinal: u64,
    pub(crate) relative_path: String,
    pub(crate) kind: BundleEntryKind,
    pub(crate) size_at_open: u64,
    pub(crate) best_effort: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleDescriptor {
    pub(crate) bundle_no: u64,
    pub(crate) first_work_ordinal: u64,
    pub(crate) last_work_ordinal: u64,
    pub(crate) last_relative_path: String,
    pub(crate) entries: Vec<BundleEntryDescriptor>,
    pub(crate) raw_payload_bytes: u64,
    pub(crate) tar_payload_bytes: u64,
    pub(crate) compressed_payload_bytes: u64,
    pub(crate) compressed_xxh3_64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundlePlanDescriptor {
    pub(crate) bundle_no: u64,
    pub(crate) first_work_ordinal: u64,
    pub(crate) last_work_ordinal: u64,
    pub(crate) last_relative_path: String,
    pub(crate) entries: Vec<BundleEntryDescriptor>,
    pub(crate) raw_payload_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleCommitDescriptor {
    pub(crate) bundle_no: u64,
    pub(crate) tar_payload_bytes: u64,
    pub(crate) compressed_payload_bytes: u64,
    pub(crate) compressed_xxh3_64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedBundle {
    pub(crate) descriptor: BundleDescriptor,
    pub(crate) compressed_payload: Vec<u8>,
}

impl BundlePlanDescriptor {
    #[cfg(test)]
    pub(crate) fn from_bundle_descriptor(descriptor: &BundleDescriptor) -> Self {
        Self {
            bundle_no: descriptor.bundle_no,
            first_work_ordinal: descriptor.first_work_ordinal,
            last_work_ordinal: descriptor.last_work_ordinal,
            last_relative_path: descriptor.last_relative_path.clone(),
            entries: descriptor.entries.clone(),
            raw_payload_bytes: descriptor.raw_payload_bytes,
        }
    }

    pub(crate) fn into_bundle_descriptor(self, commit: BundleCommitDescriptor) -> BundleDescriptor {
        BundleDescriptor {
            bundle_no: self.bundle_no,
            first_work_ordinal: self.first_work_ordinal,
            last_work_ordinal: self.last_work_ordinal,
            last_relative_path: self.last_relative_path,
            entries: self.entries,
            raw_payload_bytes: self.raw_payload_bytes,
            tar_payload_bytes: commit.tar_payload_bytes,
            compressed_payload_bytes: commit.compressed_payload_bytes,
            compressed_xxh3_64: commit.compressed_xxh3_64,
        }
    }
}

impl BundleCommitDescriptor {
    pub(crate) const fn from_bundle_descriptor(descriptor: &BundleDescriptor) -> Self {
        Self {
            bundle_no: descriptor.bundle_no,
            tar_payload_bytes: descriptor.tar_payload_bytes,
            compressed_payload_bytes: descriptor.compressed_payload_bytes,
            compressed_xxh3_64: descriptor.compressed_xxh3_64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SkipReason {
    OpenFailed,
    UnstableAfterRetries,
    ReadSymlinkFailed,
    UnsupportedEntry,
}

impl SkipReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OpenFailed => "open_failed",
            Self::UnstableAfterRetries => "unstable_after_retries",
            Self::ReadSymlinkFailed => "read_symlink_failed",
            Self::UnsupportedEntry => "unsupported_entry",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SkippedEntry {
    pub(crate) work_ordinal: u64,
    pub(crate) relative_path: String,
    pub(crate) reason: SkipReason,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResumeState {
    pub(crate) last_committed_bundle_no: u64,
    pub(crate) last_committed_work_ordinal: u64,
    pub(crate) last_committed_relative_path: String,
    pub(crate) archive_len: u64,
    pub(crate) terminal_skips: Vec<SkippedEntry>,
}

#[cfg(test)]
mod dump_id_tests {
    use super::validate_dump_id;

    #[test]
    fn dump_id_rejects_paths_streams_devices_and_nonportable_names() {
        for invalid in [
            "",
            ".",
            "..",
            "../outside",
            "a/b",
            "a\\b",
            "C:dump",
            "dump:stream",
            "bad\0name",
            "bad\nname",
            "bad\u{7f}name",
            "bad<name",
            "bad>name",
            "bad|name",
            "bad?name",
            "bad*name",
            "bad\"name",
            "trailing.",
            "trailing ",
            "CON",
            "nul.txt",
            "aUx.backup",
            "prn",
            "COM1",
            "com9.data",
            "lpt1",
            "LPT9",
            "COM¹",
            "LPT².log",
            "com³",
            "CONIN$",
            "conout$",
            "NUL .log",
        ] {
            assert!(validate_dump_id(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn dump_id_keeps_unicode_spaces_and_ordinary_dotted_names() {
        for valid in [
            "dump-01",
            "my dump",
            "архив 🙂",
            "日本語",
            ".hidden",
            "a..b",
            "COM10",
            "LPT0",
            "nul-data",
            "CONSOLE",
            "dump.v4",
        ] {
            validate_dump_id(valid).unwrap_or_else(|error| panic!("rejected {valid:?}: {error}"));
        }
    }
}
