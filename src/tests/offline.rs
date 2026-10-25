use std::path::PathBuf;

use anyhow::Result;

use crate::network::protocol::ResumeState;

#[derive(Debug, Clone, Copy)]
pub(crate) struct OfflineDumpOptions {
    pub(crate) bundle_target_bytes: usize,
    pub(crate) compression_level: i32,
}

impl Default for OfflineDumpOptions {
    fn default() -> Self {
        Self {
            bundle_target_bytes: 8 * 1024 * 1024,
            compression_level: 3,
        }
    }
}

pub(crate) type OfflineDumpReport = crate::receiver::OfflineReceiveReport;

pub(crate) fn run_offline_dump(
    source_dir: PathBuf,
    data_dir: PathBuf,
    dump_id: &str,
    options: OfflineDumpOptions,
) -> Result<OfflineDumpReport> {
    let mut receiver =
        crate::receiver::ReceiverSession::open(data_dir, dump_id, options.compression_level)?;
    let resume_state: ResumeState = receiver.resume_state().clone();
    let plan = crate::sender::prepare_dump(
        &source_dir,
        &resume_state,
        crate::sender::BundlePlanOptions {
            bundle_target_bytes: options.bundle_target_bytes,
            compression_level: options.compression_level,
            ..Default::default()
        },
    )?;
    receiver.record_terminal_skips(&plan.new_terminal_skips)?;
    for bundle in &plan.bundles {
        receiver.commit_bundle(bundle)?;
    }
    receiver.finalize()
}
