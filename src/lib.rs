pub mod app;
pub mod cli;
pub mod network;
mod profile;
pub mod receiver;
pub mod role;
pub mod sender;
pub mod telemetry;

#[cfg(test)]
mod tests;

#[cfg(test)]
use std::path::PathBuf;

#[cfg(test)]
use anyhow::Result;

#[cfg(test)]
use crate::network::protocol::ResumeState;

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct OfflineDumpOptions {
    pub(crate) bundle_target_bytes: usize,
    pub(crate) compression_level: i32,
}

#[cfg(test)]
impl Default for OfflineDumpOptions {
    fn default() -> Self {
        Self {
            bundle_target_bytes: 8 * 1024 * 1024,
            compression_level: 3,
        }
    }
}

#[cfg(test)]
pub(crate) type OfflineDumpReport = crate::receiver::OfflineReceiveReport;

#[cfg(test)]
pub(crate) fn run_offline_dump(
    source_dir: PathBuf,
    data_dir: PathBuf,
    dump_id: &str,
    options: OfflineDumpOptions,
) -> Result<OfflineDumpReport> {
    let mut receiver =
        receiver::ReceiverSession::open(data_dir, dump_id, options.compression_level)?;
    let resume_state: ResumeState = receiver.resume_state().clone();
    let plan = sender::prepare_dump(
        &source_dir,
        &resume_state,
        sender::BundlePlanOptions {
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
