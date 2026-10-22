use anyhow::Result;

use crate::network::protocol::ResumeState;

use super::{ReceiverPaths, catalog, wal};

pub(crate) fn recover(paths: &ReceiverPaths) -> Result<ResumeState> {
    let wal_state = wal::load_state(&paths.wal_path)?;
    let terminal_skips = catalog::load_terminal_skips(&paths.catalog_path)?;
    Ok(ResumeState {
        last_committed_bundle_no: wal_state.last_committed_bundle_no,
        last_committed_work_ordinal: wal_state.last_committed_work_ordinal,
        last_committed_relative_path: wal_state.last_committed_relative_path,
        archive_len: wal_state.archive_len,
        terminal_skips,
    })
}
