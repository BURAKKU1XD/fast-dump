mod compress;
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod file_bench;
mod network;
mod plan;
mod read;
mod read_ahead;
mod scan;
mod source_policy;
mod stream;
mod tar;
#[cfg(test)]
mod telemetry_perf;

pub use crate::profile::{SenderProfile, StageProfile};
pub(crate) use compress::compress_tar_termination_frame;
pub(crate) use network::send_dump_controlled;
pub use network::{SendReport, send_dump, send_dump_profile};
pub use plan::BundlePlanOptions;
pub(crate) use plan::plan_dump;
pub use scan::{SourceScanReport, SourceWalkReport, scan_source, walk_source};

#[cfg(test)]
pub(crate) use plan::prepare_dump;

#[cfg(test)]
pub(crate) use read::test_hooks;

fn check_control(control: Option<&crate::runtime::SessionControl>) -> anyhow::Result<()> {
    if let Some(control) = control {
        control.check()?;
    }
    Ok(())
}
