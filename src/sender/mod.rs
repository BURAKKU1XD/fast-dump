mod frame;
mod profile;
mod readers;
mod session;
pub(crate) mod source;

// Этот модуль остаётся тонким фасадом sender и только реэкспортирует API.
pub use frame::{
    OutboundFrame, OutboundFrameV3, PreparedDump, PreparedFrame, prepare_dump, prepare_dump_profile,
};
pub use profile::{FileReadProfile, PipelineProfile, StageProfile};
pub use session::{
    SendDiscoveryIssue, SendDiscoveryIssueReason, SendDiscoveryIssueScope, SendIssue,
    SendIssueReason, SendOptions, SendReport, SendStatus, send_dump, send_dump_profile,
    send_dump_profile_v3, send_dump_v3, send_finish, send_finish_v3, send_frame, send_frame_abort,
    send_frame_abort_v3, send_frame_v3,
};
pub use source::{SenderSource, SenderSourceKind, SourceScanReport};

pub(crate) use frame::{FrameBuildStats, PendingFrameFiles};
pub(crate) use profile::PipelineProfileCollector;
pub(crate) use readers::{
    DirectFileStream, FileDisposition, FileReadOutcome, FileSkipReason, FileSnapshot, ReadEvent,
    ReaderPool, SnapshotStorage,
};
pub use source::scan_source;
pub(crate) use source::{InputFile, InputFileKind, PendingFileWalker};
