mod frame;
mod profile;
mod readers;
mod session;
mod source;

// Этот модуль остаётся тонким фасадом sender и только реэкспортирует API.
pub use frame::{OutboundFrame, PreparedDump, PreparedFrame, prepare_dump, prepare_dump_profile};
pub use profile::{FileReadProfile, PipelineProfile, StageProfile};
pub use session::{
    SendIssue, SendIssueReason, SendOptions, SendReport, SendStatus, send_dump, send_dump_profile,
    send_finish, send_frame, send_frame_abort,
};
pub use source::SourceScanReport;

pub(crate) use frame::{FrameBuildStats, PendingFrameFiles};
pub(crate) use profile::PipelineProfileCollector;
pub(crate) use readers::{
    DirectFileStream, FileDisposition, FileReadOutcome, FileSkipReason, FileSnapshot, ReadEvent,
    ReaderPool, SnapshotStorage,
};
pub use source::scan_source;
pub(crate) use source::{InputFile, InputFileKind, PendingFileWalker};
