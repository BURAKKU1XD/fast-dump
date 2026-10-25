use super::{DATA_HEADER_BYTES, MAX_CONTROL_BODY_BYTES, MAX_DATA_CHUNK_BYTES};
use crate::network::protocol::{
    BundleCommitDescriptor, BundlePlanDescriptor, ResumeState, SkippedEntry,
};
use crate::role::Role;
use anyhow::{Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum ChannelKind {
    Control = 1,
    Data = 2,
}

impl ChannelKind {
    pub(crate) fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Control),
            2 => Ok(Self::Data),
            _ => bail!("unknown v4 channel kind: {value}"),
        }
    }

    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub(crate) enum MessageKind {
    Hello = 1,
    HelloAck = 2,
    ResumeQuery = 3,
    ResumeState = 4,
    SkipEntries = 5,
    SkipAck = 6,
    BundlePlan = 7,
    BundleData = 8,
    BundleCommit = 9,
    BundleAbort = 10,
    BundleAbortAck = 11,
    BundleAck = 12,
    Finish = 13,
    FinishAck = 14,
    Error = 15,
}

impl MessageKind {
    pub(super) fn from_u16(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Hello),
            2 => Ok(Self::HelloAck),
            3 => Ok(Self::ResumeQuery),
            4 => Ok(Self::ResumeState),
            5 => Ok(Self::SkipEntries),
            6 => Ok(Self::SkipAck),
            7 => Ok(Self::BundlePlan),
            8 => Ok(Self::BundleData),
            9 => Ok(Self::BundleCommit),
            10 => Ok(Self::BundleAbort),
            11 => Ok(Self::BundleAbortAck),
            12 => Ok(Self::BundleAck),
            13 => Ok(Self::Finish),
            14 => Ok(Self::FinishAck),
            15 => Ok(Self::Error),
            _ => bail!("unknown v4 message kind: {value}"),
        }
    }

    pub(super) const fn max_body_len(self) -> usize {
        match self {
            Self::BundleData => MAX_DATA_CHUNK_BYTES + DATA_HEADER_BYTES,
            _ => MAX_CONTROL_BODY_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Hello {
    pub(crate) role: Role,
    pub(crate) protocol_version: u16,
    pub(crate) dump_id: String,
    pub(crate) token: String,
    pub(crate) channel_kind: ChannelKind,
    pub(crate) session_nonce: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HelloAck {
    pub(crate) role: Role,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResumeQuery {
    pub(crate) dump_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkipEntries {
    pub(crate) entries: Vec<SkippedEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkipAck {
    pub(crate) total_terminal_skips: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundlePlan {
    pub(crate) attempt_no: u32,
    pub(crate) descriptor: BundlePlanDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleData {
    pub(crate) bundle_no: u64,
    pub(crate) attempt_no: u32,
    pub(crate) is_last: bool,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleDataHeader {
    pub(crate) bundle_no: u64,
    pub(crate) attempt_no: u32,
    pub(crate) is_last: bool,
    pub(crate) payload_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleCommit {
    pub(crate) attempt_no: u32,
    pub(crate) descriptor: BundleCommitDescriptor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleAbort {
    pub(crate) bundle_no: u64,
    pub(crate) attempt_no: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BundleAbortAck {
    pub(crate) bundle_no: u64,
    pub(crate) attempt_no: u32,
    pub(crate) archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BundleAck {
    pub(crate) bundle_no: u64,
    pub(crate) last_work_ordinal: u64,
    pub(crate) archive_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Finish {
    pub(crate) final_bundle_no: u64,
    pub(crate) final_work_ordinal: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ErrorMessage {
    pub(crate) message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Message {
    Hello(Hello),
    HelloAck(HelloAck),
    ResumeQuery(ResumeQuery),
    ResumeState(ResumeState),
    SkipEntries(SkipEntries),
    SkipAck(SkipAck),
    BundlePlan(BundlePlan),
    BundleData(BundleData),
    BundleCommit(BundleCommit),
    BundleAbort(BundleAbort),
    BundleAbortAck(BundleAbortAck),
    BundleAck(BundleAck),
    Finish(Finish),
    FinishAck,
    Error(ErrorMessage),
}

impl Message {
    pub(crate) const fn kind(&self) -> MessageKind {
        match self {
            Self::Hello(_) => MessageKind::Hello,
            Self::HelloAck(_) => MessageKind::HelloAck,
            Self::ResumeQuery(_) => MessageKind::ResumeQuery,
            Self::ResumeState(_) => MessageKind::ResumeState,
            Self::SkipEntries(_) => MessageKind::SkipEntries,
            Self::SkipAck(_) => MessageKind::SkipAck,
            Self::BundlePlan(_) => MessageKind::BundlePlan,
            Self::BundleData(_) => MessageKind::BundleData,
            Self::BundleCommit(_) => MessageKind::BundleCommit,
            Self::BundleAbort(_) => MessageKind::BundleAbort,
            Self::BundleAbortAck(_) => MessageKind::BundleAbortAck,
            Self::BundleAck(_) => MessageKind::BundleAck,
            Self::Finish(_) => MessageKind::Finish,
            Self::FinishAck => MessageKind::FinishAck,
            Self::Error(_) => MessageKind::Error,
        }
    }
}
