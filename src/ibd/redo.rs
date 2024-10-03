use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;

use crate::util;

// log file size
pub const OS_FILE_LOG_BLOCK_SIZE: usize = 512;

// log file header
pub const LOG_HEADER_CREATOR_BEG: usize = 16;
pub const LOG_HEADER_CREATOR_END: usize = 48;

/// Log file header, see log0constants.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct LogFileHeader {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) log group id, Log file header format identifier (32-bit
    /// unsigned big-endian integer). This used to be called LOG_GROUP_ID and
    /// always written as 0, because InnoDB never supported more than one copy
    /// of the redo log.
    pub log_group_id: u32,

    /// (4 bytes) log uuid, Offset within the log file header, to the field
    /// which stores the log_uuid. The log_uuid is chosen after a new data
    /// directory is initialized, and allows to detect situation, in which some
    /// of log files came from other data directory (detection is performed on
    /// startup, before starting recovery).
    pub log_uuid: u32,

    /// (8 bytes) start LSN, LSN of the start of data in this log file (with
    /// format version 1 and 2).
    pub start_lsn: u64,

    /// (32 bytes) A null-terminated string which will contain either the string
    /// 'MEB' and the MySQL version if the log file was created by mysqlbackup,
    /// or 'MySQL' and the MySQL version that created the redo log file.
    pub creator: Bytes,
}

impl LogFileHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            log_group_id: util::u32_val(&buf, addr),
            log_uuid: util::u32_val(&buf, addr + 4),
            start_lsn: util::u64_val(&buf, addr + 8),
            creator: buf.slice(addr + LOG_HEADER_CREATOR_BEG..addr + LOG_HEADER_CREATOR_END),
            buf: buf.clone(),
            addr,
        }
    }
}
