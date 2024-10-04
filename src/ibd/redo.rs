use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;

use crate::util;

// log file size
pub const OS_FILE_LOG_BLOCK_SIZE: usize = 512;

// log file header
pub const LOG_HEADER_CREATOR_BEG: usize = 16;
pub const LOG_HEADER_CREATOR_END: usize = 48;

/// log file, see log0constants.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct LogFile {
    /// file address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// file data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// block 0: is log file header
    pub block_0: Blocks,

    /// block 1: LOG_CHECKPOINT_1 or unused
    pub block_1: Blocks,

    /// block 2: LOG_ENCRYPTION or Unused
    pub block_2: Blocks,

    /// block 3: LOG_CHECKPOINT_2 or unused
    pub block_3: Blocks,
}

impl LogFile {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            block_0: Blocks::FileHeader(LogFileHeader::new(addr, buf.clone())),
            block_1: LogCheckpoint::from(addr + 1 * OS_FILE_LOG_BLOCK_SIZE, buf.clone()),
            block_2: Blocks::Unused,
            block_3: LogCheckpoint::from(addr + 3 * OS_FILE_LOG_BLOCK_SIZE, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum Blocks {
    FileHeader(LogFileHeader),
    Block(LogBlock),
    Checkpoint(LogCheckpoint),
    Unknown(Arc<Bytes>),
    Unused,
}

/// log file header, see log0constants.h
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
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub log_uuid: u32,

    /// (8 bytes) start LSN, LSN of the start of data in this log file (with
    /// format version 1 and 2).
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub start_lsn: u64,

    /// (32 bytes) A null-terminated string which will contain either the string
    /// 'MEB' and the MySQL version if the log file was created by mysqlbackup,
    /// or 'MySQL' and the MySQL version that created the redo log file.
    #[derivative(Debug(format_with = "util::fmt_bytes_str"))]
    pub creator: Bytes,

    /// (4 bytes) 32 BITs flag, log header flags
    pub log_hdr_flags: u32,
}

impl LogFileHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            log_group_id: util::u32_val(&buf, addr),
            log_uuid: util::u32_val(&buf, addr + 4),
            start_lsn: util::u64_val(&buf, addr + 8),
            creator: buf.slice(addr + LOG_HEADER_CREATOR_BEG..addr + LOG_HEADER_CREATOR_END),
            log_hdr_flags: util::u32_val(&buf, addr + LOG_HEADER_CREATOR_END),
            buf: buf.clone(),
            addr,
        }
    }
}

/// log checkpoint, see log0constants.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct LogCheckpoint {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (8 bytes) checkpoint number
    pub checkpoint_no: u64,

    /// (8 bytes) LOG_CHECKPOINT_LSN, Checkpoint lsn. Recovery starts from this
    /// lsn and searches for the first log record group that starts since then.
    pub checkpoint_lsn: u64,

    /// (4 bytes) last checksum
    pub checksum: u32,
}

impl LogCheckpoint {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            checkpoint_no: util::u64_val(&buf, addr),
            checkpoint_lsn: util::u64_val(&buf, addr + 8),
            checksum: util::u32_val(&buf, addr + OS_FILE_LOG_BLOCK_SIZE - 4),
            buf: buf.clone(),
            addr,
        }
    }

    pub fn from(addr: usize, buf: Arc<Bytes>) -> Blocks {
        let chk = Self::new(addr, buf);
        if chk.checksum > 0 {
            Blocks::Checkpoint(chk)
        } else {
            Blocks::Unused
        }
    }
}

/// log block, see log0constants.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct LogBlock {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) log block number, see LOG_BLOCK_HDR_NO, Offset to hdr_no,
    /// which is a log block number and must be > 0. It is allowed to wrap
    /// around at LOG_BLOCK_MAX_NO. In older versions of MySQL the highest bit
    /// (LOG_BLOCK_FLUSH_BIT_MASK) of hdr_no is set to 1, if this is the first
    /// block in a call to write.
    pub hdr_no: u32,

    /// (1 bit) log flush flag, the bit from log_block_no
    pub flush_flag: bool,

    /// (2 bytes) log data length, see LOG_BLOCK_HDR_DATA_LEN, Offset to number
    /// of bytes written to this block (also header bytes).
    pub data_len: u16,

    /// (2 bytes) first record offset, see LOG_BLOCK_FIRST_REC_GROUP, An archive
    /// recovery can start parsing the log records starting from this offset in
    /// this log block, if value is not 0.
    pub first_rec_group: u16,

    /// (4 bytes) checkpoint number, see LOG_BLOCK_EPOCH_NO. Offset to epoch_no
    /// stored in this log block. The epoch_no is computed as the number of
    /// epochs passed by the value of start_lsn of the log block. Single epoch
    /// is defined as range of lsn values containing LOG_BLOCK_MAX_NO log
    /// blocks, each of OS_FILE_LOG_BLOCK_SIZE bytes. Note, that hdr_no stored
    /// in header of log block at offset=LOG_BLOCK_HDR_NO, can address the block
    /// within a given epoch, whereas epoch_no stored at
    /// offset=LOG_BLOCK_EPOCH_NO is the number of full epochs that were
    /// before. The pair <epoch_no, hdr_no> would be the absolute block number,
    /// so the epoch_no helps in discovery of unexpected end of the log during
    /// recovery in similar way as hdr_no does. @remarks The epoch_no for block
    /// that starts at start_lsn is computed as the start_lsn divided by
    /// OS_FILE_LOG_BLOCK_SIZE, and then divided by the LOG_BLOCK_MAX_NO.
    pub epoch_no: u32,

    /// (4 bytes) last checksum
    pub checksum: u32,
}

impl LogBlock {
    /// Mask used to get the highest bit in the hdr_no field. In the older MySQL
    /// versions this bit was used to mark first block in a write.
    const LOG_BLOCK_FLUSH_BIT_MASK: u32 = 0x80000000;

    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let b0 = util::u32_val(&buf, addr);
        Self {
            hdr_no: b0 & (!Self::LOG_BLOCK_FLUSH_BIT_MASK),
            flush_flag: b0 & Self::LOG_BLOCK_FLUSH_BIT_MASK > 0,
            data_len: util::u16_val(&buf, addr + 4),
            first_rec_group: util::u16_val(&buf, addr + 6),
            epoch_no: util::u32_val(&buf, addr + 8),
            checksum: util::u32_val(&buf, addr + OS_FILE_LOG_BLOCK_SIZE - 4),
            buf: buf.clone(),
            addr,
        }
    }
}
