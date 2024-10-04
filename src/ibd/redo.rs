use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use super::page::{PageNumber, SpaceId};
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

    /// other block is log block
    pub log_block_list: Vec<Blocks>,
}

impl LogFile {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let mut blocks = vec![];
        let mut ptr = addr + 4 * OS_FILE_LOG_BLOCK_SIZE;
        loop {
            if ptr >= buf.len() {
                break;
            }
            blocks.push(LogBlock::new(ptr, buf.clone()).into());
            ptr += OS_FILE_LOG_BLOCK_SIZE;
        }

        Self {
            block_0: Blocks::FileHeader(LogFileHeader::new(addr, buf.clone())),
            block_1: LogCheckpoint::new(addr + OS_FILE_LOG_BLOCK_SIZE, buf.clone()).into(),
            block_2: Blocks::Unused,
            block_3: LogCheckpoint::new(addr + OS_FILE_LOG_BLOCK_SIZE * 3, buf.clone()).into(),
            log_block_list: blocks,
            buf: buf.clone(),
            addr,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum Blocks {
    FileHeader(LogFileHeader),
    Checkpoint(LogCheckpoint),
    Block(LogBlock),
    Unused,
}

impl From<LogBlock> for Blocks {
    fn from(value: LogBlock) -> Self {
        if value.checksum > 0 {
            Blocks::Block(value)
        } else {
            Blocks::Unused
        }
    }
}

impl From<LogCheckpoint> for Blocks {
    fn from(value: LogCheckpoint) -> Self {
        if value.checksum > 0 {
            Blocks::Checkpoint(value)
        } else {
            Blocks::Unused
        }
    }
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
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
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
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub checksum: u32,

    /// log record
    pub record: Option<LogRecord>,
}

impl LogBlock {
    /// Mask used to get the highest bit in the hdr_no field. In the older MySQL
    /// versions this bit was used to mark first block in a write.
    const LOG_BLOCK_FLUSH_BIT_MASK: u32 = 0x80000000;

    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let b0 = util::u32_val(&buf, addr);

        let first_rec_offset = util::u16_val(&buf, addr + 6);
        let rec = if first_rec_offset > 0 {
            Some(LogRecord::new(
                addr + (first_rec_offset as usize),
                buf.clone(),
            ))
        } else {
            None
        };

        Self {
            hdr_no: b0 & (!Self::LOG_BLOCK_FLUSH_BIT_MASK),
            flush_flag: b0 & Self::LOG_BLOCK_FLUSH_BIT_MASK > 0,
            data_len: util::u16_val(&buf, addr + 4),
            first_rec_group: first_rec_offset,
            epoch_no: util::u32_val(&buf, addr + 8),
            record: rec,
            checksum: util::u32_val(&buf, addr + OS_FILE_LOG_BLOCK_SIZE - 4),
            buf: buf.clone(),
            addr,
        }
    }
}

/// log record
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct LogRecord {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// log record header
    pub log_rec_hdr: LogRecordHeader,
}

impl LogRecord {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            log_rec_hdr: LogRecordHeader::new(addr, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// types of a redo log record
#[repr(u8)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum LogRecordTypes {
    /// if the mtr contains only one log record for one page, i.e.,
    /// write_initial_log_record has been called only once, this flag is ORed to
    /// the type of that first log record
    MLOG_SINGLE_REC_FLAG = 128,

    /// one byte is written
    MLOG_1BYTE = 1,

    /// 2 bytes ...
    MLOG_2BYTES = 2,

    /// 4 bytes ...
    MLOG_4BYTES = 4,

    /// 8 bytes ...
    MLOG_8BYTES = 8,

    /// Record insert
    MLOG_REC_INSERT_8027 = 9,

    /// Mark clustered index record deleted
    MLOG_REC_CLUST_DELETE_MARK_8027 = 10,

    /// Mark secondary index record deleted
    MLOG_REC_SEC_DELETE_MARK = 11,

    /// update of a record, preserves record field sizes
    MLOG_REC_UPDATE_IN_PLACE_8027 = 13,

    /// Delete a record from a page
    MLOG_REC_DELETE_8027 = 14,

    /// Delete record list end on index page
    MLOG_LIST_END_DELETE_8027 = 15,

    /// Delete record list start on index page
    MLOG_LIST_START_DELETE_8027 = 16,

    /// Copy record list end to a new created index page
    MLOG_LIST_END_COPY_CREATED_8027 = 17,

    /// Reorganize an index page in ROW_FORMAT=REDUNDANT
    MLOG_PAGE_REORGANIZE_8027 = 18,

    /// Create an index page
    MLOG_PAGE_CREATE = 19,

    /// Insert entry in an undo log
    MLOG_UNDO_INSERT = 20,

    /// erase an undo log page end
    MLOG_UNDO_ERASE_END = 21,

    /// initialize a page in an undo log
    MLOG_UNDO_INIT = 22,

    /// reuse an insert undo log header
    MLOG_UNDO_HDR_REUSE = 24,

    /// create an undo log header
    MLOG_UNDO_HDR_CREATE = 25,

    /// mark an index record as the predefined minimum record
    MLOG_REC_MIN_MARK = 26,

    /// initialize an ibuf bitmap page
    MLOG_IBUF_BITMAP_INIT = 27,

    /// Current LSN
    MLOG_LSN = 28,

    /// this means that a file page is taken into use and the prior contents of
    /// the page should be ignored: in recovery we must not trust the lsn values
    /// stored to the file page. Note: it's deprecated because it causes crash
    /// recovery problem in bulk create index, and actually we don't need to reset
    /// page lsn in recv_recover_page_func() now.
    MLOG_INIT_FILE_PAGE = 29,

    /// write a string to a page
    MLOG_WRITE_STRING = 30,

    /// If a single mtr writes several log records, this log record ends the
    /// sequence of these records
    MLOG_MULTI_REC_END = 31,

    /// dummy log record used to pad a log block full
    MLOG_DUMMY_RECORD = 32,

    /// log record about creating an .ibd file, with format
    MLOG_FILE_CREATE = 33,

    /// rename a tablespace file that starts with (space_id,page_no)
    MLOG_FILE_RENAME = 34,

    /// delete a tablespace file that starts with (space_id,page_no)
    MLOG_FILE_DELETE = 35,

    /// mark a compact index record as the predefined minimum record
    MLOG_COMP_REC_MIN_MARK = 36,

    /// create a compact index page
    MLOG_COMP_PAGE_CREATE = 37,

    /// compact record insert
    MLOG_COMP_REC_INSERT_8027 = 38,

    /// mark compact clustered index record deleted
    MLOG_COMP_REC_CLUST_DELETE_MARK_8027 = 39,

    /// mark compact secondary index record deleted; this log record type is
    /// redundant, as MLOG_REC_SEC_DELETE_MARK is independent of the record
    /// format.
    MLOG_COMP_REC_SEC_DELETE_MARK = 40,

    /// update of a compact record, preserves record field sizes
    MLOG_COMP_REC_UPDATE_IN_PLACE_8027 = 41,

    /// delete a compact record from a page
    MLOG_COMP_REC_DELETE_8027 = 42,

    /// delete compact record list end on index page
    MLOG_COMP_LIST_END_DELETE_8027 = 43,

    /// * delete compact record list start on index page
    MLOG_COMP_LIST_START_DELETE_8027 = 44,

    /// copy compact record list end to a new created index page
    MLOG_COMP_LIST_END_COPY_CREATED_8027 = 45,

    /// reorganize an index page
    MLOG_COMP_PAGE_REORGANIZE_8027 = 46,

    /// write the node pointer of a record on a compressed non-leaf B-tree page
    MLOG_ZIP_WRITE_NODE_PTR = 48,

    /// write the BLOB pointer of an externally stored column on a compressed page
    MLOG_ZIP_WRITE_BLOB_PTR = 49,

    /// write to compressed page header
    MLOG_ZIP_WRITE_HEADER = 50,

    /// compress an index page
    MLOG_ZIP_PAGE_COMPRESS = 51,

    /// compress an index page without logging it's image
    MLOG_ZIP_PAGE_COMPRESS_NO_DATA_8027 = 52,

    /// reorganize a compressed page
    MLOG_ZIP_PAGE_REORGANIZE_8027 = 53,

    /// Create a R-Tree index page
    MLOG_PAGE_CREATE_RTREE = 57,

    /// create a R-tree compact page
    MLOG_COMP_PAGE_CREATE_RTREE = 58,

    /// this means that a file page is taken into use. We use it to replace
    /// MLOG_INIT_FILE_PAGE.
    MLOG_INIT_FILE_PAGE2 = 59,

    /// Table is being truncated. (Marked only for file-per-table), Disabled for
    /// WL6378
    MLOG_TRUNCATE = 60,

    /// notify that an index tree is being loaded without writing redo log about
    /// individual pages
    MLOG_INDEX_LOAD = 61,

    /// log for some persistent dynamic metadata change
    MLOG_TABLE_DYNAMIC_META = 62,

    /// create a SDI index page
    MLOG_PAGE_CREATE_SDI = 63,

    /// create a SDI compact page
    MLOG_COMP_PAGE_CREATE_SDI = 64,

    /// Extend the space
    MLOG_FILE_EXTEND = 65,

    /// Used in tests of redo log. It must never be used outside unit tests.
    MLOG_TEST = 66,

    MLOG_REC_INSERT = 67,
    MLOG_REC_CLUST_DELETE_MARK = 68,
    MLOG_REC_DELETE = 69,
    MLOG_REC_UPDATE_IN_PLACE = 70,
    MLOG_LIST_END_COPY_CREATED = 71,
    MLOG_PAGE_REORGANIZE = 72,
    MLOG_ZIP_PAGE_REORGANIZE = 73,
    MLOG_ZIP_PAGE_COMPRESS_NO_DATA = 74,
    MLOG_LIST_END_DELETE = 75,
    MLOG_LIST_START_DELETE = 76,

    /// biggest value (used in assertions)
    // MLOG_BIGGEST_TYPE = MLOG_LIST_START_DELETE

    #[default]
    UNDEF,
}

/// log record header, see mtr0log.ic, mlog_write_initial_log_record_low(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct LogRecordHeader {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (1 bit) single record flag, highest bit on log_rec_type, see
    /// MLOG_SINGLE_REC_FLAG
    #[derivative(Debug(format_with = "util::fmt_bool"))]
    pub single_rec_flag: bool,

    /// (1 byte) log record type
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub log_rec_type: LogRecordTypes,

    /// space ID
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub space_id: SpaceId,

    /// Page number
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub page_no: PageNumber,

    /// total bytes
    pub total_bytes: usize,
}

impl LogRecordHeader {
    /// if the mtr contains only one log record for one page, i.e.,
    /// write_initial_log_record has been called only once, this flag is ORed to
    /// the type of that first log record
    const MLOG_SINGLE_REC_FLAG: u8 = 0x80;

    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let mut ptr = addr;

        let flag_type = util::u8_val(&buf, ptr);
        ptr += 1;

        let space_id = util::mach_u32_read_compressed(ptr, buf.clone());
        ptr += space_id.0;

        let page_no = util::mach_u32_read_compressed(ptr, buf.clone());
        ptr += page_no.0;

        Self {
            log_rec_type: (flag_type & !Self::MLOG_SINGLE_REC_FLAG).into(),
            single_rec_flag: (flag_type & Self::MLOG_SINGLE_REC_FLAG) > 0,
            space_id: space_id.1.into(),
            page_no: page_no.1.into(),
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}
