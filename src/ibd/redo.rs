use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;
use log::{debug, info, warn};
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use super::page::{PageNumber, SpaceId};
use crate::{
    ibd::{record::DATA_ROLL_PTR_LEN, undo::RollPtr},
    util,
};

// log file size
pub const OS_FILE_LOG_BLOCK_SIZE: usize = 512;

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
    #[derivative(Debug(format_with = "util::fmt_str"))]
    pub creator: String,

    /// (4 bytes) 32 BITs flag, log header flags
    pub log_hdr_flags: u32,
}

impl LogFileHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            log_group_id: util::u32_val(&buf, addr),
            log_uuid: util::u32_val(&buf, addr + 4),
            start_lsn: util::u64_val(&buf, addr + 8),
            creator: util::str_val(&buf, addr + 16, 32),
            log_hdr_flags: util::u32_val(&buf, addr + 16 + 32),
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

    /// redo log record
    pub log_record: Option<LogRecord>,

    /// (4 bytes) last checksum
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub checksum: u32,
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
            log_record: rec,
            checksum: util::u32_val(&buf, addr + OS_FILE_LOG_BLOCK_SIZE - 4),
            buf: buf.clone(),
            addr,
        }
    }
}

/// log record, see recv_parse_log_rec(...)
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

    /// log record payload
    pub redo_rec_data: RedoRecordPayloads,
}

impl LogRecord {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        debug!(
            "LogRecord: addr={}, peek={:?}",
            addr,
            buf.slice(addr..addr + 16).to_vec()
        );

        let hdr = LogRecordHeader::new(addr, buf.clone());
        info!("{:>4} => {:?}", hdr.addr / OS_FILE_LOG_BLOCK_SIZE, &hdr);
        let payload = match hdr.log_rec_type {
            LogRecordTypes::MLOG_1BYTE
            | LogRecordTypes::MLOG_2BYTES
            | LogRecordTypes::MLOG_4BYTES
            | LogRecordTypes::MLOG_8BYTES => RedoRecordPayloads::NByte(RedoRecForNByte::new(
                addr + hdr.total_bytes,
                buf.clone(),
                &hdr,
            )),
            LogRecordTypes::MLOG_FILE_DELETE => RedoRecordPayloads::DeleteFile(
                RedoRecForFileDelete::new(addr + hdr.total_bytes, buf.clone(), &hdr),
            ),
            LogRecordTypes::MLOG_REC_INSERT => RedoRecordPayloads::RecInsert(
                RedoRecForRecordInsert::new(addr + hdr.total_bytes, buf.clone(), &hdr),
            ),
            LogRecordTypes::MLOG_REC_DELETE => RedoRecordPayloads::RecDelete(
                RedoRecForRecordDelete::new(addr + hdr.total_bytes, buf.clone(), &hdr),
            ),
            LogRecordTypes::MLOG_REC_UPDATE_IN_PLACE => RedoRecordPayloads::RecUpdateInPlace(
                RedoRecForRecordUpdateInPlace::new(addr + hdr.total_bytes, buf.clone(), &hdr),
            ),
            LogRecordTypes::MLOG_REC_CLUST_DELETE_MARK => RedoRecordPayloads::RecClusterDeleteMark(
                RedoRecForRecordClusterDeleteMark::new(addr + hdr.total_bytes, buf.clone(), &hdr),
            ),
            LogRecordTypes::MLOG_DUMMY_RECORD | LogRecordTypes::MLOG_MULTI_REC_END => {
                RedoRecordPayloads::NoBody
            }
            _ => RedoRecordPayloads::Unknown,
        };

        Self {
            log_rec_hdr: hdr,
            redo_rec_data: payload,
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
    #[derivative(Debug(format_with = "util::fmt_enum_2"))]
    pub space_id: SpaceId,

    /// Page number
    #[derivative(Debug(format_with = "util::fmt_enum_3"))]
    pub page_no: PageNumber,

    /// total bytes
    #[derivative(Debug = "ignore")]
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
        let log_rec_type: LogRecordTypes = (flag_type & !Self::MLOG_SINGLE_REC_FLAG).into();

        let mut space_id = 0;
        let mut page_no = 0;
        if !matches!(
            log_rec_type,
            LogRecordTypes::MLOG_DUMMY_RECORD | LogRecordTypes::MLOG_MULTI_REC_END
        ) {
            let space = util::u32_compressed(ptr, buf.clone());
            ptr += space.0;
            space_id = space.1;

            let page = util::u32_compressed(ptr, buf.clone());
            ptr += page.0;
            page_no = page.1;
        }

        Self {
            log_rec_type,
            single_rec_flag: (flag_type & Self::MLOG_SINGLE_REC_FLAG) > 0,
            space_id: space_id.into(),
            page_no: page_no.into(),
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// redo record payload, see recv_parse_or_apply_log_rec_body(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum RedoRecordPayloads {
    NByte(RedoRecForNByte),
    DeleteFile(RedoRecForFileDelete),
    RecInsert(RedoRecForRecordInsert),
    RecDelete(RedoRecForRecordDelete),
    RecUpdateInPlace(RedoRecForRecordUpdateInPlace),
    RecClusterDeleteMark(RedoRecForRecordClusterDeleteMark),
    NoBody,
    Unknown,
}

/// log record payload for nByte, see mlog_parse_nbytes(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoRecForNByte {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (2 bytes) page offset
    pub page_offset: u16,

    /// (1..4 bytes) value
    pub value: u64,
}

impl RedoRecForNByte {
    pub fn new(addr: usize, buf: Arc<Bytes>, hdr: &LogRecordHeader) -> Self {
        let offset = util::u16_val(&buf, addr);
        let value = match hdr.log_rec_type {
            LogRecordTypes::MLOG_1BYTE => util::u8_val(&buf, addr + 2).into(),
            LogRecordTypes::MLOG_2BYTES => util::u16_val(&buf, addr + 2).into(),
            LogRecordTypes::MLOG_4BYTES => util::u32_val(&buf, addr + 2).into(),
            LogRecordTypes::MLOG_8BYTES => util::u64_val(&buf, addr + 2),
            _ => panic!("未知的 MLOG_nBYTES 类型"),
        };
        Self {
            page_offset: offset,
            value,
            buf: buf.clone(),
            addr,
        }
    }
}

/// log record payload for file delete, see fil_tablespace_redo_delete(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoRecForFileDelete {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (2 bytes) file name length
    pub length: u16,

    /// (??? bytes) file name
    #[derivative(Debug(format_with = "util::fmt_str"))]
    pub file_name: String,
}

impl RedoRecForFileDelete {
    pub fn new(addr: usize, buf: Arc<Bytes>, _hdr: &LogRecordHeader) -> Self {
        let len = util::u16_val(&buf, addr);
        assert!(len >= 5);
        Self {
            length: len,
            file_name: util::str_val(&buf, addr + 2, len as usize),
            buf: buf.clone(),
            addr,
        }
    }
}

/// index flags
#[repr(u8)]
#[derive(Debug, Display, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString)]
pub enum IndexInfoFlags {
    COMPACT,
    VERSION,
    INSTANT,
}

/// index field nullable
#[repr(u8)]
#[derive(Debug, Display, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString)]
pub enum IndexFieldNullable {
    Null,
    NotNull,
}

/// index field fixed
#[repr(u8)]
#[derive(Debug, Display, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString)]
pub enum IndexFieldFixed {
    Fixed,
    NotFixed,
}

/// redo log index info, see mlog_parse_index(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoLogIndexInfo {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (1 byte) index log version
    pub index_log_version: u8,

    /// (1 byte) index flags
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub index_flags: Vec<IndexInfoFlags>,

    /// see index_flags
    pub index_flags_byte: u8,

    /// (2 bytes) number of index fields
    pub n: u16,
    /// (2 bytes) n_uniq for index
    pub n_uniq: u16,
    /// (2 bytes) number of column before first instant add was done.
    pub inst_cols: u16,

    /// (2*n bytes) index fields info
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub index_fields: Vec<(u16, IndexFieldNullable, IndexFieldFixed)>,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl RedoLogIndexInfo {
    const COMPACT_FLAG: u8 = 0x01;
    const VERSION_FLAG: u8 = 0x02;
    const INSTANT_FLAG: u8 = 0x04;

    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let mut ptr = addr;

        // parse index log version
        let version = util::u8_val(&buf, ptr);
        ptr += 1;

        // parse index flags
        let b = util::u8_val(&buf, ptr);
        ptr += 1;
        let is_compact = (b & Self::COMPACT_FLAG) > 0;
        let is_version = (b & Self::VERSION_FLAG) > 0;
        let is_instant = (b & Self::INSTANT_FLAG) > 0;
        let mut flags = vec![];
        if is_compact {
            flags.push(IndexInfoFlags::COMPACT);
        }
        if is_version {
            flags.push(IndexInfoFlags::VERSION);
        }
        if is_instant {
            flags.push(IndexInfoFlags::INSTANT);
        }

        // parse n, n_uniq, inst_cols
        let mut n = 1;
        let mut n_uniq = 1;
        let mut inst_cols = 0;
        if is_version || is_compact {
            n = util::u16_val(&buf, ptr);
            ptr += 2;
            if is_compact {
                if is_instant {
                    inst_cols = util::u16_val(&buf, ptr);
                    ptr += 2;
                }
                n_uniq = util::u16_val(&buf, ptr);
                ptr += 2;
            }
        }

        // it will meet bad date that n_uniq > n, just skip parse index_info
        if n_uniq > n {
            warn!(
                "flags={:?}, n={}, n_uniq={}, inst_cols={}",
                flags, n, n_uniq, inst_cols
            );
            n_uniq = 0;
            n = 0;
            ptr = addr;
        }

        assert!(n_uniq <= n);

        // parse index field
        let mut index_fields = vec![];
        for _ in 0..n {
            let data = util::u16_val(&buf, ptr);
            ptr += 2;

            // The high-order bit of data is the NOT NULL flag;
            // the rest is 0 or 0x7fff for variable-length fields,
            // and 1..0x7ffe for fixed-length fields.
            let fixed = if (((data as u32) + 1) & 0x7fff) > 1 {
                IndexFieldFixed::Fixed
            } else {
                IndexFieldFixed::NotFixed
            };
            let nullable = if (data & 0x8000) > 0 {
                IndexFieldNullable::Null
            } else {
                IndexFieldNullable::NotNull
            };
            let length = data & 0x7fff;
            index_fields.push((length, nullable, fixed));
        }

        Self {
            index_log_version: version,
            index_flags: flags,
            index_flags_byte: b,
            n,
            n_uniq,
            inst_cols,
            index_fields,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// log record payload for insert record, see page_cur_parse_insert_rec(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoRecForRecordInsert {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// redo log index info
    pub index_info: RedoLogIndexInfo,

    /// (2 bytes) offset
    pub rec_offset: u16,

    /// (compressed) length of mismatch_index, lowest bit is end_seg_flag
    pub end_seg_len: u32,

    /// end_seg_flag = (end_seg_len & 0x1)
    #[derivative(Debug(format_with = "util::fmt_bool"))]
    pub end_seg_flag: bool,

    /// data_len = (end_seg_len >> 1)
    pub data_len: usize,

    /// (1 byte)
    pub info_and_status_bits: u8,

    /// (compressed) length of record header
    pub origin_offset: u32,

    /// (compressed) the inserted index record end segment which differs from the
    /// cursor record
    pub mismatch_index: u32,

    /// (??? bytes) insert content data
    #[derivative(Debug(format_with = "util::fmt_bytes_vec"))]
    pub data: Bytes,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl RedoRecForRecordInsert {
    pub fn new(addr: usize, buf: Arc<Bytes>, _hdr: &LogRecordHeader) -> Self {
        let mut ptr = addr;
        let index = RedoLogIndexInfo::new(ptr, buf.clone());
        ptr += index.total_bytes;

        let rec_offset = util::u16_val(&buf, ptr);
        ptr += 2;

        let end_seg_len = util::u32_compressed(ptr, buf.clone());
        ptr += end_seg_len.0;

        let data_len = (end_seg_len.1 >> 1) as usize;
        let end_seg_flag = (end_seg_len.1 & 0x1) > 0;

        let mut info_bits = 0;
        let mut origin_offset = 0;
        let mut mismatch_index = 0;
        if end_seg_flag {
            info_bits = util::u8_val(&buf, ptr);
            ptr += 1;

            let tmp_origin_offset = util::u32_compressed(ptr, buf.clone());
            ptr += tmp_origin_offset.0;
            origin_offset = tmp_origin_offset.1;

            let tmp_mismatch_index = util::u32_compressed(ptr, buf.clone());
            ptr += tmp_mismatch_index.0;
            mismatch_index = tmp_mismatch_index.1;
        }

        let data = buf.slice(ptr..ptr + data_len);
        ptr += data_len;

        Self {
            index_info: index,
            rec_offset,
            end_seg_len: end_seg_len.1,
            end_seg_flag,
            data_len,
            origin_offset,
            mismatch_index,
            info_and_status_bits: info_bits,
            data,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// log record payload for delete record, see page_cur_parse_delete_rec(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoRecForRecordDelete {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// redo log index info
    pub index_info: RedoLogIndexInfo,

    /// (2 bytes) offset
    pub rec_offset: u16,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl RedoRecForRecordDelete {
    pub fn new(addr: usize, buf: Arc<Bytes>, _hdr: &LogRecordHeader) -> Self {
        info!(
            "RedoRecForRecordDelete::new() addr={}, peek={:?}",
            addr,
            buf.slice(addr..addr + 8).to_vec()
        );
        let mut ptr = addr;
        let index = RedoLogIndexInfo::new(ptr, buf.clone());
        ptr += index.total_bytes;

        debug!("index = {:?}", &index);

        let rec_offset = util::u16_val(&buf, ptr);
        ptr += 2;

        Self {
            index_info: index,
            rec_offset,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// redo record updated fields
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoRecUpdatedField {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// sequence number
    pub sequence: usize,

    /// (1-5 bytes) field number
    pub field_no: u32,

    /// (1-5 bytes) key length
    pub field_len: usize,

    /// (field_len bytes) data, see length for total size
    #[derivative(Debug(format_with = "util::fmt_bytes_vec"))]
    pub field_data: Bytes,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl RedoRecUpdatedField {
    pub fn new(addr: usize, buf: Arc<Bytes>, seq: usize) -> Self {
        let mut ptr = addr;

        let field_no = util::u32_compressed(ptr, buf.clone());
        ptr += field_no.0;

        let length = util::u32_compressed(ptr, buf.clone());
        ptr += length.0;

        let data = buf.slice(ptr..ptr + (length.1 as usize));
        ptr += length.1 as usize;

        Self {
            sequence: seq,
            field_no: field_no.1,
            field_len: length.1 as usize,
            field_data: data,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Display, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString)]
pub enum BtrModeFlags {
    /// do no undo logging
    NO_UNDO_LOG_FLAG,

    /// do no record lock checking
    NO_LOCKING_FLAG,

    /// sys fields will be found in the update vector or inserted entry
    KEEP_SYS_FLAG,

    /// btr_cur_pessimistic_update() must keep cursor position when moving
    /// columns to big_rec
    KEEP_POS_FLAG,

    /// the caller is creating the index or wants to bypass the
    /// index->info.online creation log
    CREATE_FLAG,

    /// the caller of btr_cur_optimistic_update() or btr_cur_update_in_place()
    /// will take care of updating IBUF_BITMAP_FREE
    KEEP_IBUF_BITMAP,
}

const BTR_NO_UNDO_LOG_FLAG: u8 = 1;
const BTR_NO_LOCKING_FLAG: u8 = 2;
const BTR_KEEP_SYS_FLAG: u8 = 4;
const BTR_KEEP_POS_FLAG: u8 = 8;
const BTR_CREATE_FLAG: u8 = 16;
const BTR_KEEP_IBUF_BITMAP: u8 = 32;

/// parse mode flags
pub fn parse_mode_flags(flags: u8) -> Vec<BtrModeFlags> {
    let mut parsed_flags = vec![];
    if (flags & BTR_NO_UNDO_LOG_FLAG) > 0 {
        parsed_flags.push(BtrModeFlags::NO_UNDO_LOG_FLAG);
    }
    if (flags & BTR_NO_LOCKING_FLAG) > 0 {
        parsed_flags.push(BtrModeFlags::NO_LOCKING_FLAG);
    }
    if (flags & BTR_KEEP_SYS_FLAG) > 0 {
        parsed_flags.push(BtrModeFlags::KEEP_SYS_FLAG);
    }
    if (flags & BTR_KEEP_POS_FLAG) > 0 {
        parsed_flags.push(BtrModeFlags::KEEP_POS_FLAG);
    }
    if (flags & BTR_CREATE_FLAG) > 0 {
        parsed_flags.push(BtrModeFlags::CREATE_FLAG);
    }
    if (flags & BTR_KEEP_IBUF_BITMAP) > 0 {
        parsed_flags.push(BtrModeFlags::KEEP_IBUF_BITMAP);
    }
    parsed_flags
}

/// log record payload for update record in-place, see
/// btr_cur_parse_update_in_place(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoRecForRecordUpdateInPlace {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// redo log index info
    pub index_info: RedoLogIndexInfo,

    /// (1 byte) flags, Mode flags for btr_cur operations; these can be ORed
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub mode_flags: Vec<BtrModeFlags>,

    #[derivative(Debug = "ignore")]
    pub mode_flags_byte: u8,

    /// (1-5 bytes) TRX_ID position in record
    pub trx_id_pos: u32,

    /// (7 bytes) rollback pointer
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub roll_ptr: RollPtr,

    /// (5-9 bytes) transaction ID
    pub trx_id: u64,

    /// (2 bytes) rec_offset
    pub rec_offset: u16,

    /// (1 byte) info bits
    pub info_bits: u8,

    /// (1-5 bytes) number of field
    pub n_fields: u32,

    /// updated fields
    pub upd_fields: Vec<RedoRecUpdatedField>,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl RedoRecForRecordUpdateInPlace {
    pub fn new(addr: usize, buf: Arc<Bytes>, _hdr: &LogRecordHeader) -> Self {
        let mut ptr = addr;
        let index = RedoLogIndexInfo::new(ptr, buf.clone());
        ptr += index.total_bytes;

        let b0 = util::u8_val(&buf, ptr);
        ptr += 1;

        let pos = util::u32_compressed(ptr, buf.clone());
        ptr += pos.0;

        let roll_ptr = util::u56_val(&buf, ptr);
        ptr += DATA_ROLL_PTR_LEN;

        let trx_id = util::u64_compressed(ptr, buf.clone());
        ptr += trx_id.0;

        let rec_offset = util::u16_val(&buf, ptr);
        ptr += 2;

        let info_bits = util::u8_val(&buf, ptr);
        ptr += 1;

        let n_fields = util::u32_compressed(ptr, buf.clone());
        ptr += n_fields.0;

        // updated fields
        let mut upd_fields = vec![];
        for i in 0..(n_fields.1 as usize) {
            let fld = RedoRecUpdatedField::new(ptr, buf.clone(), i);
            ptr += fld.total_bytes;
            upd_fields.push(fld);
        }

        Self {
            index_info: index,
            mode_flags: parse_mode_flags(b0),
            mode_flags_byte: b0,
            trx_id_pos: pos.1,
            roll_ptr: RollPtr::new(roll_ptr),
            trx_id: trx_id.1,
            rec_offset,
            info_bits,
            n_fields: n_fields.1,
            upd_fields,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// log record payload for delete marking or unmarking of a clustered index
/// record, see btr_cur_parse_del_mark_set_clust_rec(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RedoRecForRecordClusterDeleteMark {
    /// block address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// block data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// redo log index info
    pub index_info: RedoLogIndexInfo,

    /// (1 byte) flags, Mode flags for btr_cur operations; these can be ORed
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub mode_flags: Vec<BtrModeFlags>,

    #[derivative(Debug = "ignore")]
    pub mode_flags_byte: u8,

    /// (1 byte) value
    pub value: u8,

    /// (1-5 bytes) TRX_ID position in record
    pub trx_id_pos: u32,

    /// (7 bytes) rollback pointer
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub roll_ptr: RollPtr,

    /// (5-9 bytes) transaction ID
    pub trx_id: u64,

    /// (2 bytes) rec_offset
    pub rec_offset: u16,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl RedoRecForRecordClusterDeleteMark {
    pub fn new(addr: usize, buf: Arc<Bytes>, _hdr: &LogRecordHeader) -> Self {
        let mut ptr = addr;
        let index = RedoLogIndexInfo::new(ptr, buf.clone());
        ptr += index.total_bytes;

        let b0 = util::u8_val(&buf, ptr);
        ptr += 1;

        let val = util::u8_val(&buf, ptr);
        ptr += 1;

        let pos = util::u32_compressed(ptr, buf.clone());
        ptr += pos.0;

        let roll_ptr = util::u56_val(&buf, ptr);
        ptr += DATA_ROLL_PTR_LEN;

        let trx_id = util::u64_compressed(ptr, buf.clone());
        ptr += trx_id.0;

        let rec_offset = util::u16_val(&buf, ptr);
        ptr += 2;

        Self {
            index_info: index,
            mode_flags: parse_mode_flags(b0),
            mode_flags_byte: b0,
            value: val,
            trx_id_pos: pos.1,
            roll_ptr: RollPtr::new(roll_ptr),
            trx_id: trx_id.1,
            rec_offset,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}
