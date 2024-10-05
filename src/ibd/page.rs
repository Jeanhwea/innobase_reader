use core::fmt;
use std::{
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

use anyhow::{Error, Result};
use bytes::Bytes;
use colored::Colorize;
use derivative::Derivative;
use log::{debug, info};
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use super::{sdi::SdiRecord, undo::UndoLog};
use crate::{
    ibd::{
        record::{Record, RecordHeader, RowData, RowInfo},
        sdi::SdiDataHeader,
    },
    meta::def::TableDef,
    sdi::record::EntryTypes,
    util,
};

// page
pub const PAGE_SIZE: usize = 16 * 1024;

// file
pub const FIL_HEADER_SIZE: usize = 38;
pub const FIL_TRAILER_SIZE: usize = 8;

// file space
pub const FSP_HEADER_SIZE: usize = 112;
pub const XDES_ENTRY_SIZE: usize = 40;
pub const XDES_ENTRY_MAX_COUNT: usize = 256;
pub const XDES_PAGE_COUNT: usize = 64;
pub const EXTENT_PAGE_NUM: usize = XDES_PAGE_COUNT * XDES_ENTRY_MAX_COUNT;

// inode
pub const INODE_FLST_NODE_SIZE: usize = 12;
pub const INODE_ENTRY_SIZE: usize = 192;
pub const INODE_ENTRY_MAX_COUNT: usize = 85;
pub const INODE_ENTRY_ARR_COUNT: usize = 32;
pub const FSEG_MAGIC_NUMBER: u32 = 97937874;
pub const FSEG_FRAG_ARR_OFFSET: usize = 64;
pub const FRAG_ARR_ENTRY_SIZE: usize = 4;
pub const PAGE_DIR_ENTRY_SIZE: usize = 2;

// index & record
pub const INDEX_HEADER_SIZE: usize = 36;
pub const INDEX_FSEG_HEADER_SIZE: usize = 20;
pub const INF_PAGE_BYTE_OFF: usize = 99;
pub const SUP_PAGE_BYTE_OFF: usize = 112;
pub const RECORD_HEADER_SIZE: usize = 5;

// TRX_SYS transaction system page
pub const TRX_SYS_N_RSEGS: usize = 128;
pub const TRX_SYS_MYSQL_LOG_INFO: usize = PAGE_SIZE - 2000;
pub const TRX_SYS_BINLOG_LOG_INFO: usize = PAGE_SIZE - 1000;
pub const TRX_SYS_DBLWR_LOG_INFO: usize = PAGE_SIZE - 200;
// magic number
pub const TRX_SYS_DOUBLEWRITE_MAGIC_N: u32 = 536853855;
pub const TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N: u32 = 1783657386;

// Rollback segment header
pub const TRX_RSEG_SLOT_SIZE: usize = 4;
pub const TRX_RSEG_N_SLOTS: usize = PAGE_SIZE / 16;

// space constants, see dict0dict.h, s_log_space_id, s_dict_space_id, ...
pub const INVALID_SPACE_ID: u32 = 0xFFFFFFFF;
pub const REDO_LOG_SPACE_ID: u32 = 0xFFFFFFF0;
pub const UNDO_SPACE_ID_MAX: u32 = REDO_LOG_SPACE_ID - 1;
pub const UNDO_SPACE_ID_MIN: u32 = REDO_LOG_SPACE_ID - (127 * 400000);
pub const DICT_SPACE_ID: u32 = 0xFFFFFFFE;
pub const INNODB_TEMP_SPACE_ID: u32 = 0xFFFFFFFD;
pub const TEMP_SPACE_ID_MAX: u32 = UNDO_SPACE_ID_MIN - 1;
pub const TEMP_SPACE_ID_MIN: u32 = UNDO_SPACE_ID_MIN - 400000;
pub const TRX_SYS_SPACE_ID: u32 = 0;

/// Tablespace ID
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum SpaceId {
    /// TRX_SYS_SPACE, Space id of the transaction system page (the system tablespace),
    SystemSpace,

    /// s_log_space_id, The first ID of the redo log pseudo-tablespace
    RedoSpace,

    /// s_invalid_space_id, Use maximum UINT value to indicate invalid space ID.
    Invalid,

    /// s_dict_space_id, The data dictionary tablespace ID.
    DictSpace,

    /// s_temp_space_id, The innodb_temporary tablespace ID.
    InnoTempSpace,

    /// s_min_undo_space_id, s_max_undo_space_id. undo tablespace ID
    UndoSpace(u32),

    /// s_min_temp_space_id, s_max_temp_space_id, for temporary tablespaces.
    TempSpace(u32),

    /// s_dd_dict_space_id, The dd::Tablespace::id of the dictionary tablespace.
    DdDictSpace,

    /// s_dd_sys_space_id, The dd::Tablespace::id of innodb_system.
    DdSysSpace,

    /// s_dd_temp_space_id, The dd::Tablespace::id of innodb_temporary.
    DdTempSpace,

    /// normal space ID
    Space(u32),
}

impl From<u32> for SpaceId {
    fn from(value: u32) -> SpaceId {
        match value {
            TRX_SYS_SPACE_ID => Self::SystemSpace,
            INVALID_SPACE_ID => Self::Invalid,
            1 => Self::DdDictSpace,
            2 => Self::DdSysSpace,
            3 => Self::DdTempSpace,
            REDO_LOG_SPACE_ID => Self::RedoSpace,
            val @ (UNDO_SPACE_ID_MIN..=UNDO_SPACE_ID_MAX) => {
                Self::UndoSpace(UNDO_SPACE_ID_MAX - val + 1)
            }
            DICT_SPACE_ID => Self::DictSpace,
            INNODB_TEMP_SPACE_ID => Self::InnoTempSpace,
            val @ (TEMP_SPACE_ID_MIN..=TEMP_SPACE_ID_MAX) => {
                Self::TempSpace(TEMP_SPACE_ID_MAX - val + 1)
            }
            val => Self::Space(val),
        }
    }
}

impl Display for SpaceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SpaceId::Space(val) => {
                write!(f, "{:?}", val)
            }
            _ => {
                write!(f, "{:?}", self)
            }
        }
    }
}

pub const PAGE_NONE: u32 = 0xffffffff;

/// Page Number
#[derive(Clone, Copy, PartialEq, Eq, Derivative)]
#[derivative(Debug)]
pub enum PageNumber {
    None,
    Page(u32),
}

impl From<u32> for PageNumber {
    fn from(value: u32) -> PageNumber {
        match value {
            PAGE_NONE => PageNumber::None,
            val => PageNumber::Page(val),
        }
    }
}

impl From<PageNumber> for usize {
    fn from(value: PageNumber) -> usize {
        match value {
            PageNumber::None => PAGE_NONE as usize,
            PageNumber::Page(val) => val as usize,
        }
    }
}

impl Display for PageNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PageNumber::Page(val) => {
                write!(f, "{:?}", val)
            }
            _ => {
                write!(f, "{:?}", self)
            }
        }
    }
}

/// Base Page Structure
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BasePage<B> {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (38 bytes) File page header
    pub fil_hdr: FilePageHeader,

    /// Page body, the data of this page
    pub page_body: B,

    /// (8 bytes) File page trailer
    pub fil_trl: FilePageTrailer,
}

/// Base Page Body
pub trait BasePageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self;
}

impl<B> BasePage<B>
where
    B: BasePageBody,
{
    pub fn new(addr: usize, buf: Arc<Bytes>) -> BasePage<B> {
        let header = FilePageHeader::new(0, buf.clone());
        let trailer = FilePageTrailer::new(buf.len() - FIL_TRAILER_SIZE, buf.clone());
        assert_eq!(
            header.check_sum, trailer.check_sum,
            "fil_hdr 和 trl_hdr 校验和不一致"
        );

        let body = BasePageBody::new(FIL_HEADER_SIZE, buf.clone());

        Self {
            fil_hdr: header,
            page_body: body,
            fil_trl: trailer,
            buf: buf.clone(),
            addr,
        }
    }
}

/// MySQL Page Type, see fil0fil.h
#[repr(u16)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum PageTypes {
    /// Freshly allocated page
    ALLOCATED = 0,

    /// This page type is unused.
    UNUSED = 1,

    /// Undo log page
    UNDO_LOG = 2,

    /// Index node
    INODE = 3,

    /// Insert buffer free list
    IBUF_FREE_LIST = 4,

    /// Insert buffer bitmap
    IBUF_BITMAP = 5,

    /// System page
    SYS = 6,

    /// Transaction system data
    TRX_SYS = 7,

    /// File space header
    FSP_HDR = 8,

    /// Extent descriptor page
    XDES = 9,

    /// Uncompressed BLOB page
    BLOB = 10,

    /// First compressed BLOB page
    ZBLOB = 11,

    /// Subsequent compressed BLOB page
    ZBLOB2 = 12,

    /// this value when flushing pages.
    UNKNOWN = 13,

    /// Compressed page
    COMPRESSED = 14,

    /// Encrypted page
    ENCRYPTED = 15,

    /// Compressed and Encrypted page
    COMPRESSED_AND_ENCRYPTED = 16,

    /// Encrypted R-tree page
    ENCRYPTED_RTREE = 17,

    /// Uncompressed SDI BLOB page
    SDI_BLOB = 18,

    /// Compressed SDI BLOB page
    SDI_ZBLOB = 19,

    /// Legacy doublewrite buffer page.
    LEGACY_DBLWR = 20,

    /// Rollback Segment Array page
    RSEG_ARRAY = 21,

    /// Index pages of uncompressed LOB
    LOB_INDEX = 22,

    /// Data pages of uncompressed LOB
    LOB_DATA = 23,

    /// The first page of an uncompressed LOB
    LOB_FIRST = 24,

    /// The first page of a compressed LOB
    ZLOB_FIRST = 25,

    /// Data pages of compressed LOB
    ZLOB_DATA = 26,

    /// Index pages of compressed LOB.
    ZLOB_INDEX = 27,

    /// Fragment pages of compressed LOB.
    ZLOB_FRAG = 28,

    /// Index pages of fragment pages (compressed LOB).
    ZLOB_FRAG_ENTRY = 29,

    /// Tablespace SDI Index page
    SDI = 17853,

    /// R-tree node
    RTREE = 17854,

    /// B-tree node
    INDEX = 17855,

    /// undefined page type
    #[default]
    UNDEF,
}

/// FIL Header, see fil0types.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FilePageHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) checksum, FIL_PAGE_SPACE_OR_CHKSUM
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub check_sum: u32,

    /// (4 bytes) page number/offset, FIL_PAGE_OFFSET
    #[derivative(Debug(format_with = "util::fmt_enum_3"))]
    pub page_no: PageNumber,

    /// (4 bytes) previous page, FIL_PAGE_PREV
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub prev_page: u32,
    /// (4 bytes) next page, FIL_PAGE_NEXT
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub next_page: u32,

    /// (8 bytes) LSN for last page modification, FIL_PAGE_LSN
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub lsn: u64,

    /// (2 bytes) page type, FIL_PAGE_TYPE
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub page_type: PageTypes,

    /// (8 bytes) flush LSN, FIL_PAGE_FILE_FLUSH_LSN
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub flush_lsn: u64,

    /// (4 bytes) space ID, FIL_PAGE_SPACE_ID
    #[derivative(Debug(format_with = "util::fmt_enum_2"))]
    pub space_id: SpaceId,
}

impl FilePageHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            check_sum: util::u32_val(&buf, addr),
            page_no: util::u32_val(&buf, addr + 4).into(),
            prev_page: util::u32_val(&buf, addr + 8),
            next_page: util::u32_val(&buf, addr + 12),
            lsn: util::u64_val(&buf, addr + 16),
            page_type: util::u16_val(&buf, addr + 24).into(),
            flush_lsn: util::u64_val(&buf, addr + 26),
            space_id: util::u32_val(&buf, addr + 34).into(),
            buf: buf.clone(),
            addr,
        }
    }

    pub fn server_version(&self) -> u32 {
        self.prev_page
    }

    pub fn space_version(&self) -> u32 {
        self.next_page
    }
}

/// FIL Trailer, see fil0types.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FilePageTrailer {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) Old-style Checksum, FIL_PAGE_END_LSN_OLD_CHKSUM
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub check_sum: u32,

    /// (4 bytes) Low 32-bits of LSN, last 4 bytes of FIL_PAGE_LSN
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub lsn_low32bit: u32,
}

impl FilePageTrailer {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            check_sum: util::u32_val(&buf, addr),
            lsn_low32bit: util::u32_val(&buf, addr + 4),
            buf: buf.clone(),
            addr,
        }
    }
}

/// File List Base Node
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FlstBaseNode {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) Length
    pub len: u32,

    /// (6 bytes) first node link
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub first: FilAddr,

    /// (6 bytes) last node link
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub last: FilAddr,
}

impl FlstBaseNode {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            len: util::u32_val(&buf, addr),
            first: FilAddr::new(addr + 4, buf.clone()),
            last: FilAddr::new(addr + 10, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// File List Node
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FlstNode {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (6 bytes) previous node link
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub prev: FilAddr,

    /// (6 bytes) next node link
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub next: FilAddr,
}

impl FlstNode {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            prev: FilAddr::new(addr, buf.clone()),
            next: FilAddr::new(addr + 6, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// File Address
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FilAddr {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) Page number within a space
    #[derivative(Debug(format_with = "util::fmt_enum_3"))]
    pub page_no: PageNumber,

    /// (2 bytes) Byte offset within the page
    pub boffset: u16,
}

impl FilAddr {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            page_no: util::u32_val(&buf, addr).into(),
            boffset: util::u16_val(&buf, addr + 4),
            buf: buf.clone(),
            addr,
        }
    }
}

/// FSP Header, see fsp0types.h, FSP_FLAGS_WIDTH_xxxx
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FileSpaceFlags {
    /// (1 bit) POST_ANTELOPE flag.
    pub post_antelope: bool,

    /// (4 bit) Number of flag bits used to indicate the tablespace zip page size
    pub zip_ssize: u32,

    /// (1 bit) ATOMIC_BLOBS flag.  The ability to break up a long column into
    /// an in-record prefix and an externally stored part is available to
    /// ROW_FORMAT=REDUNDANT and ROW_FORMAT=COMPACT.
    pub atomic_blobs: bool,

    /// (4 bit) Number of flag bits used to indicate the tablespace page size
    pub page_ssize: u32,

    /// (1 bit) DATA_DIR flag.  This flag indicates that the tablespace is found
    /// in a remote location, not the default data directory.
    pub data_dir: bool,

    /// (1 bit) SHARED flag.  This flag indicates that the tablespace was
    /// created with CREATE TABLESPACE and can be shared by multiple tables.
    pub shared: bool,

    /// (1 bit) TEMPORARY flag.  This flag indicates that the tablespace is a
    /// temporary tablespace and everything in it is temporary, meaning that it
    /// is for a single client and should be deleted upon startup if it exists.
    pub temporary: bool,

    /// (1 bit) ENCRYPTION flag.  This flag indicates that the tablespace is a
    /// tablespace with encryption.
    pub encryption: bool,

    /// (1 bit) SDI flag.  This flag indicates the presence of tablespace dictionary
    pub sdi: bool,

    /// (18 bit) the UNUSED bits
    pub unused: u32,
}

impl FileSpaceFlags {
    pub fn new(flags: u32) -> Self {
        Self {
            post_antelope: (flags & 0x1) > 0,
            zip_ssize: ((flags >> 1) & 0xf),
            atomic_blobs: ((flags >> 5) & 1) > 0,
            page_ssize: ((flags >> 6) & 0xf),
            data_dir: ((flags >> 10) & 1) > 0,
            shared: ((flags >> 11) & 1) > 0,
            temporary: ((flags >> 12) & 1) > 0,
            encryption: ((flags >> 13) & 1) > 0,
            sdi: ((flags >> 14) & 1) > 0,
            unused: (flags >> 15) & 0x3ffff,
        }
    }
}

/// FSP Header, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FileSpaceHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) tablespace ID
    #[derivative(Debug(format_with = "util::fmt_enum_2"))]
    pub space_id: SpaceId,

    /// (4 bytes) not used now
    pub notused: u32,

    /// (4 bytes) current size of the space in pages
    pub fsp_size: u32,

    /// (4 bytes) minimum page number for which the free list has not been initialized
    pub free_limit: u32,

    /// (4 bytes) fsp_space_t.flags, see fsp0types.h, FSP_FLAGS_WIDTH_xxxx
    #[derivative(Debug(format_with = "util::fmt_bin32"))]
    pub fsp_flags_bytes: u32,
    pub fsp_flags: FileSpaceFlags,

    /// (4 bytes) number of used pages in the FSP_FREE_FRAG list
    pub fsp_frag_n_used: u32,

    /// (16 bytes) list of free extents
    pub fsp_free: FlstBaseNode,

    /// (16 bytes) list of partially free extents not belonging to any segment
    pub free_frag: FlstBaseNode,

    /// (16 bytes) list of full extents not belonging to any segment
    pub full_frag: FlstBaseNode,

    /// (8 bytes) next segment id, 8 bytes which give the first unused segment id
    pub seg_id: u64,

    /// (16 bytes) list of pages containing segment headers, where all the
    /// segment inode slots are reserved
    pub inodes_full: FlstBaseNode,

    /// (16 bytes) list of pages containing segment headers, where not all the
    /// segment header slots are reserved
    pub inodes_free: FlstBaseNode,
}

impl FileSpaceHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let flags = util::u32_val(&buf, addr + 16);
        Self {
            space_id: util::u32_val(&buf, addr).into(),
            notused: util::u32_val(&buf, addr + 4),
            fsp_size: util::u32_val(&buf, addr + 8),
            free_limit: util::u32_val(&buf, addr + 12),
            fsp_flags_bytes: flags,
            fsp_flags: FileSpaceFlags::new(flags),
            fsp_frag_n_used: util::u32_val(&buf, addr + 20),
            fsp_free: FlstBaseNode::new(addr + 24, buf.clone()),
            free_frag: FlstBaseNode::new(addr + 40, buf.clone()),
            full_frag: FlstBaseNode::new(addr + 56, buf.clone()),
            seg_id: util::u64_val(&buf, addr + 72),
            inodes_full: FlstBaseNode::new(addr + 80, buf.clone()),
            inodes_free: FlstBaseNode::new(addr + 96, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// File Space Header Page
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FileSpaceHeaderPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (112 bytes) file space header
    pub fsp_hdr: FileSpaceHeader,

    /// (40*255 bytes) extent descriptor entry list
    #[derivative(Debug = "ignore")]
    pub xdes_ent_list: Vec<XDesEntry>,

    /// XDES entries that initialized
    pub xdes_ent_inited: Vec<XDesEntry>,
}

impl FileSpaceHeaderPageBody {
    // INFO_SIZE = 3 + 4 + 32*2 + 36 + 4 = 111
    // static constexpr size_t INFO_SIZE =
    //     (MAGIC_SIZE + sizeof(uint32) + (KEY_LEN * 2) + SERVER_UUID_LEN +
    //      sizeof(uint32));
    // INFO_MAX_SIZE = 111 + 4 = 115
    // static constexpr size_t INFO_MAX_SIZE = INFO_SIZE + sizeof(uint32);
    const INFO_MAX_SIZE: usize = 115;

    pub fn sdi_meta(&self) -> SdiMetaData {
        // sdi_addr, page offset = 10505
        let sdi_addr = self.addr
            + FSP_HEADER_SIZE
            + XDES_ENTRY_MAX_COUNT * XDES_ENTRY_SIZE
            + Self::INFO_MAX_SIZE;
        SdiMetaData::new(sdi_addr, self.buf.clone())
    }
}

impl BasePageBody for FileSpaceHeaderPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let hdr = FileSpaceHeader::new(addr, buf.clone());

        let entries = (0..XDES_ENTRY_MAX_COUNT)
            .map(|offset| {
                XDesEntry::new(
                    addr + FSP_HEADER_SIZE + offset * XDES_ENTRY_SIZE,
                    buf.clone(),
                    offset,
                )
            })
            .collect::<Vec<_>>();

        Self {
            fsp_hdr: hdr,
            xdes_ent_inited: entries
                .iter()
                .filter(|ent| ent.state != XDesStates::XDES_NOT_INITED)
                .cloned()
                .collect(),
            xdes_ent_list: entries,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Extent Descriptor Page,
/// much-like FileSpaceHeaderPageBody, except zero-fill the fsp_hdr
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct XDesPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// All XDES entries
    #[derivative(Debug = "ignore")]
    pub xdes_ent_list: Vec<XDesEntry>,

    /// XDES entries that initialized
    pub xdes_ent_inited: Vec<XDesEntry>,
}

impl BasePageBody for XDesPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let len = XDES_ENTRY_MAX_COUNT;
        let entries = (0..len)
            .map(|offset| {
                XDesEntry::new(
                    addr + FSP_HEADER_SIZE + offset * XDES_ENTRY_SIZE,
                    buf.clone(),
                    offset,
                )
            })
            .collect::<Vec<_>>();

        Self {
            xdes_ent_inited: entries
                .iter()
                .filter(|ent| ent.state != XDesStates::XDES_NOT_INITED)
                .cloned()
                .collect(),
            xdes_ent_list: entries,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Extent Descriptor State
#[repr(u32)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum XDesStates {
    XDES_NOT_INITED = 0, // extent descriptor is not initialized
    XDES_FREE = 1,       // extent is in free list of space
    XDES_FREE_FRAG = 2,  // extent is in free fragment list of space
    XDES_FULL_FRAG = 3,  // extent is in full fragment list of space
    XDES_FSEG = 4,       // extent belongs to a segment
    XDES_FSEG_FRAG = 5,  // fragment extent leased to segment
    #[default]
    UNDEF,
}

/// Free Bit
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct F(u8);

impl F {
    pub fn free(&self) -> bool {
        self.0 == 1
    }
}

/// Clean Bit
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct C(u8);

impl C {
    pub fn clean(&self) -> bool {
        self.0 == 1
    }
}

/// Extent Descriptor Entry, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct XDesEntry {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// XDes entry sequence
    pub xdes_seq: usize,

    /// (8 bytes) segment ID
    pub seg_id: u64,

    /// (12 bytes) list node data
    pub flst_node: FlstNode,

    /// (4 bytes) XDES state
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub state: XDesStates,

    /// (16 bytes) XDES bitmap. total 64 entries (1 extent = 64 pages), each
    /// entry has 2 bits:
    ///
    ///   1. first bit for free flag
    ///   2. second bit for clean flag
    ///
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub bitmap: [(u32, F, C); XDES_PAGE_COUNT],

    // #[derivative(Debug(format_with = "util::fmt_bytes_bin"))]
    #[derivative(Debug = "ignore")]
    pub bitmap_bytes: Bytes,
}

impl XDesEntry {
    pub fn new(addr: usize, buf: Arc<Bytes>, pos: usize) -> Self {
        let bits = (0..XDES_PAGE_COUNT)
            .map(|page_no| {
                let nth = page_no >> 2;
                let off = page_no & 0x3;
                let val = buf[addr + 24 + nth];
                // info!("page_no={}, nth={}, off={}", page_no, nth, off);
                (
                    // which page number it refers
                    page_no as u32,
                    // the free flag
                    if val & 1 << (2 * off) > 0 { F(1) } else { F(0) },
                    // the clean flag
                    if val & 1 << (2 * off + 1) > 0 {
                        C(1)
                    } else {
                        C(0)
                    },
                )
            })
            .collect::<Vec<_>>();

        Self {
            xdes_seq: pos,
            seg_id: util::u64_val(&buf, addr),
            flst_node: FlstNode::new(addr + 8, buf.clone()),
            state: util::u32_val(&buf, addr + 20).into(),
            bitmap: bits.try_into().unwrap(),
            bitmap_bytes: buf.slice(addr + 24..addr + 24 + XDES_PAGE_COUNT / 4),
            buf: buf.clone(),
            addr,
        }
    }
}

/// SDI Meta Data
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiMetaData {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    pub sdi_version: u32, // SDI Version
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub sdi_page_no: PageNumber, // SDI Page Number
}

impl SdiMetaData {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            sdi_version: util::u32_val(&buf, addr),
            sdi_page_no: util::u32_val(&buf, addr + 4).into(),
            buf: buf.clone(),
            addr,
        }
    }
}

/// File Segment Inode, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct INodePageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (12 bytes) The list node for linking segment inode pages,
    /// FSEG_INODE_PAGE_NODE
    pub inode_page_node: FlstNode,

    /// (192*85 bytes) INode entries
    pub inode_ent_list: Vec<INodeEntry>,
}

impl BasePageBody for INodePageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let entries = (0..INODE_ENTRY_MAX_COUNT)
            .map(|offset| {
                INodeEntry::new(
                    addr + INODE_FLST_NODE_SIZE + offset * INODE_ENTRY_SIZE,
                    buf.clone(),
                    offset,
                )
            })
            .filter(|entry| entry.fseg_magic_n == FSEG_MAGIC_NUMBER)
            .collect();

        Self {
            inode_page_node: FlstNode::new(addr, buf.clone()),
            inode_ent_list: entries,
            buf: buf.clone(),
            addr,
        }
    }
}

/// INode Entry, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct INodeEntry {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// INode sequence
    pub inode_seq: usize,

    /// (8 bytes) file segment ID
    pub fseg_id: u64,

    /// (4 bytes) number of file segment that not full used
    pub fseg_not_full_n_used: u32,

    /// (16 bytes) free file segment
    pub fseg_free: FlstBaseNode,

    /// (16 bytes) not full used file segment
    pub fseg_not_full: FlstBaseNode,

    /// (16 bytes) full used file segment
    pub fseg_full: FlstBaseNode,

    /// (4 bytes) file segment magic number, FSEG_MAGIC_N_VALUE = 97937874;
    pub fseg_magic_n: u32,

    /// (4*32 bytes) file segment fragment array
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub fseg_frag_arr: Vec<u32>, // frag page number
}

impl INodeEntry {
    pub fn new(addr: usize, buf: Arc<Bytes>, pos: usize) -> Self {
        let arr = (0..INODE_ENTRY_ARR_COUNT)
            .map(|offset| {
                util::u32_val(
                    &buf,
                    addr + FSEG_FRAG_ARR_OFFSET + offset * FRAG_ARR_ENTRY_SIZE,
                )
            })
            .filter(|page_no| *page_no != 0xffffffff)
            .collect();
        debug!("INodeEntry::arr={:?}", arr);

        Self {
            inode_seq: pos,
            fseg_id: util::u64_val(&buf, addr),
            fseg_not_full_n_used: util::u32_val(&buf, addr + 8),
            fseg_free: FlstBaseNode::new(addr + 12, buf.clone()),
            fseg_not_full: FlstBaseNode::new(addr + 28, buf.clone()),
            fseg_full: FlstBaseNode::new(addr + 44, buf.clone()),
            fseg_magic_n: util::u32_val(&buf, addr + 60),
            fseg_frag_arr: arr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Index Page
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct IndexPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (36 bytes) index header
    pub idx_hdr: IndexHeader,

    /// (10 bytes) leaf segment header, FSEG header
    pub fseg_hdr_0: FSegHeader,
    /// (10 bytes) non-leaf segment header, FSEG header
    pub fseg_hdr_1: FSegHeader,

    /// (13*2 bytes) system record
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub infimum: RecordHeader, // infimum_extra[], see page0page.h
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub supremum: RecordHeader, // supremum_extra_data[], see page0page.h

    /// record header
    #[derivative(Debug(format_with = "util::fmt_oneline_vec"))]
    pub data_rec_hdrs: Vec<RecordHeader>, // Data Record Header List
    #[derivative(Debug(format_with = "util::fmt_oneline_vec"))]
    pub free_rec_hdrs: Vec<RecordHeader>, // Free Record Header List

    /// (2*N bytes) page directory, grows "downwards" from @16376 (16384 - 8)
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub page_dirs: Vec<u16>, // page directory slots
}

impl BasePageBody for IndexPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let idx_hdr = IndexHeader::new(addr, buf.clone());
        debug!("idx_hdr={:?}", &idx_hdr);
        assert_eq!(
            idx_hdr.page_format,
            PageFormats::COMPACT,
            "只支持 COMPACT 行记录格式"
        );

        // Infimum
        let inf = RecordHeader::new(
            addr + INF_PAGE_BYTE_OFF - FIL_HEADER_SIZE - RECORD_HEADER_SIZE,
            buf.clone(),
        );

        // Parse Record Headers
        let mut free_addr = (INF_PAGE_BYTE_OFF as i16 + inf.next_rec_offset) as usize;
        let data_hdrs = (0..idx_hdr.page_n_recs)
            .map(|_nrec| {
                let rec_hdr = RecordHeader::new(free_addr - RECORD_HEADER_SIZE, buf.clone());
                free_addr = rec_hdr.next_addr();
                rec_hdr
            })
            .collect();

        let mut free_hdrs = vec![];
        if idx_hdr.page_garbage > 0 {
            let mut free_addr = idx_hdr.page_free as usize;
            loop {
                // if addr is invalid, just break
                if free_addr < SUP_PAGE_BYTE_OFF {
                    break;
                }

                // parse the garbage record
                let rec_hdr = RecordHeader::new(free_addr - RECORD_HEADER_SIZE, buf.clone());
                let next_addr = rec_hdr.next_addr();
                free_hdrs.push(rec_hdr);

                // update next record address
                if next_addr != free_addr {
                    free_addr = next_addr;
                } else {
                    break;
                }
            }
        }

        // Parse Page Directory Slots
        let slots = (0..idx_hdr.page_n_dir_slots as usize)
            .map(|offset| {
                util::u16_val(
                    &buf,
                    buf.len() - FIL_TRAILER_SIZE - (offset + 1) * PAGE_DIR_ENTRY_SIZE,
                )
            })
            .collect();
        debug!("slots={:?}", &slots);

        assert_eq!(
            buf.slice(INF_PAGE_BYTE_OFF..INF_PAGE_BYTE_OFF + 8).to_vec(),
            vec![b'i', b'n', b'f', b'i', b'm', b'u', b'm', 0],
            "infimum string checking"
        );
        assert_eq!(
            buf.slice(SUP_PAGE_BYTE_OFF..SUP_PAGE_BYTE_OFF + 8).to_vec(),
            vec![b's', b'u', b'p', b'r', b'e', b'm', b'u', b'm'],
            "supremum string checking"
        );

        Self {
            idx_hdr,
            fseg_hdr_0: FSegHeader::new(addr + 36, buf.clone()),
            fseg_hdr_1: FSegHeader::new(addr + 46, buf.clone()),
            infimum: inf,
            supremum: RecordHeader::new(
                addr + SUP_PAGE_BYTE_OFF - FIL_HEADER_SIZE - RECORD_HEADER_SIZE,
                buf.clone(),
            ),
            data_rec_hdrs: data_hdrs,
            free_rec_hdrs: free_hdrs,
            page_dirs: slots,
            buf: buf.clone(),
            addr,
        }
    }
}

impl IndexPageBody {
    pub fn read_user_records(
        &self,
        tabdef: Arc<TableDef>,
        index_pos: usize,
    ) -> Result<Vec<Record>, Error> {
        let inf = &self.infimum;
        let mut rec_addr = (INF_PAGE_BYTE_OFF as i16 + inf.next_rec_offset) as usize;

        let mut records = vec![];
        for nrec in 0..self.idx_hdr.page_n_recs {
            let rec = self.parse_record(rec_addr, tabdef.clone(), index_pos)?;
            info!("nrec={}, rec={:?}", nrec.to_string().green(), &rec);
            rec_addr = rec.rec_hdr.next_addr();
            records.push(rec);
        }

        assert_eq!(
            rec_addr, SUP_PAGE_BYTE_OFF,
            "记录地址 rec_addr 应该到达上确界"
        );
        Ok(records)
    }

    pub fn read_free_records(
        &self,
        tabdef: Arc<TableDef>,
        index_pos: usize,
    ) -> Result<Vec<Record>, Error> {
        let mut rec_addr = self.idx_hdr.page_free as usize;
        let mut free_records = vec![];
        loop {
            // if addr is invalid, just break
            if rec_addr < SUP_PAGE_BYTE_OFF {
                break;
            }

            // parse the garbage record
            let rec = self.parse_record(rec_addr, tabdef.clone(), index_pos)?;
            let next_addr = rec.rec_hdr.next_addr();
            free_records.push(rec);

            // update next record address
            if next_addr != rec_addr {
                rec_addr = next_addr;
            } else {
                break;
            }
        }
        Ok(free_records)
    }

    fn parse_record(
        &self,
        rec_addr: usize,
        tabdef: Arc<TableDef>,
        index_pos: usize,
    ) -> Result<Record, Error> {
        // Record Header
        let rec_hdr = RecordHeader::new(rec_addr - RECORD_HEADER_SIZE, self.buf.clone());

        // if rec_hdr.is_instant() {
        //     return Err(Error::msg(format!("不支持解析 INSTANT 标记的记录: {:?}", &rec_hdr)));
        // }

        // if rec_hdr.is_version() {
        //     return Err(Error::msg(format!("不支持解析 VERSION 标记的记录: {:?}", &rec_hdr)));
        // }

        // Row Info: depends on table definition
        let row_info = Arc::new(RowInfo::new(&rec_hdr, tabdef.clone(), index_pos));

        // Row Data: depends on table definition for unpack row
        let row_data = RowData::new(rec_addr, self.buf.clone(), row_info.clone());

        let rec = Record::new(
            rec_addr,
            self.buf.clone(),
            rec_hdr,
            row_info.clone(),
            row_data,
        );

        Ok(rec)
    }
}

/// Page Format, redundant or compact
#[repr(u8)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum PageFormats {
    REDUNDANT = 0,
    COMPACT = 1,
    #[default]
    UNDEF,
}

/// Page Direction
#[repr(u16)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum PageDirections {
    PAGE_LEFT = 1,
    PAGE_RIGHT = 2,
    PAGE_SAME_REC = 3,
    PAGE_SAME_PAGE = 4,
    PAGE_NO_DIRECTION = 5,
    #[default]
    UNDEF,
}

/// Index Page Header, see page0types.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct IndexHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (2 bytes) number of slots in page directory
    pub page_n_dir_slots: u16,

    /// (2 bytes) pointer to record heap top
    pub page_heap_top: u16,

    /// (1 bit) page format, steal 1 bit from page_n_heap
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub page_format: PageFormats,

    /// (15 bits) number of records in the heap, bit 15=flag: new-style compact
    /// page format
    pub page_n_heap: u16,

    /// (2 bytes) pointer to start of page free record list
    pub page_free: u16,

    /// (2 bytes) number of bytes in deleted records
    pub page_garbage: u16,

    /// (2 bytes) pointer to the last inserted record, or NULL if this info has
    /// been reset by a deletion
    pub page_last_insert: u16,

    /// (2 bytes) last insert direction: PAGE_LEFT, ...
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub page_direction: PageDirections,

    /// (2 bytes) number of consecutive inserts to the same direction
    pub page_n_direction: u16,

    /// (2 bytes) number of user records on the page
    pub page_n_recs: u16,

    /// (8 bytes) highest id of a trx which may have modified a record on the
    /// page; trx_id_t; defined only in secondary indexes and in the insert
    /// buffer tree
    pub page_max_trx_id: u64,

    /// (2 bytes) level of the node in an index tree; the leaf level is the
    /// level 0. This field should not be written to after page creation.
    pub page_level: u16,

    /// (8 bytes) index id where the page belongs. This field should not be
    /// written to after page creation.
    pub page_index_id: u64,
}

impl IndexHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let n_heap = util::u16_val(&buf, addr + 4);
        let fmt_flag = ((n_heap & 0x8000) >> 15) as u8;

        Self {
            page_n_dir_slots: util::u16_val(&buf, addr),
            page_heap_top: util::u16_val(&buf, addr + 2),
            page_format: fmt_flag.into(),
            page_n_heap: n_heap,
            page_free: util::u16_val(&buf, addr + 6),
            page_garbage: util::u16_val(&buf, addr + 8),
            page_last_insert: util::u16_val(&buf, addr + 10),
            page_direction: util::u16_val(&buf, addr + 12).into(),
            page_n_direction: util::u16_val(&buf, addr + 14),
            page_n_recs: util::u16_val(&buf, addr + 16),
            page_max_trx_id: util::u64_val(&buf, addr + 18),
            page_level: util::u16_val(&buf, addr + 26),
            page_index_id: util::u64_val(&buf, addr + 28),
            buf: buf.clone(),
            addr,
        }
    }
}

/// File Segment Header, see fsp0types.h/page0types.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FSegHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) space id
    #[derivative(Debug(format_with = "util::fmt_enum_2"))]
    pub space_id: SpaceId,

    /// (4 bytes) page number
    #[derivative(Debug(format_with = "util::fmt_enum_3"))]
    pub page_no: PageNumber,

    /// (2 bytes) byte offset
    pub offset: u16,
}

impl FSegHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            space_id: util::u32_val(&buf, addr).into(),
            page_no: util::u32_val(&buf, addr + 4).into(),
            offset: util::u16_val(&buf, addr + 8),
            buf: buf.clone(),
            addr,
        }
    }
}

/// SDI Index Page, see ibd2sdi.cc
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    pub index: IndexPageBody, // common Index Page
}

impl BasePageBody for SdiPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            index: IndexPageBody::new(addr, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

impl SdiPageBody {
    pub fn get_tabdef_str(&self) -> Result<String> {
        let sdi_objects = self.read_sdi_objects()?;
        let sdi_str = sdi_objects
            .iter()
            .find(|obj| obj.sdi_hdr.data_type == EntryTypes::Table)
            .map(|obj| obj.sdi_str.clone())
            .unwrap();
        Ok(sdi_str)
    }

    pub fn read_sdi_objects(&self) -> Result<Vec<SdiRecord>, Error> {
        let inf = &self.index.infimum;
        let mut rec_addr = (INF_PAGE_BYTE_OFF as i16 + inf.next_rec_offset) as usize;
        let records = (0..self.index.idx_hdr.page_n_recs)
            .map(|nrec| {
                debug!("nrec={}, rec_addr={}", &nrec, &rec_addr);
                let rec = self.parse_sdi_record(rec_addr);
                rec_addr = rec.rec_hdr.next_addr();
                rec
            })
            .collect();
        assert_eq!(rec_addr, SUP_PAGE_BYTE_OFF, "rec_addr 没有到达 supremum");
        Ok(records)
    }

    fn parse_sdi_record(&self, rec_addr: usize) -> SdiRecord {
        // Record Header
        let rec_hdr = RecordHeader::new(rec_addr - RECORD_HEADER_SIZE, self.buf.clone());

        // SDI Header
        let hdr = SdiDataHeader::new(rec_addr, self.buf.clone());
        debug!("sdi_hdr={:?}", &hdr);

        SdiRecord::new(rec_addr, self.buf.clone(), rec_hdr, hdr)
    }
}

/// Transaction System Page, see trx0sys.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct TrxSysPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (8 bytes) Transaction ID
    pub trx_id: u64,

    /// (10 bytes) segment header
    pub fseg_hdr: FSegHeader,

    /// (10 bytes) the array of rollback segment specification slots
    pub rseg_slots: Vec<RSegInfo>,

    /// (112 bytes) Master log info
    pub log_info_0: LogInfo,
    /// (112 bytes) binlog log info
    pub log_info_1: LogInfo,
    /// (112 bytes) double write log info
    pub dbw_info: DoubleWriteBufferInfo,
}

impl BasePageBody for TrxSysPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let slots = (0..TRX_SYS_N_RSEGS)
            .map(|nth| RSegInfo::new(addr + 8 + 10 + 8 * nth, buf.clone()))
            .filter(|rseg| !matches!(rseg.page_no, PageNumber::None))
            .collect();
        Self {
            trx_id: util::u64_val(&buf, addr),
            fseg_hdr: FSegHeader::new(addr + 8, buf.clone()),
            rseg_slots: slots,
            log_info_0: LogInfo::new(TRX_SYS_MYSQL_LOG_INFO, buf.clone()),
            log_info_1: LogInfo::new(TRX_SYS_BINLOG_LOG_INFO, buf.clone()),
            dbw_info: DoubleWriteBufferInfo::new(TRX_SYS_DBLWR_LOG_INFO, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// Rollback segment information, see trx0sys.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RSegInfo {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) space ID
    #[derivative(Debug(format_with = "util::fmt_enum_2"))]
    pub space_id: SpaceId,

    /// (4 bytes) page number
    #[derivative(Debug(format_with = "util::fmt_enum_3"))]
    pub page_no: PageNumber,
}

impl RSegInfo {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            space_id: util::u32_val(&buf, addr).into(),
            page_no: util::u32_val(&buf, addr + 4).into(),
            buf: buf.clone(),
            addr,
        }
    }
}

/// MySQL Log Info, see trx0sys.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct LogInfo {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) TRX_SYS_MYSQL_LOG_MAGIC_N_FLD
    pub magic_number: u32,

    /// (8 bytes) log offset
    /// TRX_SYS_MYSQL_LOG_OFFSET_HIGH/TRX_SYS_MYSQL_LOG_OFFSET_LOG
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub log_offset: u64,

    /// (100 bytes) MySQL log file name, TRX_SYS_MYSQL_LOG_NAME
    pub log_name: String,
}

impl LogInfo {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let name = buf.clone().slice(addr + 12..addr + 112);
        Self {
            magic_number: util::u32_val(&buf, addr),
            log_offset: util::u64_val(&buf, addr + 4),
            log_name: String::from_utf8(name.to_vec()).unwrap_or("".to_string()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// MySQL Double Write Buffer Info, see trx0sys.h, TRX_SYS_DOUBLEWRITE
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct DoubleWriteBufferInfo {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (10 bytes) fseg header
    pub fseg_hdr: FSegHeader,

    /// (4 bytes) TRX_SYS_MYSQL_LOG_MAGIC_N_FLD
    pub a_magic_number: u32,
    /// (4 bytes) Block 1 start page number
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub a_blk1_page_no: PageNumber,
    /// (4 bytes) Block 2 start page number
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub a_blk2_page_no: PageNumber,

    /// (4 bytes) TRX_SYS_MYSQL_LOG_MAGIC_N_FLD
    pub b_magic_number: u32,
    /// (4 bytes) Block 1 start page number
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub b_blk1_page_no: PageNumber,
    /// (4 bytes) Block 2 start page number
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub b_blk2_page_no: PageNumber,

    /// (4 bytes) magic number
    pub space_id_stored_magic_number: u32,
}

impl DoubleWriteBufferInfo {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let info = Self {
            fseg_hdr: FSegHeader::new(addr, buf.clone()),
            a_magic_number: util::u32_val(&buf, addr + 10),
            a_blk1_page_no: util::u32_val(&buf, addr + 14).into(),
            a_blk2_page_no: util::u32_val(&buf, addr + 18).into(),
            b_magic_number: util::u32_val(&buf, addr + 22),
            b_blk1_page_no: util::u32_val(&buf, addr + 26).into(),
            b_blk2_page_no: util::u32_val(&buf, addr + 30).into(),
            space_id_stored_magic_number: util::u32_val(&buf, addr + 34),
            buf: buf.clone(),
            addr,
        };

        assert_eq!(
            info.a_magic_number, TRX_SYS_DOUBLEWRITE_MAGIC_N,
            "TRX_SYS_DOUBLEWRITE_MAGIC_N 数值错误"
        );
        assert_eq!(
            info.b_magic_number, TRX_SYS_DOUBLEWRITE_MAGIC_N,
            "TRX_SYS_DOUBLEWRITE_MAGIC_N 数值错误"
        );
        assert_eq!(
            info.space_id_stored_magic_number, TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N,
            "TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N 数值错误"
        );

        info
    }
}

/// Rollback Segment Header Page, see trx0rseg.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RSegArrayPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) The RSEG ARRAY version offset in the header.
    pub rseg_version: u32,

    /// (4 bytes) The current number of rollback segments being tracked in this
    /// array
    pub rseg_arr_size: u32,

    /// (10 bytes) This is the pointer to the file segment inode that tracks
    /// this rseg array page.
    pub fseg_hdr: FSegHeader,

    /// (4*128 bytes) The start of the array of rollback segment header page
    /// numbers for this undo tablespace. The potential size of this array is
    /// limited only by the page size minus overhead. The actual size of the
    /// array is limited by srv_rollback_segments.
    #[derivative(Debug(format_with = "util::fmt_oneline_vec"))]
    pub arr_slots: Vec<(usize, PageNumber)>,
}

impl BasePageBody for RSegArrayPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let slots = (0..TRX_SYS_N_RSEGS)
            .map(|offset| {
                (
                    offset,
                    util::u32_val(&buf, addr + 18 + offset * TRX_RSEG_SLOT_SIZE).into(),
                )
            })
            .filter(|entry| !matches!(entry.1, PageNumber::None))
            .collect();

        Self {
            rseg_version: util::u32_val(&buf, addr) - Self::RSEG_ARRAY_BASE_VERSION,
            rseg_arr_size: util::u32_val(&buf, addr + 4),
            fseg_hdr: FSegHeader::new(addr + 8, buf.clone()),
            arr_slots: slots,
            buf: buf.clone(),
            addr,
        }
    }
}

impl RSegArrayPageBody {
    /// The RSEG ARRAY base version is a number derived from the string
    /// 'RSEG' [0x 52 53 45 47] for extra validation. Each new version
    /// increments the base version by 1.
    pub const RSEG_ARRAY_BASE_VERSION: u32 = 0x52534547;
}

/// Rollback Segment Header Page, see trx0rseg.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RSegHeaderPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (34 bytes) rollback segment header
    pub rseg_hdr: RollbackSegmentHeader,

    /// (4*1024 bytes) undo segment slots
    #[derivative(Debug(format_with = "util::fmt_oneline_vec"))]
    pub undo_slots: Vec<(usize, PageNumber)>,
}

impl BasePageBody for RSegHeaderPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let slots = (0..TRX_RSEG_N_SLOTS)
            .map(|offset| {
                (
                    offset,
                    util::u32_val(&buf, addr + 34 + offset * TRX_RSEG_SLOT_SIZE).into(),
                )
            })
            .filter(|entry| !matches!(entry.1, PageNumber::None))
            .collect();

        Self {
            rseg_hdr: RollbackSegmentHeader::new(addr, buf.clone()),
            undo_slots: slots,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Rollback Segment Header, see trx0rseg.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RollbackSegmentHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) TRX_RSEG_MAX_SIZE, Maximum allowed size for rollback segment
    /// in pages
    pub max_size: u32,

    /// (4 bytes) TRX_RSEG_HISTORY_SIZE, Number of file pages occupied by the
    /// logs in the history list
    pub history_size: u32,

    /// (16 bytes) TRX_RSEG_HISTORY, The update undo logs for committed
    /// transactions
    pub history_flst: FlstBaseNode,

    /// (10 bytes) TRX_RSEG_FSEG_HEADER, Header for the file segment where this
    /// page is placed
    pub fseg_hdr: FSegHeader,
}

impl RollbackSegmentHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            max_size: util::u32_val(&buf, addr),
            history_size: util::u32_val(&buf, addr + 4),
            history_flst: FlstBaseNode::new(addr + 8, buf.clone()),
            fseg_hdr: FSegHeader::new(addr + 24, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// UNDO Log Page, see trx0undo.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoLogPageBody {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (18 bytes) undo page header
    pub undo_page_hdr: UndoPageHeader,

    /// (30 bytes) undo segment header
    pub undo_seg_hdr: UndoSegmentHeader,

    /// Undo Log
    pub undo_log: Option<UndoLog>,
}

impl BasePageBody for UndoLogPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let page_hdr = UndoPageHeader::new(addr, buf.clone());
        let seg_hdr = UndoSegmentHeader::new(addr + 18, buf.clone());

        Self {
            undo_log: if seg_hdr.undo_last_log > 0 {
                Some(UndoLog::new(
                    seg_hdr.undo_last_log as usize,
                    buf.clone(),
                    &page_hdr,
                ))
            } else {
                None
            },
            undo_page_hdr: page_hdr,
            undo_seg_hdr: seg_hdr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Types of an undo log segment
#[repr(u16)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum UndoPageTypes {
    /// contains undo entries for inserts
    TRX_UNDO_INSERT = 1,

    /// contains undo entries for updates and delete markings: in short, modifys
    /// (the name 'UPDATE' is a historical relic)
    TRX_UNDO_UPDATE = 2,

    #[default]
    UNDEF,
}

/// Undo Page Header, see trx0undo.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoPageHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (2 bytes) TRX_UNDO_PAGE_TYPE, TRX_UNDO_INSERT or TRX_UNDO_UPDATE
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub page_type: UndoPageTypes,

    /// (2 bytes) TRX_UNDO_PAGE_START, Byte offset where the undo log records
    /// for the LATEST transaction start on this page (remember that in an
    /// update undo log, the first page can contain several undo logs)
    pub page_start: u16,

    /// (2 bytes) TRX_UNDO_PAGE_FREE, On each page of the undo log this field
    /// contains the byte offset of the first free byte on the page
    pub page_free: u16,

    /// (12 bytes) TRX_UNDO_PAGE_NODE, The file list node in the chain of undo
    /// log pages
    pub page_node: FlstNode,
}

impl UndoPageHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            page_type: util::u16_val(&buf, addr).into(),
            page_start: util::u16_val(&buf, addr + 2),
            page_free: util::u16_val(&buf, addr + 4),
            page_node: FlstNode::new(addr + 6, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

/// States of an undo log segment
#[repr(u16)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum UndoPageStates {
    /// contains an undo log of an active transaction
    TRX_UNDO_ACTIVE = 1,

    /// cached for quick reuse
    TRX_UNDO_CACHED = 2,

    /// insert undo segment can be freed
    TRX_UNDO_TO_FREE = 3,

    /// update undo segment will not be reused: it can be freed in purge when
    /// all undo data in it is removed
    TRX_UNDO_TO_PURGE = 4,

    /// contains an undo log of an prepared transaction for a server version
    /// older than 8.0.29
    TRX_UNDO_PREPARED_80028 = 5,

    /// contains an undo log of an prepared transaction
    TRX_UNDO_PREPARED = 6,

    /// contains an undo log of a prepared transaction that has been processed
    /// by the transaction coordinator
    TRX_UNDO_PREPARED_IN_TC = 7,

    #[default]
    UNDEF,
}

/// Undo Segment Header, see trx0undo.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoSegmentHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (2 bytes) undo state
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub undo_state: UndoPageStates,

    /// (2 bytes) Offset of the last undo log header on the segment header page,
    /// 0 if none
    pub undo_last_log: u16,

    /// (10 bytes) Header for the file segment which the undo log segment
    /// occupies
    pub undo_fseg_hdr: FSegHeader,

    /// (16 bytes) Base node for the list of pages in the undo log segment;
    /// defined only on the undo log segment's first page
    pub undo_page_list: FlstBaseNode,
}

impl UndoSegmentHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            undo_state: util::u16_val(&buf, addr).into(),
            undo_last_log: util::u16_val(&buf, addr + 2),
            undo_fseg_hdr: FSegHeader::new(addr + 4, buf.clone()),
            undo_page_list: FlstBaseNode::new(addr + 14, buf.clone()),
            buf: buf.clone(),
            addr,
        }
    }
}

#[cfg(test)]
mod page_tests {

    use super::*;
    use crate::util;

    #[test]
    fn info_datafile() {
        util::init_unit_test();
        let page = PageNumber::Page(10);
        info!("page={:?}", page);
    }
}
