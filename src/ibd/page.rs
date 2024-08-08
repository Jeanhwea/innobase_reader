use std::{fmt::Debug, sync::Arc};

use anyhow::{Error, Result};
use bytes::Bytes;
use derivative::Derivative;
use log::debug;
use num_enum::FromPrimitive;
use strum::{Display, EnumString};

use super::record::{Record, SdiDataHeader, SdiObject, SdiRecord};
use crate::{
    ibd::{
        page::{
            CleanBit::{Clean, Dirty},
            FreeBit::{Free, Taken},
        },
        record::{RecordHeader, Row, RowInfo},
    },
    meta::def::{IndexDef, TableDef},
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
pub const INF_PAGE_BYTE_OFF: usize = 99;
pub const SUP_PAGE_BYTE_OFF: usize = 112;
pub const RECORD_HEADER_SIZE: usize = 5;

// sdi
pub const SDI_DATA_HEADER_SIZE: usize = 33;

/// Base Page Structure
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BasePage<B> {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    pub fil_hdr: FilePageHeader,
    pub page_body: B,
    pub fil_trl: FilePageTrailer,
}

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
            "file header and trailer checksum should same"
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
#[derive(Debug, Display, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageTypes {
    ALLOCATED = 0,                 // Freshly allocated page
    UNUSED = 1,                    // This page type is unused.
    UNDO_LOG = 2,                  // Undo log page
    INODE = 3,                     // Index node
    IBUF_FREE_LIST = 4,            // Insert buffer free list
    IBUF_BITMAP = 5,               // Insert buffer bitmap
    SYS = 6,                       // System page
    TRX_SYS = 7,                   // Transaction system data
    FSP_HDR = 8,                   // File space header
    XDES = 9,                      // Extent descriptor page
    BLOB = 10,                     // Uncompressed BLOB page
    ZBLOB = 11,                    // First compressed BLOB page
    ZBLOB2 = 12,                   // Subsequent compressed BLOB page
    UNKNOWN = 13,                  // this value when flushing pages.
    COMPRESSED = 14,               // Compressed page
    ENCRYPTED = 15,                // Encrypted page
    COMPRESSED_AND_ENCRYPTED = 16, // Compressed and Encrypted page
    ENCRYPTED_RTREE = 17,          // Encrypted R-tree page
    SDI_BLOB = 18,                 // Uncompressed SDI BLOB page
    SDI_ZBLOB = 19,                // Compressed SDI BLOB page
    LEGACY_DBLWR = 20,             // Legacy doublewrite buffer page.
    RSEG_ARRAY = 21,               // Rollback Segment Array page
    LOB_INDEX = 22,                // Index pages of uncompressed LOB
    LOB_DATA = 23,                 // Data pages of uncompressed LOB
    LOB_FIRST = 24,                // The first page of an uncompressed LOB
    ZLOB_FIRST = 25,               // The first page of a compressed LOB
    ZLOB_DATA = 26,                // Data pages of compressed LOB
    ZLOB_INDEX = 27,               // Index pages of compressed LOB.
    ZLOB_FRAG = 28,                // Fragment pages of compressed LOB.
    ZLOB_FRAG_ENTRY = 29,          // Index pages of fragment pages (compressed LOB).
    SDI = 17853,                   // Tablespace SDI Index page
    RTREE = 17854,                 // R-tree node
    INDEX = 17855,                 // B-tree node
    #[default]
    UNDEF,
}

/// FIL Header, see fil0types.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FilePageHeader {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub check_sum: u32, // check_sum, FIL_PAGE_SPACE_OR_CHKSUM
    pub page_no: u32, // page_number/offset, FIL_PAGE_OFFSET
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub prev_page: u32, // Previous Page, FIL_PAGE_PREV
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub next_page: u32, // Next Page, FIL_PAGE_NEXT
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub lsn: u64, // LSN for last page modification, FIL_PAGE_LSN
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub page_type: PageTypes, // Page Type, FIL_PAGE_TYPE
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub flush_lsn: u64, // Flush LSN, FIL_PAGE_FILE_FLUSH_LSN
    pub space_id: u32, // Space ID, FIL_PAGE_SPACE_ID
}

impl FilePageHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            check_sum: util::u32_val(&buf, addr),
            page_no: util::u32_val(&buf, addr + 4),
            prev_page: util::u32_val(&buf, addr + 8),
            next_page: util::u32_val(&buf, addr + 12),
            lsn: util::u64_val(&buf, addr + 16),
            page_type: util::u16_val(&buf, addr + 24).into(),
            flush_lsn: util::u64_val(&buf, addr + 26),
            space_id: util::u32_val(&buf, addr + 34),
            buf: buf.clone(),
            addr,
        }
    }

    pub fn server_version(&self) -> u32 {
        assert_eq!(self.page_no, 0);
        self.prev_page
    }

    pub fn space_version(&self) -> u32 {
        assert_eq!(self.page_no, 0);
        self.next_page
    }
}

/// FIL Trailer, see fil0types.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FilePageTrailer {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub check_sum: u32, // Old-style Checksum, FIL_PAGE_END_LSN_OLD_CHKSUM
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub lsn_low32bit: u32, // Low 32-bits of LSN, last 4 bytes of FIL_PAGE_LSN
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    pub len: u32,
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub first: FilAddr,
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub prev: FilAddr,
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug(format_with = "util::fmt_page_no"))]
    pub page: u32, // Page number within a space
    pub boffset: u16, // Byte offset within the page
}

impl FilAddr {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            page: util::u32_val(&buf, addr),
            boffset: util::u16_val(&buf, addr + 4),
            buf: buf.clone(),
            addr,
        }
    }
}

/// FSP Header, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FileSpaceHeader {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    /// Table space ID
    pub space_id: u32,
    /// not used now
    pub notused: u32,
    /// Current size of the space in pages
    pub fsp_size: u32,
    /// Minimum page number for which the free list has not been initialized
    pub free_limit: u32,
    /// fsp_space_t.flags, see fsp0types.h
    #[derivative(Debug(format_with = "util::fmt_bin32"))]
    pub fsp_flags: u32,
    /// number of used pages in the FSP_FREE_FRAG list
    pub fsp_frag_n_used: u32,
    /// list of free extents
    pub fsp_free: FlstBaseNode,
    /// list of partially free extents not belonging to any segment
    pub free_frag: FlstBaseNode,
    /// list of full extents not belonging to any segment
    pub full_frag: FlstBaseNode,
    /// next segment id, 8 bytes which give the first unused segment id
    pub seg_id: u64,
    /// list of pages containing segment headers, where all the segment inode
    /// slots are reserved
    pub inodes_full: FlstBaseNode,
    /// list of pages containing segment headers, where not all the segment
    /// header slots are reserved
    pub inodes_free: FlstBaseNode,
}

impl FileSpaceHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            space_id: util::u32_val(&buf, addr),
            notused: util::u32_val(&buf, addr + 4),
            fsp_size: util::u32_val(&buf, addr + 8),
            free_limit: util::u32_val(&buf, addr + 12),
            fsp_flags: util::u32_val(&buf, addr + 16),
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    pub fsp_hdr: FileSpaceHeader,

    #[derivative(Debug = "ignore")]
    pub xdes_ent_list: Vec<XDesEntry>,
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
        let sdi_addr = self.addr + FSP_HEADER_SIZE + XDES_ENTRY_MAX_COUNT * XDES_ENTRY_SIZE + Self::INFO_MAX_SIZE;
        SdiMetaData::new(sdi_addr, self.buf.clone())
    }
}

impl BasePageBody for FileSpaceHeaderPageBody {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let hdr = FileSpaceHeader::new(addr, buf.clone());
        // let len: usize =
        //     (hdr.fsp_free.len + hdr.free_frag.len + hdr.full_frag.len + hdr.inodes_free.len + hdr.inodes_full.len)
        //         as usize;
        let len = XDES_ENTRY_MAX_COUNT;
        let entries = (0..len)
            .map(|offset| XDesEntry::new(addr + FSP_HEADER_SIZE + offset * XDES_ENTRY_SIZE, buf.clone(), offset))
            .collect();

        Self {
            fsp_hdr: hdr,
            xdes_ent_list: entries,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Extent Descriptor State
#[repr(u32)]
#[derive(Debug, Display, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
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

/// Extent Descriptor Entry, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct XDesEntry {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
    pub ent_pos: usize,
    pub seg_id: u64,         // seg_id
    pub flst_node: FlstNode, // list node data
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub state: XDesStates, // state information
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub bitmap: [(u32, FreeBit, CleanBit); XDES_PAGE_COUNT], // bitmap
}

impl XDesEntry {
    pub fn new(addr: usize, buf: Arc<Bytes>, pos: usize) -> Self {
        let bits = (0..XDES_PAGE_COUNT)
            .map(|page_no| {
                let nth = page_no >> 2;
                let off = page_no & 0x3;
                let val = buf[addr + 24 + nth];
                (
                    page_no as u32,
                    if val & (1 << off) > 0 { Free } else { Taken },
                    if val & (1 << (off + 1)) > 0 { Clean } else { Dirty },
                )
            })
            .collect::<Vec<_>>();

        Self {
            ent_pos: pos,
            seg_id: util::u64_val(&buf, addr),
            flst_node: FlstNode::new(addr + 8, buf.clone()),
            state: util::u32_val(&buf, addr + 20).into(),
            bitmap: bits.try_into().unwrap(),
            buf: buf.clone(),
            addr,
        }
    }
}

/// Free Bit
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum FreeBit {
    Free,
    Taken,
}

/// Clean Bit
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum CleanBit {
    Clean,
    Dirty,
}

/// SDI Meta Data
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiMetaData {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    pub sdi_version: u32, // SDI Version
    pub sdi_page_no: u32, // SDI Page Number
}

impl SdiMetaData {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            sdi_version: util::u32_val(&buf, addr),
            sdi_page_no: util::u32_val(&buf, addr + 4),
            buf: buf.clone(),
            addr,
        }
    }
}

/// File Segment Inode, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct INodePageBody {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    pub inode_flst_node: FlstNode,
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
            inode_flst_node: FlstNode::new(addr, buf.clone()),
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    pub ent_pos: usize,
    pub fseg_id: u64,
    pub fseg_not_full_n_used: u32,
    pub fseg_free: FlstBaseNode,
    pub fseg_not_full: FlstBaseNode,
    pub fseg_full: FlstBaseNode,
    pub fseg_magic_n: u32, // FSEG_MAGIC_N_VALUE = 97937874;
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub fseg_frag_arr: Vec<u32>, // frag page number
}

impl INodeEntry {
    pub fn new(addr: usize, buf: Arc<Bytes>, pos: usize) -> Self {
        let arr = (0..INODE_ENTRY_ARR_COUNT)
            .map(|offset| util::u32_val(&buf, addr + FSEG_FRAG_ARR_OFFSET + offset * FRAG_ARR_ENTRY_SIZE))
            .filter(|page_no| *page_no != 0xffffffff)
            .collect();
        debug!("INodeEntry::arr={:?}", arr);

        Self {
            ent_pos: pos,
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    /// Index Header
    pub idx_hdr: IndexHeader, // Index Header
    /// FSEG Header
    pub fseg_hdr: FSegHeader, // FSEG Header

    /// System Record
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub infimum: RecordHeader, // infimum_extra[], see page0page.h
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub supremum: RecordHeader, // supremum_extra_data[], see page0page.h

    /// Record Headers
    #[derivative(Debug(format_with = "util::fmt_oneline_vec"))]
    pub data_rec_hdrs: Vec<RecordHeader>, // Data Record Header List
    #[derivative(Debug(format_with = "util::fmt_oneline_vec"))]
    pub free_rec_hdrs: Vec<RecordHeader>, // Free Record Header List

    /// Page Directory, grows "downwards" from @16376 (16384 - 8)
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
            "only support compact row format"
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

        let mut free_hdrs = Vec::new();
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
            .map(|offset| util::u16_val(&buf, buf.len() - FIL_TRAILER_SIZE - (offset + 1) * PAGE_DIR_ENTRY_SIZE))
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
            fseg_hdr: FSegHeader::new(addr + 36, buf.clone()),
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
    pub fn read_user_records(&self, tabdef: Arc<TableDef>, idxdef: &IndexDef) -> Result<Vec<Record>, Error> {
        let inf = &self.infimum;
        let mut rec_addr = (INF_PAGE_BYTE_OFF as i16 + inf.next_rec_offset) as usize;
        let records = (0..self.idx_hdr.page_n_recs)
            .map(|nrec| {
                debug!("nrec={}, rec_addr={}", &nrec, &rec_addr);
                let rec = self.parse_record(rec_addr, tabdef.clone(), idxdef);
                rec_addr = rec.rec_hdr.next_addr();
                rec
            })
            .collect();
        assert_eq!(rec_addr, SUP_PAGE_BYTE_OFF, "rec_addr should reach supremum");
        Ok(records)
    }

    pub fn read_free_records(&self, tabdef: Arc<TableDef>, idxdef: &IndexDef) -> Result<Vec<Record>, Error> {
        let mut rec_addr = self.idx_hdr.page_free as usize;
        let mut free_records = Vec::new();
        loop {
            // if addr is invalid, just break
            if rec_addr < SUP_PAGE_BYTE_OFF {
                break;
            }

            // parse the garbage record
            let rec = self.parse_record(rec_addr, tabdef.clone(), idxdef);
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

    fn parse_record(&self, rec_addr: usize, tabdef: Arc<TableDef>, idxdef: &IndexDef) -> Record {
        // Record Header
        let rec_hdr = RecordHeader::new(rec_addr - RECORD_HEADER_SIZE, self.buf.clone());

        // Row Info
        let row_info = RowInfo::new(rec_addr - RECORD_HEADER_SIZE, self.buf.clone(), tabdef.clone(), idxdef);

        // Row Data
        let row = Row::new(rec_addr, self.buf.clone(), tabdef.clone());

        Record::new(rec_addr, self.buf.clone(), rec_hdr, row_info, row)
    }
}

/// Page Format, redundant or compact
#[repr(u8)]
#[derive(Debug, Display, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageFormats {
    REDUNDANT = 0,
    COMPACT = 1,
    #[default]
    UNDEF,
}

/// Page Direction
#[repr(u16)]
#[derive(Debug, Display, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    /// number of slots in page directory
    pub page_n_dir_slots: u16,
    /// pointer to record heap top
    pub page_heap_top: u16,
    /// number of records in the heap, bit 15=flag: new-style compact page format
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub page_format: PageFormats,
    pub page_n_heap: u16,
    /// pointer to start of page free record list
    pub page_free: u16,
    /// number of bytes in deleted records
    pub page_garbage: u16,
    /// pointer to the last inserted record, or NULL if this info has been reset by a deletion
    pub page_last_insert: u16,
    /// last insert direction: PAGE_LEFT, ...
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub page_direction: PageDirections,
    /// number of consecutive inserts to the same direction
    pub page_n_direction: u16,
    /// number of user records on the page
    pub page_n_recs: u16,
    /// highest id of a trx which may have modified a record on the page; trx_id_t;
    /// defined only in secondary indexes and in the insert buffer tree
    pub page_max_trx_id: u64,
    /// level of the node in an index tree; the leaf level is the level 0. This
    /// field should not be written to after page creation.
    pub page_level: u16,
    /// index id where the page belongs. This field should not be written to after page creation.
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
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    /// leaf page
    pub leaf_space_id: u32, // space id
    pub leaf_page_no: u32, // page number
    pub leaf_offset: u16,  // byte offset
    /// non-leaf page
    pub nonleaf_space_id: u32, // space id
    pub nonleaf_page_no: u32, // page number
    pub nonleaf_offset: u16, // byte offset
}

impl FSegHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            leaf_space_id: util::u32_val(&buf, addr),
            leaf_page_no: util::u32_val(&buf, addr + 4),
            leaf_offset: util::u16_val(&buf, addr + 8),
            nonleaf_space_id: util::u32_val(&buf, addr + 10),
            nonleaf_page_no: util::u32_val(&buf, addr + 14),
            nonleaf_offset: util::u16_val(&buf, addr + 18),
            buf: buf.clone(),
            addr,
        }
    }
}

/// SDI Index Page, see ibd2sdi.cc
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiPageBody {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

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
    pub fn get_tabdef_sdiobj(&self) -> Result<SdiObject, Error> {
        let sdi_objects = self.read_sdi_objects()?;
        let sdi_str = sdi_objects
            .iter()
            .find(|obj| obj.sdi_hdr.data_type == 1) // 1 => Table
            .map(|obj| &obj.sdi_str)
            .unwrap();
        Ok(serde_json::from_str(sdi_str).expect("ERR_SDI_FORMAT"))
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
        assert_eq!(rec_addr, SUP_PAGE_BYTE_OFF, "rec_addr should reach supremum");
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
