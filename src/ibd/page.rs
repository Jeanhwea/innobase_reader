use super::record::{Record, SdiObject};
use crate::ibd::record::{RecordHeader, Row, RowInfo, PAGE_ADDR_INF, PAGE_ADDR_SUP};
use crate::meta::def::TableDef;
use crate::util;
use anyhow::{Error, Result};
use bytes::Bytes;
use colored::Colorize;
use derivative::Derivative;
use log::{debug, info};
use num_enum::FromPrimitive;
use std::fmt::{Formatter, Debug};
use std::sync::Arc;
use std::{cmp, fmt};
use strum::{Display, EnumString};

pub const PAGE_SIZE: usize = 16 * 1024;

pub const FIL_HEADER_SIZE: usize = 38;
pub const FIL_TRAILER_SIZE: usize = 8;
pub const FSP_HEADER_SIZE: usize = 112;
pub const XDES_ENTRY_SIZE: usize = 40;
pub const XDES_ENTRY_MAX_COUNT: usize = 256;
pub const INODE_FLST_NODE_SIZE: usize = 12;
pub const INODE_ENTRY_SIZE: usize = 192;
pub const INODE_ENTRY_MAX_COUNT: usize = 85;
pub const INODE_ENTRY_ARR_COUNT: usize = 32;
pub const FRAG_ARR_ENTRY_SIZE: usize = 4;
pub const PAGE_DIR_ENTRY_SIZE: usize = 2;

// Base Page Structure
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BasePage<P> {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    pub page_size: usize, // page size
    pub fil_hdr: FilePageHeader,
    pub page_body: P,
    pub fil_trl: FilePageTrailer,
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
}

pub trait BasePageOperation {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self;
}

impl<P> BasePage<P>
where
    P: BasePageOperation,
{
    pub fn new(addr: usize, buf: Arc<Bytes>) -> BasePage<P> {
        let page_size = buf.len();
        let header = FilePageHeader::new(0, buf.clone());
        let trailer = FilePageTrailer::new(page_size - FIL_TRAILER_SIZE, buf.clone());
        assert_eq!(header.check_sum, trailer.check_sum);

        let body = BasePageOperation::new(FIL_HEADER_SIZE, buf.clone());

        Self {
            fil_hdr: header,
            page_body: body,
            fil_trl: trailer,
            buf: buf.clone(),
            addr,
            page_size,
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
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub check_sum: u32, // check_sum, FIL_PAGE_SPACE_OR_CHKSUM
    pub page_no: u32, // page_number/offset, FIL_PAGE_OFFSET
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub prev_page: u32, // Previous Page, FIL_PAGE_PREV
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub next_page: u32, // Next Page, FIL_PAGE_NEXT
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub lsn: u64, // LSN for last page modification, FIL_PAGE_LSN
    pub page_type: PageTypes, // Page Type, FIL_PAGE_TYPE
    #[derivative(Debug(format_with = "util::fmt_hex64"))]
    pub flush_lsn: u64, // Flush LSN, FIL_PAGE_FILE_FLUSH_LSN
    pub space_id: u32, // Space ID, FIL_PAGE_SPACE_ID
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
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
            buf,
            addr,
        }
    }
}

/// FIL Trailer, see fil0types.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FilePageTrailer {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub check_sum: u32, // Old-style Checksum, FIL_PAGE_END_LSN_OLD_CHKSUM
    #[derivative(Debug(format_with = "util::fmt_hex32"))]
    pub lsn_low32bit: u32, // Low 32-bits of LSN, last 4 bytes of FIL_PAGE_LSN
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
}

impl FilePageTrailer {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            check_sum: util::u32_val(&buf, addr),
            lsn_low32bit: util::u32_val(&buf, addr + 4),
            buf,
            addr,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FlstBaseNode {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    pub len: u32,
    pub first: FilAddr,
    pub last: FilAddr,
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
}

impl FlstBaseNode {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            len: util::u32_val(&buf, addr),
            first: FilAddr::new(addr + 4, buf.clone()),
            last: FilAddr::new(addr + 10, buf.clone()),
            buf,
            addr,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FlstNode {
    pub prev: FilAddr,
    pub next: FilAddr,
}

impl FlstNode {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            prev: FilAddr::new(addr, buf.clone()),
            next: FilAddr::new(addr + 6, buf.clone()),
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FilAddr {
    pub page: u32,    // Page number within a space
    pub boffset: u16, // Byte offset within the page
}

impl FilAddr {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            page: util::u32_val(&buf, addr),
            boffset: util::u16_val(&buf, addr + 4),
        }
    }
}

/// FSP Header, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FileSpaceHeader {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    /// Table space ID
    pub space_id: u32,
    /// not used now
    pub notused: u32,
    /// Current size of the space in pages
    pub fsp_size: u32,
    /// Minimum page number for which the free list has not been initialized
    pub free_limit: u32,
    /// fsp_space_t.flags, see fsp0types.h
    pub fsp_flags: u32,
    /// number of used pages in the FSP_FREE_FRAG list
    pub fsp_frag_n_used: u32,
    /// list of free extents
    pub fsp_free: FlstBaseNode,
    /// list of partially free extents not belonging to any segment
    pub free_frag: FlstBaseNode,
    /// list of full extents not belonging to any segment
    pub full_frag: FlstBaseNode,
    /// next segemnt id, 8 bytes which give the first unused segment id
    pub segid: u64,
    /// list of pages containing segment headers, where all the segment inode
    /// slots are reserved
    pub inodes_full: FlstBaseNode,
    /// list of pages containing segment headers, where not all the segment
    /// header slots are reserved
    pub inodes_free: FlstBaseNode,
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
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
            segid: util::u64_val(&buf, addr + 72),
            inodes_full: FlstBaseNode::new(addr + 80, buf.clone()),
            inodes_free: FlstBaseNode::new(addr + 96, buf.clone()),
            buf,
            addr,
        }
    }
}

// File Space Header Page
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FileSpaceHeaderPage {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    pub fsp_hdr: FileSpaceHeader,
    pub xdes_ent_list: Vec<XDesEntry>,
    pub sdi_meta_data: Option<SdiMetaInfo>,
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,
}

impl FileSpaceHeaderPage {
    // INFO_SIZE = 3 + 4 + 32*2 + 36 + 4 = 111
    // static constexpr size_t INFO_SIZE =
    //     (MAGIC_SIZE + sizeof(uint32) + (KEY_LEN * 2) + SERVER_UUID_LEN +
    //      sizeof(uint32));
    // INFO_MAX_SIZE = 111 + 4 = 115
    // static constexpr size_t INFO_MAX_SIZE = INFO_SIZE + sizeof(uint32);
    const INFO_MAX_SIZE: usize = 115;

    pub fn parse_sdi_meta(&mut self) {
        // sdi_addr, page offset = 10505
        let sdi_addr = FSP_HEADER_SIZE + XDES_ENTRY_MAX_COUNT * XDES_ENTRY_SIZE + Self::INFO_MAX_SIZE;

        // info!("len = {}, sdi_addr = {}", len, sdi_addr);
        let sdi_meta = SdiMetaInfo::new(self.buf.slice(sdi_addr..sdi_addr + 8));

        self.sdi_meta_data = Some(sdi_meta);
    }
}

impl BasePageOperation for FileSpaceHeaderPage {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let hdr = FileSpaceHeader::new(addr, buf.clone());
        let mut entries = Vec::new();
        let len: usize =
            (hdr.fsp_free.len + hdr.free_frag.len + hdr.full_frag.len + hdr.inodes_free.len + hdr.inodes_full.len)
                as usize;
        for offset in 0..len {
            let beg = FSP_HEADER_SIZE + offset * XDES_ENTRY_SIZE;
            let end = beg + XDES_ENTRY_SIZE;
            entries.push(XDesEntry::new(beg, buf.clone()));
        }

        Self {
            fsp_hdr: hdr,
            xdes_ent_list: entries,
            sdi_meta_data: None,
            buf,
            addr,
        }
    }
}

#[repr(u32)]
#[derive(Debug, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
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

// Extent Descriptor Entry, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct XDesEntry {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    seg_id: u64,         // seg_id
    flst_node: FlstNode, // list node data
    state: XDesStates,   // state information
    bitmap: Bytes,       // bitmap
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
}

impl XDesEntry {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            seg_id: util::u64_val(&buf, addr),
            flst_node: FlstNode::new(addr + 8, buf.clone()),
            state: util::u32_val(&buf, addr + 20).into(),
            bitmap: buf.clone().slice(addr + 24..addr + 40),
            buf,
            addr,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiMetaInfo {
    pub sdi_version: u32, // SDI Version
    pub sdi_page_no: u32, // SDI Page Number
}

impl SdiMetaInfo {
    pub fn new(buffer: Bytes) -> Self {
        Self {
            sdi_version: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            sdi_page_no: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
        }
    }
}

// File Segment Inode, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct INodePage {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    pub inode_flst_node: FlstNode,
    pub inode_ent_list: Vec<INodeEntry>,
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
}

impl BasePageOperation for INodePage {
    fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let mut entries = Vec::new();
        for offset in 0..INODE_ENTRY_MAX_COUNT {
            let beg = addr + INODE_FLST_NODE_SIZE + offset * INODE_ENTRY_SIZE;
            let end = beg + INODE_ENTRY_SIZE;
            let entry = INodeEntry::new(beg, buf.clone());
            info!("0x{:08x}", &entry.fseg_magic_n);
            if entry.fseg_magic_n == 0 || entry.fseg_id == 0 {
                break;
            }
            entries.push(entry);
        }
        Self {
            inode_flst_node: FlstNode::new(addr, buf.clone()),
            inode_ent_list: entries,
            buf,
            addr,
        }
    }
}

// INode Entry, see fsp0fsp.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct INodeEntry {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    fseg_id: u64,
    fseg_not_full_n_used: u32,
    fseg_free: FlstBaseNode,
    fseg_not_full: FlstBaseNode,
    fseg_full: FlstBaseNode,
    fseg_magic_n: u32, // FSEG_MAGIC_N_VALUE = 97937874;
    fseg_frag_arr: [u32; INODE_ENTRY_ARR_COUNT],
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer
}

impl INodeEntry {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let mut arr = [0u32; INODE_ENTRY_ARR_COUNT];
        for (offset, element) in arr.iter_mut().enumerate() {
            let beg = addr + 64 + offset * FRAG_ARR_ENTRY_SIZE;
            let end = beg + FRAG_ARR_ENTRY_SIZE;
            *element = util::u32_val(&buf, beg);
        }

        Self {
            fseg_id: util::u64_val(&buf, addr),
            fseg_not_full_n_used: util::u32_val(&buf, addr + 8),
            fseg_free: FlstBaseNode::new(addr + 12, buf.clone()),
            fseg_not_full: FlstBaseNode::new(addr + 28, buf.clone()),
            fseg_full: FlstBaseNode::new(addr + 44, buf.clone()),
            fseg_magic_n: util::u32_val(&buf, addr + 60),
            fseg_frag_arr: arr,
            buf,
            addr,
        }
    }
}

// Index Page
pub struct IndexPage {
    /// Index Header
    index_header: IndexHeader, // Index Header
    /// FSEG Header
    fseg_header: FSegHeader, // FSEG Header

    /// System Record
    infimum: RecordHeader, // infimum_extra[], see page0page.h
    supremum: RecordHeader, // supremum_extra_data[], see page0page.h

    /// User Records, grow down
    records: Vec<Record>, // User Record List

    ////////////////////////////////////////
    //
    //  Free Space
    //
    ////////////////////////////////////////
    /// Page Directory, grows up
    dir_slots: Vec<u16>, // page directory slots

    buf: Arc<Bytes>,
}

impl fmt::Debug for IndexPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexPage")
            .field("index_header", &self.index_header)
            .field("fseg_header", &self.fseg_header)
            .field("infimum", &self.infimum)
            .field("supremum", &self.supremum)
            .field("records", &self.records)
            .field("dir_slots", &format!("{:?}", &self.dir_slots))
            .finish()
    }
}

impl BasePageOperation for IndexPage {
    fn new(addr: usize, buffer: Arc<Bytes>) -> Self {
        let idx_hdr = IndexHeader::new(buffer.slice(0..36));

        // Parse Page Directory Slots
        let n_slots = idx_hdr.page_n_dir_slots as usize;
        let mut slots = vec![0; n_slots];
        for (offset, element) in slots.iter_mut().enumerate() {
            let end = buffer.len() - offset * PAGE_DIR_ENTRY_SIZE;
            let beg = end - PAGE_DIR_ENTRY_SIZE;
            *element = u16::from_be_bytes(buffer.as_ref()[beg..end].try_into().unwrap());
        }

        Self {
            index_header: idx_hdr,
            fseg_header: FSegHeader::new(buffer.slice(36..56)),
            infimum: RecordHeader::new(buffer.slice(56..69)),
            supremum: RecordHeader::new(buffer.slice(69..82)),
            records: Vec::new(),
            dir_slots: slots,
            buf: buffer,
        }
    }
}

impl IndexPage {
    pub fn parse_records(&mut self, tabdef: Arc<TableDef>) -> Result<(), Error> {
        let inf = &self.infimum;
        let urecs = &mut self.records;
        let mut addr = (PAGE_ADDR_INF - FIL_HEADER_SIZE) as i16;
        addr += inf.next_rec_offset;

        let idxdef = &tabdef.idx_defs[0];
        assert_eq!(idxdef.idx_name, "PRIMARY");

        for _nrec in 0..self.index_header.page_n_recs {
            let mut end = addr as usize;
            let rec_hdr = RecordHeader::new(self.buf.slice(end - 5..end));

            end -= 5;
            let mut narr = self.buf.slice(end - idxdef.null_size..end).to_vec();
            narr.reverse();
            end -= idxdef.null_size;
            let mut varr = self.buf.slice(end - idxdef.vfld_size..end).to_vec();
            varr.reverse();
            let rowinfo = RowInfo::new(varr, narr, tabdef.clone());
            debug!("rowinfo={:?}", &rowinfo);

            end = addr as usize;
            let rdi = rowinfo.dyninfo(idxdef);
            debug!("Row Dynamic Info = {:?}", &rdi);
            let total: usize = rdi.iter().map(|e| e.1).sum();
            let rbuf = self.buf.slice(end..end + total);
            let row = Row::new(end + FIL_HEADER_SIZE, rbuf, tabdef.clone(), rdi);

            addr += rec_hdr.next_rec_offset;
            let mut urec = Record::new(rec_hdr, rowinfo, row);
            urec.unpack(idxdef);

            urecs.push(urec);
        }
        assert_eq!(addr as usize, PAGE_ADDR_SUP - FIL_HEADER_SIZE);

        Ok(())
    }

    pub fn records(&self) -> &Vec<Record> {
        &self.records
    }
}

#[repr(u8)]
#[derive(Debug, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageFormats {
    REDUNDANT = 0,
    COMPACT = 1,
    #[default]
    UNDEF,
}

#[repr(u16)]
#[derive(Debug, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageDirections {
    PAGE_LEFT = 1,
    PAGE_RIGHT = 2,
    PAGE_SAME_REC = 3,
    PAGE_SAME_PAGE = 4,
    PAGE_NO_DIRECTION = 5,
    #[default]
    UNDEF,
}

// Index Page Header, see page0types.h
#[derive(Debug)]
pub struct IndexHeader {
    /// number of slots in page directory
    page_n_dir_slots: u16,
    /// pointer to record heap top
    page_heap_top: u16,
    /// number of records in the heap, bit 15=flag: new-style compact page format
    page_format: PageFormats,
    page_n_heap: u16,
    /// pointer to start of page free record list
    page_free: u16,
    /// number of bytes in deleted records
    page_garbage: u16,
    /// pointer to the last inserted record, or NULL if this info has been reset by a deletion
    page_last_insert: u16,
    /// last insert direction: PAGE_LEFT, ...
    page_direction: PageDirections,
    /// number of consecutive inserts to the same direction
    page_n_direction: u16,
    /// number of user records on the page
    page_n_recs: u16,
    /// highest id of a trx which may have modified a record on the page; trx_id_t;
    /// defined only in secondary indexes and in the insert buffer tree
    page_max_trx_id: u64,
    /// level of the node in an index tree; the leaf level is the level 0. This
    /// field should not be written to after page creation.
    page_level: u16,
    /// index id where the page belongs. This field should not be written to after page creation.
    page_index_id: u64,
}

impl IndexHeader {
    pub fn new(buffer: Bytes) -> Self {
        let n_heap = u16::from_be_bytes(buffer.as_ref()[4..6].try_into().unwrap());
        let fmt_flag = ((n_heap & 0x8000) >> 15) as u8;
        let page_direct = u16::from_be_bytes(buffer.as_ref()[12..14].try_into().unwrap());
        Self {
            page_n_dir_slots: u16::from_be_bytes(buffer.as_ref()[..2].try_into().unwrap()),
            page_heap_top: u16::from_be_bytes(buffer.as_ref()[2..4].try_into().unwrap()),
            page_format: fmt_flag.into(),
            page_n_heap: n_heap,
            page_free: u16::from_be_bytes(buffer.as_ref()[6..8].try_into().unwrap()),
            page_garbage: u16::from_be_bytes(buffer.as_ref()[8..10].try_into().unwrap()),
            page_last_insert: u16::from_be_bytes(buffer.as_ref()[10..12].try_into().unwrap()),
            page_direction: page_direct.into(),
            page_n_direction: u16::from_be_bytes(buffer.as_ref()[14..16].try_into().unwrap()),
            page_n_recs: u16::from_be_bytes(buffer.as_ref()[16..18].try_into().unwrap()),
            page_max_trx_id: u64::from_be_bytes(buffer.as_ref()[18..26].try_into().unwrap()),
            page_level: u16::from_be_bytes(buffer.as_ref()[26..28].try_into().unwrap()),
            page_index_id: u64::from_be_bytes(buffer.as_ref()[28..].try_into().unwrap()),
        }
    }
}

// File Segment Header, see fsp0types.h/page0types.h
#[derive(Debug)]
pub struct FSegHeader {
    /// leaf page
    leaf_space_id: u32, // space id
    leaf_page_no: u32, // page number
    leaf_offset: u16,  // byte offset
    /// non-leaf page
    nonleaf_space_id: u32, // space id
    nonleaf_page_no: u32, // page number
    nonleaf_offset: u16, // byte offset
}

impl FSegHeader {
    pub fn new(buffer: Bytes) -> Self {
        Self {
            leaf_space_id: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            leaf_page_no: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
            leaf_offset: u16::from_be_bytes(buffer.as_ref()[8..10].try_into().unwrap()),
            nonleaf_space_id: u32::from_be_bytes(buffer.as_ref()[10..14].try_into().unwrap()),
            nonleaf_page_no: u32::from_be_bytes(buffer.as_ref()[14..18].try_into().unwrap()),
            nonleaf_offset: u16::from_be_bytes(buffer.as_ref()[18..20].try_into().unwrap()),
        }
    }
}

// SDI Index Page, see ibd2sdi.cc
pub struct SdiIndexPage {
    index: IndexPage,        // common Index Page
    sdi_hdr: SdiDataHeader,  // SDI Data Header
    pub uncomp_data: String, // unzipped SDI Data, a JSON string
}

impl fmt::Debug for SdiIndexPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let _trunc = cmp::min(self.uncomp_data.len(), 520);
        f.debug_struct("SdiIndexPage")
            .field("index", &self.index)
            .field("sdi_hdr", &self.sdi_hdr)
            .finish()
    }
}

impl BasePageOperation for SdiIndexPage {
    fn new(addr: usize, buffer: Arc<Bytes>) -> Self {
        let index = IndexPage::new(addr, buffer.clone());
        let beg = PAGE_ADDR_INF - FIL_HEADER_SIZE + index.infimum.next_rec_offset as usize;
        let end = beg + 33;
        let hdr = SdiDataHeader::new(buffer.slice(beg..end));
        debug!(
            "beg={}, end={}, comp_len={}, umcomp_len={}",
            beg.to_string().green(),
            end.to_string().magenta(),
            hdr.comp_len.to_string().yellow(),
            hdr.uncomp_len.to_string().yellow()
        );

        let comped_data = buffer.slice(end..end + (hdr.comp_len as usize));
        let uncomped_data = util::zlib_uncomp(comped_data).unwrap();
        assert_eq!(uncomped_data.len(), hdr.uncomp_len as usize);

        Self {
            index,
            sdi_hdr: hdr,
            uncomp_data: uncomped_data,
        }
    }
}

impl SdiIndexPage {
    pub fn get_sdi_object(&self) -> SdiObject {
        if self.uncomp_data.is_empty() {
            panic!("ERR_SID_UNCOMP_STRING_EMPTY");
        }
        serde_json::from_str(&self.uncomp_data).expect("ERR_SDI_STRING")
    }
}

#[derive(Debug)]
pub struct SdiDataHeader {
    /// Length of TYPE field in record of SDI Index.
    data_type: u32, // 4 bytes
    /// Length of ID field in record of SDI Index.
    data_id: u64, // 8 bytes
    /// trx id
    trx_id: u64, // 6 bytes
    /// 7-byte roll-ptr.
    roll_ptr: u64, // 7 bytes
    /// Length of UNCOMPRESSED_LEN field in record of SDI Index.
    uncomp_len: u32, // 4 bytes
    /// Length of COMPRESSED_LEN field in record of SDI Index.
    comp_len: u32, // 4 bytes
}

impl SdiDataHeader {
    pub fn new(buffer: Bytes) -> Self {
        Self {
            data_type: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            data_id: u64::from_be_bytes(buffer.as_ref()[4..12].try_into().unwrap()),
            trx_id: util::from_bytes6(buffer.slice(12..18)),
            roll_ptr: util::from_bytes7(buffer.slice(18..25)),
            uncomp_len: u32::from_be_bytes(buffer.as_ref()[25..29].try_into().unwrap()),
            comp_len: u32::from_be_bytes(buffer.as_ref()[29..33].try_into().unwrap()),
        }
    }
}
