use bytes::Bytes;

use enum_display::EnumDisplay;
use log::{debug, info};
use std::fmt::Formatter;
use std::{cmp, fmt};

use crate::ibd::record::{RecordHeader, PAGE_ADDR_INF};
use crate::util;

use super::record::{Record, SdiObject};

pub const PAGE_SIZE: usize = 16 * 1024;

pub const FIL_HEADER_SIZE: usize = 38;
pub const FIL_TRAILER_SIZE: usize = 8;
pub const FSP_HEADER_SIZE: usize = 112;
pub const XDES_ENTRY_SIZE: usize = 40;
pub const INODE_FLST_NODE_SIZE: usize = 12;
pub const INODE_ENTRY_SIZE: usize = 192;
pub const INODE_ENTRY_COUNT: usize = 85;
pub const INODE_ENTRY_ARR_COUNT: usize = 32;
pub const FRAG_ARR_ENTRY_SIZE: usize = 4;
pub const PAGE_DIR_ENTRY_SIZE: usize = 2;

/// MySQL Page Type, see fil0fil.h
#[repr(u16)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, EnumDisplay, Clone, Eq, PartialEq, Ord, PartialOrd)]
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
    MARKED(u16),
}

impl From<u16> for PageTypes {
    fn from(value: u16) -> Self {
        match value {
            0 => PageTypes::ALLOCATED,
            1 => PageTypes::UNUSED,
            2 => PageTypes::UNDO_LOG,
            3 => PageTypes::INODE,
            4 => PageTypes::IBUF_FREE_LIST,
            5 => PageTypes::IBUF_BITMAP,
            6 => PageTypes::SYS,
            7 => PageTypes::TRX_SYS,
            8 => PageTypes::FSP_HDR,
            9 => PageTypes::XDES,
            10 => PageTypes::BLOB,
            11 => PageTypes::ZBLOB,
            12 => PageTypes::ZBLOB2,
            13 => PageTypes::UNKNOWN,
            14 => PageTypes::COMPRESSED,
            15 => PageTypes::ENCRYPTED,
            16 => PageTypes::COMPRESSED_AND_ENCRYPTED,
            17 => PageTypes::ENCRYPTED_RTREE,
            18 => PageTypes::SDI_BLOB,
            19 => PageTypes::SDI_ZBLOB,
            20 => PageTypes::LEGACY_DBLWR,
            21 => PageTypes::RSEG_ARRAY,
            22 => PageTypes::LOB_INDEX,
            23 => PageTypes::LOB_DATA,
            24 => PageTypes::LOB_FIRST,
            25 => PageTypes::ZLOB_FIRST,
            26 => PageTypes::ZLOB_DATA,
            27 => PageTypes::ZLOB_INDEX,
            28 => PageTypes::ZLOB_FRAG,
            29 => PageTypes::ZLOB_FRAG_ENTRY,
            17853 => PageTypes::SDI,
            17855 => PageTypes::INDEX,
            17854 => PageTypes::RTREE,
            _ => PageTypes::MARKED(value),
        }
    }
}

/// FIL Header, see fil0types.h
#[derive(Clone)]
pub struct FilePageHeader {
    pub check_sum: u32,       // check_sum, FIL_PAGE_SPACE_OR_CHKSUM
    pub page_no: u32,         // page_number/offset, FIL_PAGE_OFFSET
    pub prev_page: u32,       // Previous Page, FIL_PAGE_PREV
    pub next_page: u32,       // Next Page, FIL_PAGE_NEXT
    pub lsn: u64,             // LSN for last page modification, FIL_PAGE_LSN
    pub page_type: PageTypes, // Page Type, FIL_PAGE_TYPE
    pub flush_lsn: u64,       // Flush LSN, FIL_PAGE_FILE_FLUSH_LSN
    pub space_id: u32,        // Space ID, FIL_PAGE_SPACE_ID
}

impl fmt::Debug for FilePageHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilePageHeader")
            .field("check_sum", &format!("0x{:08x}", self.check_sum))
            .field("page_no", &self.page_no)
            .field(
                "prev_page",
                &format!("0x{:08x} ({})", self.prev_page, self.prev_page),
            )
            .field(
                "next_page",
                &format!("0x{:08x} ({})", self.next_page, self.next_page),
            )
            .field("lsn", &format!("0x{:016x} ({})", self.lsn, self.lsn))
            .field("page_type", &self.page_type)
            .field(
                "flush_lsn",
                &format!("0x{:016x} ({})", self.flush_lsn, self.flush_lsn),
            )
            .field("space_id", &self.space_id)
            .finish()
    }
}

impl FilePageHeader {
    pub fn new<B>(buffer: B) -> Self
    where
        B: AsRef<[u8]>,
    {
        Self {
            check_sum: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            page_no: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
            prev_page: u32::from_be_bytes(buffer.as_ref()[8..12].try_into().unwrap()),
            next_page: u32::from_be_bytes(buffer.as_ref()[12..16].try_into().unwrap()),
            lsn: u64::from_be_bytes(buffer.as_ref()[16..24].try_into().unwrap()),
            page_type: u16::from_be_bytes(buffer.as_ref()[24..26].try_into().unwrap()).into(),
            flush_lsn: u64::from_be_bytes(buffer.as_ref()[26..34].try_into().unwrap()),
            space_id: u32::from_be_bytes(buffer.as_ref()[34..38].try_into().unwrap()),
        }
    }
}

/// FIL Trailer, see fil0types.h
pub struct FilePageTrailer {
    check_sum: u32,    // Old-style Checksum, FIL_PAGE_END_LSN_OLD_CHKSUM
    lsn_low32bit: u32, // Low 32-bits of LSN, last 4 bytes of FIL_PAGE_LSN
}

impl fmt::Debug for FilePageTrailer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilePageTrailer")
            .field("check_sum", &format!("0x{:08x}", self.check_sum))
            .field(
                "lsn_low32bit",
                &format!("0x{:08x} ({})", self.lsn_low32bit, self.lsn_low32bit),
            )
            .finish()
    }
}

impl FilePageTrailer {
    pub fn new<B>(buffer: B) -> Self
    where
        B: AsRef<[u8]>,
    {
        Self {
            check_sum: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            lsn_low32bit: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
        }
    }
}

#[derive(Debug)]
pub struct FlstBaseNode {
    pub len: u32,
    pub first: FilAddr,
    pub last: FilAddr,
}

impl FlstBaseNode {
    pub fn new(buffer: &[u8]) -> Self {
        Self {
            len: u32::from_be_bytes(buffer[..4].try_into().unwrap()),
            first: FilAddr::new(&buffer[4..10]),
            last: FilAddr::new(&buffer[10..16]),
        }
    }
}

#[derive(Debug)]
pub struct FlstNode {
    pub prev: FilAddr,
    pub next: FilAddr,
}

impl FlstNode {
    pub fn new(buffer: &[u8]) -> Self {
        Self {
            prev: FilAddr::new(&buffer[..6]),
            next: FilAddr::new(&buffer[6..12]),
        }
    }
}

pub struct FilAddr {
    pub page: u32,    // Page number within a space
    pub boffset: u16, // Byte offset within the page
}

impl fmt::Debug for FilAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FilAddr{{page: \"0x{:08x} ({})\", boffset: {}}}",
            self.page, self.page, self.boffset
        )
    }
}

impl FilAddr {
    pub fn new(buffer: &[u8]) -> Self {
        Self {
            page: u32::from_be_bytes(buffer[..4].try_into().unwrap()),
            boffset: u16::from_be_bytes(buffer[4..6].try_into().unwrap()),
        }
    }
}

/// FSP Header, see fsp0fsp.h
pub struct FileSpaceHeader {
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
}

impl fmt::Debug for FileSpaceHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSpaceHeader")
            .field("space_id", &self.space_id)
            .field("notused", &self.notused)
            .field("fsp_size", &self.fsp_size)
            .field("free_limit", &self.free_limit)
            .field(
                "fsp_flags",
                &format!("0x{:08x} ({})", self.fsp_flags, self.fsp_flags),
            )
            .field("fsp_frag_n_used", &self.fsp_frag_n_used)
            .field("fsp_free", &self.fsp_free)
            .field("free_frag", &self.free_frag)
            .field("full_frag", &self.full_frag)
            .field("segid", &self.segid)
            .field("inodes_full", &self.inodes_full)
            .field("inodes_free", &self.inodes_free)
            .finish()
    }
}

impl FileSpaceHeader {
    pub fn new<B>(buffer: B) -> Self
    where
        B: AsRef<[u8]>,
    {
        Self {
            space_id: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            notused: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
            fsp_size: u32::from_be_bytes(buffer.as_ref()[8..12].try_into().unwrap()),
            free_limit: u32::from_be_bytes(buffer.as_ref()[12..16].try_into().unwrap()),
            fsp_flags: u32::from_be_bytes(buffer.as_ref()[16..20].try_into().unwrap()),
            fsp_frag_n_used: u32::from_be_bytes(buffer.as_ref()[20..24].try_into().unwrap()),
            fsp_free: FlstBaseNode::new(&buffer.as_ref()[24..40]),
            free_frag: FlstBaseNode::new(&buffer.as_ref()[40..56]),
            full_frag: FlstBaseNode::new(&buffer.as_ref()[56..72]),
            segid: u64::from_be_bytes(buffer.as_ref()[72..80].try_into().unwrap()),
            inodes_full: FlstBaseNode::new(&buffer.as_ref()[80..96]),
            inodes_free: FlstBaseNode::new(&buffer.as_ref()[96..112]),
        }
    }
}

// Base Page Structure
#[derive(Debug)]
pub struct BasePage<P> {
    pub fil_hdr: FilePageHeader,
    pub body: P,
    pub fil_trl: FilePageTrailer,
}

pub trait BasePageOperation {
    fn new(buffer: Bytes) -> Self;
}

impl<P> BasePage<P>
where
    P: BasePageOperation,
{
    pub fn new(header: FilePageHeader, buffer: Bytes, trailer: FilePageTrailer) -> BasePage<P> {
        Self {
            fil_hdr: header,
            body: BasePageOperation::new(buffer),
            fil_trl: trailer,
        }
    }
}

// File Space Header Page
#[derive(Debug)]
pub struct FileSpaceHeaderPage {
    pub fsp_hdr: FileSpaceHeader,
    pub xdes_ent_list: Vec<XDesEntry>,
}

impl BasePageOperation for FileSpaceHeaderPage {
    fn new(buffer: Bytes) -> Self {
        let hdr = FileSpaceHeader::new(buffer.slice(..FSP_HEADER_SIZE));
        let mut entries = Vec::new();
        let len = hdr.fsp_free.len
            + hdr.free_frag.len
            + hdr.full_frag.len
            + hdr.inodes_free.len
            + hdr.inodes_full.len;
        for offset in 0..len as usize {
            let beg = FSP_HEADER_SIZE + offset * XDES_ENTRY_SIZE;
            let end = beg + XDES_ENTRY_SIZE;
            entries.push(XDesEntry::new(buffer.slice(beg..end)));
        }
        Self {
            fsp_hdr: hdr,
            xdes_ent_list: entries,
        }
    }
}

#[repr(u32)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, EnumDisplay, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum XDesStates {
    XDES_NOT_INITED = 0, // extent descriptor is not initialized
    XDES_FREE = 1,       // extent is in free list of space
    XDES_FREE_FRAG = 2,  // extent is in free fragment list of space
    XDES_FULL_FRAG = 3,  // extent is in full fragment list of space
    XDES_FSEG = 4,       // extent belongs to a segment
    XDES_FSEG_FRAG = 5,  // fragment extent leased to segment
    MARKED(u32),
}

impl From<u32> for XDesStates {
    fn from(value: u32) -> Self {
        match value {
            0 => XDesStates::XDES_NOT_INITED, // extent descriptor is not initialized
            1 => XDesStates::XDES_FREE,       // extent is in free list of space
            2 => XDesStates::XDES_FREE_FRAG,  // extent is in free fragment list of space
            3 => XDesStates::XDES_FULL_FRAG,  // extent is in full fragment list of space
            4 => XDesStates::XDES_FSEG,       // extent belongs to a segment
            5 => XDesStates::XDES_FSEG_FRAG,  // fragment extent leased to segment
            _ => XDesStates::MARKED(value),
        }
    }
}

// Extent Descriptor Entry, see fsp0fsp.h
#[derive(Debug)]
pub struct XDesEntry {
    seg_id: u64,         // seg_id
    flst_node: FlstNode, // list node data
    state: XDesStates,   // state information
    bitmap: Bytes,       // bitmap
}

impl XDesEntry {
    pub fn new(buffer: Bytes) -> Self {
        Self {
            seg_id: u64::from_be_bytes(buffer.as_ref()[..8].try_into().unwrap()),
            flst_node: FlstNode::new(&buffer.as_ref()[8..20]),
            state: u32::from_be_bytes(buffer.as_ref()[20..24].try_into().unwrap()).into(),
            bitmap: buffer.slice(24..40),
        }
    }
}

// File Segment Inode, see fsp0fsp.h
#[derive(Debug)]
pub struct INodePage {
    pub inode_flst_node: FlstNode,
    pub inode_ent_list: Vec<INodeEntry>,
}

impl BasePageOperation for INodePage {
    fn new(buffer: Bytes) -> Self {
        let mut entries = Vec::new();
        for offset in 0..INODE_ENTRY_COUNT {
            let beg = INODE_FLST_NODE_SIZE + offset * INODE_ENTRY_SIZE;
            let end = beg + INODE_ENTRY_SIZE;
            let entry = INodeEntry::new(buffer.slice(beg..end));
            info!("0x{:08x}", &entry.fseg_magic_n);
            if entry.fseg_magic_n == 0 || entry.fseg_id == 0 {
                break;
            }
            entries.push(entry);
        }
        Self {
            inode_flst_node: FlstNode::new(&buffer.as_ref()[0..INODE_FLST_NODE_SIZE]),
            inode_ent_list: entries,
        }
    }
}

// INode Entry, see fsp0fsp.h
pub struct INodeEntry {
    fseg_id: u64,
    fseg_not_full_n_used: u32,
    fseg_free: FlstBaseNode,
    fseg_not_full: FlstBaseNode,
    fseg_full: FlstBaseNode,
    fseg_magic_n: u32, // FSEG_MAGIC_N_VALUE = 97937874;
    fseg_frag_arr: [u32; INODE_ENTRY_ARR_COUNT],
}

impl fmt::Debug for INodeEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let frag_arr_str = &self
            .fseg_frag_arr
            .iter()
            .map(|e| format!("0x{:08x}", e))
            .collect::<Vec<_>>()
            .join(", ");
        f.debug_struct("INodeEntry")
            .field("fseg_id", &self.fseg_id)
            .field("fseg_not_full_n_used", &self.fseg_not_full_n_used)
            .field("fseg_free", &self.fseg_free)
            .field("fseg_not_full", &self.fseg_not_full)
            .field("fseg_full", &self.fseg_full)
            .field("fseg_magic_n", &self.fseg_magic_n)
            .field("fseg_frag_arr", &format!("[{}]", frag_arr_str))
            .finish()
    }
}

impl INodeEntry {
    pub fn new(buffer: Bytes) -> Self {
        let mut arr = [0u32; INODE_ENTRY_ARR_COUNT];
        for (offset, element) in arr.iter_mut().enumerate() {
            let beg = 64 + offset * FRAG_ARR_ENTRY_SIZE;
            let end = beg + FRAG_ARR_ENTRY_SIZE;
            *element = u32::from_be_bytes(buffer.as_ref()[beg..end].try_into().unwrap());
        }

        Self {
            fseg_id: u64::from_be_bytes(buffer.as_ref()[..8].try_into().unwrap()),
            fseg_not_full_n_used: u32::from_be_bytes(buffer.as_ref()[8..12].try_into().unwrap()),
            fseg_free: FlstBaseNode::new(&buffer.as_ref()[12..28]),
            fseg_not_full: FlstBaseNode::new(&buffer.as_ref()[28..44]),
            fseg_full: FlstBaseNode::new(&buffer.as_ref()[44..60]),
            fseg_magic_n: u32::from_be_bytes(buffer.as_ref()[60..64].try_into().unwrap()),
            fseg_frag_arr: arr,
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
    infimum: Record, // infimum_extra[], see page0page.h
    supremum: Record, // supremum_extra_data[], see page0page.h

    /// User Records, grow down
    records: Vec<Record>, // User Record List

    ////////////////////////////////////////
    //
    //  Free Space
    //
    ////////////////////////////////////////
    /// Page Directory, grows up
    dir_slots: Vec<u16>, // page directory slots
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
    fn new(buffer: Bytes) -> Self {
        let idx_hdr = IndexHeader::new(buffer.slice(0..36));

        // Parse Page Directory Slots
        let n_slots = idx_hdr.page_n_dir_slots as usize;
        let mut slots = vec![0; n_slots];
        for (offset, element) in slots.iter_mut().enumerate() {
            let end = buffer.len() - offset * PAGE_DIR_ENTRY_SIZE;
            let beg = end - PAGE_DIR_ENTRY_SIZE;
            *element = u16::from_be_bytes(buffer.as_ref()[beg..end].try_into().unwrap());
        }

        // Parse System Records
        let inf = Record::new(RecordHeader::new(buffer.slice(56..69)));
        let sup = Record::new(RecordHeader::new(buffer.slice(69..82)));

        // Parse User Records
        let mut urecs = Vec::new();
        let mut addr = PAGE_ADDR_INF + (inf.rec_hdr.next_rec_offset as usize) - FIL_HEADER_SIZE;
        for nrec in 0..idx_hdr.page_n_recs {
            debug!("Parse Record, nrec = {}", nrec);
            if addr > buffer.len() {
                break;
            }
            let rec_hdr = RecordHeader::new(buffer.slice(addr - 5..addr));
            addr += rec_hdr.next_rec_offset as usize;
            urecs.push(Record::new(rec_hdr));
        }

        Self {
            index_header: idx_hdr,
            fseg_header: FSegHeader::new(buffer.slice(36..56)),
            infimum: inf,
            supremum: sup,
            records: urecs,
            dir_slots: slots,
        }
    }
}

#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, EnumDisplay, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageFormats {
    REDUNDANT = 0,
    COMPACT = 1,
    MARKED(u8),
}

impl From<u8> for PageFormats {
    fn from(value: u8) -> Self {
        match value {
            0 => PageFormats::REDUNDANT,
            1 => PageFormats::COMPACT,
            _ => PageFormats::MARKED(value),
        }
    }
}

#[repr(u16)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, EnumDisplay, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageDirections {
    PAGE_LEFT = 1,
    PAGE_RIGHT = 2,
    PAGE_SAME_REC = 3,
    PAGE_SAME_PAGE = 4,
    PAGE_NO_DIRECTION = 5,
    MARKED(u16),
}

impl From<u16> for PageDirections {
    fn from(value: u16) -> Self {
        match value {
            1 => PageDirections::PAGE_LEFT,
            2 => PageDirections::PAGE_RIGHT,
            3 => PageDirections::PAGE_SAME_REC,
            4 => PageDirections::PAGE_SAME_PAGE,
            5 => PageDirections::PAGE_NO_DIRECTION,
            _ => PageDirections::MARKED(value),
        }
    }
}

// Index Page Header, see page0types.h
#[derive(Debug)]
pub struct IndexHeader {
    /// number of slots in page directory
    page_n_dir_slots: u16,
    /// pointer to record heap top
    page_heap_top: u16,
    /// number of records in the heap, bit 15=flag: new-style compact page
    /// format
    page_format: PageFormats,
    page_n_heap: u16,
    /// pointer to start of page free record list
    page_free: u16,
    /// number of bytes in deleted records
    page_garbage: u16,
    /// pointer to the last inserted record, or NULL if this info has been reset
    /// by a delete, for example
    page_last_insert: u16,
    /// last insert direction: PAGE_LEFT, ...
    page_direction: PageDirections,
    /// number of consecutive inserts to the same direction
    page_n_direction: u16,
    /// number of user records on the page
    page_n_recs: u16,
    /// highest id of a trx which may have modified a record on the page;
    /// trx_id_t; defined only in secondary indexes and in the insert buffer
    /// tree
    page_max_trx_id: u64,
    /// level of the node in an index tree; the leaf level is the level 0. This
    /// field should not be written to after page creation.
    page_level: u16,
    /// index id where the page belongs. This field should not be written to
    /// after page creation.
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
    index: IndexPage,
    sdi_hdr: SdiRecord,
    sdi_str: String,
}

impl fmt::Debug for SdiIndexPage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let trunc = cmp::min(self.sdi_str.len(), 220);
        f.debug_struct("SdiIndexPage")
            .field("index", &self.index)
            .field("sdi_hdr", &self.sdi_hdr)
            .field("sdi_str", &&self.sdi_str[0..trunc])
            .finish()
    }
}

impl BasePageOperation for SdiIndexPage {
    fn new(buffer: Bytes) -> Self {
        let index = IndexPage::new(buffer.clone());
        let beg = PAGE_ADDR_INF - FIL_HEADER_SIZE + index.infimum.rec_hdr.next_rec_offset as usize;
        let end = beg + 33;
        let sdi_rec = SdiRecord::new(buffer.slice(beg..end));
        debug!("beg={}, end={}, comp_len={}", beg, end, sdi_rec.comp_len);

        let data = buffer.slice(end..end + (sdi_rec.comp_len as usize));
        let out = util::uncomp(data).unwrap();
        assert_eq!(out.len(), sdi_rec.uncomp_len as usize);

        Self {
            index,
            sdi_hdr: sdi_rec,
            sdi_str: out,
        }
    }
}

impl SdiIndexPage {
    pub fn get_sdi_data(&self) -> String {
        if !self.sdi_str.is_empty() {
            jsonxf::pretty_print(&self.sdi_str).unwrap()
        } else {
            "".into()
        }
    }

    pub fn get_sdi_object(&self) -> Option<SdiObject> {
        if !self.sdi_str.is_empty() {
            serde_json::from_str(&self.sdi_str).expect("ERR_SDI_STRING")
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct SdiRecord {
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

impl SdiRecord {
    pub fn new(buffer: Bytes) -> Self {
        let a = buffer.slice(12..18);
        let trx_id_data = [a[0], a[1], a[2], a[3], a[4], a[5], 0u8, 0u8];
        let b = buffer.slice(18..25);
        let roll_ptr_data = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], 0u8];
        Self {
            data_type: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            data_id: u64::from_be_bytes(buffer.as_ref()[4..12].try_into().unwrap()),
            trx_id: u64::from_be_bytes(trx_id_data),
            roll_ptr: u64::from_be_bytes(roll_ptr_data),
            uncomp_len: u32::from_be_bytes(buffer.as_ref()[25..29].try_into().unwrap()),
            comp_len: u32::from_be_bytes(buffer.as_ref()[29..33].try_into().unwrap()),
        }
    }
}
