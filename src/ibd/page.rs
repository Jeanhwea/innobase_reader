use bytes::Bytes;

use std::fmt;
use std::fmt::Formatter;

pub const PAGE_SIZE: usize = 16 * 1024;

pub const FIL_HEADER_SIZE: usize = 38;
pub const FIL_TRAILER_SIZE: usize = 8;
pub const FSP_HEADER_SIZE: usize = 112;
pub const FSP_TRAILER_SIZE: usize = 8;
pub const XDES_ENTRY_SIZE: usize = 40;

#[repr(u16)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
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

/// FIL Header
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
    pub fn new<B>(buffer: B) -> FilePageHeader
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

/// FIL Trailer
pub struct FilePageTrailer {
    check_sum: u32, // Old-style Checksum, FIL_PAGE_END_LSN_OLD_CHKSUM
    lsn: u32,       // Low 32-bits of LSN, last 4 bytes of FIL_PAGE_LSN
}

impl fmt::Debug for FilePageTrailer {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilePageTrailer")
            .field("check_sum", &format!("0x{:08x}", self.check_sum))
            .field("lsn", &format!("0x{:08x} ({})", self.lsn, self.lsn))
            .finish()
    }
}

impl FilePageTrailer {
    pub fn new<B>(buffer: B) -> FilePageTrailer
    where
        B: AsRef<[u8]>,
    {
        Self {
            check_sum: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            lsn: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
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
            len: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
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
            page: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            boffset: u16::from_be_bytes(buffer.as_ref()[4..6].try_into().unwrap()),
        }
    }
}

/// FSP Header, see fsp0fsp.h
pub struct FileSpaceHeader {
    pub space_id: u32,
    pub notused: u32,
    pub fsp_size: u32,
    pub free_limit: u32,
    pub flags: u32,
    pub fsp_frag_n_used: u32,
    pub fsp_free: FlstBaseNode,
    pub free_frag: FlstBaseNode,
    pub full_frag: FlstBaseNode,
    pub segid: u64,
    pub inodes_full: FlstBaseNode,
    pub inodes_free: FlstBaseNode,
}

impl fmt::Debug for FileSpaceHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSpaceHeader")
            .field("space_id", &self.space_id)
            .field("notused", &self.notused)
            .field("fsp_size", &self.fsp_size)
            .field("free_limit", &self.free_limit)
            .field("flags", &format!("0x{:08x} ({})", self.flags, self.flags))
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
    pub fn new<B>(buffer: B) -> FileSpaceHeader
    where
        B: AsRef<[u8]>,
    {
        Self {
            space_id: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            notused: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
            fsp_size: u32::from_be_bytes(buffer.as_ref()[8..12].try_into().unwrap()),
            free_limit: u32::from_be_bytes(buffer.as_ref()[12..16].try_into().unwrap()),
            flags: u32::from_be_bytes(buffer.as_ref()[16..20].try_into().unwrap()),
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

/// FSP Trailer
#[derive(Debug)]
pub struct FileSpaceTrailer<B> {
    buf: B,
}

impl<B> FileSpaceTrailer<B>
where
    B: AsRef<[u8]>,
{
    pub fn new(buffer: B) -> FileSpaceTrailer<B> {
        assert_eq!(buffer.as_ref().len(), FSP_TRAILER_SIZE);
        Self { buf: buffer }
    }
}

// Base Page Structure
#[derive(Debug)]
pub struct BasePage<P> {
    pub fil_hdr: FilePageHeader,
    pub data: P,
    pub fil_trl: FilePageTrailer,
}

pub trait BasePageOperation {
    fn new(buffer: Bytes, fil_hdr: &FilePageHeader) -> Self;
}

impl<P> BasePage<P>
where
    P: BasePageOperation,
{
    pub fn new(header: FilePageHeader, buffer: Bytes, trailer: FilePageTrailer) -> BasePage<P> {
        let p = BasePageOperation::new(buffer, &header);
        Self {
            fil_hdr: header,
            data: p,
            fil_trl: trailer,
        }
    }
}

// UnknowPage
#[derive(Debug)]
pub struct UnknownPage {
    data: Bytes,
}

impl BasePageOperation for UnknownPage {
    fn new(buffer: Bytes, _: &FilePageHeader) -> Self {
        Self { data: buffer }
    }
}

// File Space Header Page
#[derive(Debug)]
pub struct FileSpaceHeaderPage {
    pub fsp_hdr: FileSpaceHeader,
    pub xdes_list: Vec<XDesEntry>,
}

impl BasePageOperation for FileSpaceHeaderPage {
    fn new(buffer: Bytes, _: &FilePageHeader) -> Self {
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
            xdes_list: entries,
        }
    }
}

#[repr(u32)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
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
    pub fn new(buffer: Bytes) -> XDesEntry {
        Self {
            seg_id: u64::from_be_bytes(buffer.as_ref()[..8].try_into().unwrap()),
            flst_node: FlstNode::new(&buffer.as_ref()[8..20]),
            state: u32::from_be_bytes(buffer.as_ref()[20..24].try_into().unwrap()).into(),
            bitmap: buffer.slice(24..40),
        }
    }
}
