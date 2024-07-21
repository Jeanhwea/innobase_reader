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
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum PageTypes {
    TYPE_ALLOCATED = 0,            // Freshly allocated page
    TYPE_UNUSED = 1,               // This page type is unused.
    UNDO_LOG = 2,                  // Undo log page
    INODE = 3,                     // Index node
    IBUF_FREE_LIST = 4,            // Insert buffer free list
    IBUF_BITMAP = 5,               // Insert buffer bitmap
    TYPE_SYS = 6,                  // System page
    TYPE_TRX_SYS = 7,              // Transaction system data
    TYPE_FSP_HDR = 8,              // File space header
    TYPE_XDES = 9,                 // Extent descriptor page
    TYPE_BLOB = 10,                // Uncompressed BLOB page
    TYPE_ZBLOB = 11,               // First compressed BLOB page
    TYPE_ZBLOB2 = 12,              // Subsequent compressed BLOB page
    TYPE_UNKNOWN = 13,             // this value when flushing pages.
    COMPRESSED = 14,               // Compressed page
    ENCRYPTED = 15,                // Encrypted page
    COMPRESSED_AND_ENCRYPTED = 16, // Compressed and Encrypted page
    ENCRYPTED_RTREE = 17,          // Encrypted R-tree page
    SDI_BLOB = 18,                 // Uncompressed SDI BLOB page
    SDI_ZBLOB = 19,                // Compressed SDI BLOB page
    TYPE_LEGACY_DBLWR = 20,        // Legacy doublewrite buffer page.
    TYPE_RSEG_ARRAY = 21,          // Rollback Segment Array page
    TYPE_LOB_INDEX = 22,           // Index pages of uncompressed LOB
    TYPE_LOB_DATA = 23,            // Data pages of uncompressed LOB
    TYPE_LOB_FIRST = 24,           // The first page of an uncompressed LOB
    TYPE_ZLOB_FIRST = 25,          // The first page of a compressed LOB
    TYPE_ZLOB_DATA = 26,           // Data pages of compressed LOB
    TYPE_ZLOB_INDEX = 27,          // Index pages of compressed LOB.
    TYPE_ZLOB_FRAG = 28,           // Fragment pages of compressed LOB.
    TYPE_ZLOB_FRAG_ENTRY = 29,     // Index pages of fragment pages (compressed LOB).
    SDI = 17853,                   // Tablespace SDI Index page
    RTREE = 17854,                 // R-tree node
    INDEX = 17855,                 // B-tree node
    Unknown(u16),
}

impl From<u16> for PageTypes {
    fn from(value: u16) -> Self {
        match value {
            0 => PageTypes::TYPE_ALLOCATED,
            1 => PageTypes::TYPE_UNUSED,
            2 => PageTypes::UNDO_LOG,
            3 => PageTypes::INODE,
            4 => PageTypes::IBUF_FREE_LIST,
            5 => PageTypes::IBUF_BITMAP,
            6 => PageTypes::TYPE_SYS,
            7 => PageTypes::TYPE_TRX_SYS,
            8 => PageTypes::TYPE_FSP_HDR,
            9 => PageTypes::TYPE_XDES,
            10 => PageTypes::TYPE_BLOB,
            11 => PageTypes::TYPE_ZBLOB,
            12 => PageTypes::TYPE_ZBLOB2,
            13 => PageTypes::TYPE_UNKNOWN,
            14 => PageTypes::COMPRESSED,
            15 => PageTypes::ENCRYPTED,
            16 => PageTypes::COMPRESSED_AND_ENCRYPTED,
            17 => PageTypes::ENCRYPTED_RTREE,
            18 => PageTypes::SDI_BLOB,
            19 => PageTypes::SDI_ZBLOB,
            20 => PageTypes::TYPE_LEGACY_DBLWR,
            21 => PageTypes::TYPE_RSEG_ARRAY,
            22 => PageTypes::TYPE_LOB_INDEX,
            23 => PageTypes::TYPE_LOB_DATA,
            24 => PageTypes::TYPE_LOB_FIRST,
            25 => PageTypes::TYPE_ZLOB_FIRST,
            26 => PageTypes::TYPE_ZLOB_DATA,
            27 => PageTypes::TYPE_ZLOB_INDEX,
            28 => PageTypes::TYPE_ZLOB_FRAG,
            29 => PageTypes::TYPE_ZLOB_FRAG_ENTRY,
            17853 => PageTypes::SDI,
            17855 => PageTypes::INDEX,
            17854 => PageTypes::RTREE,
            _ => PageTypes::Unknown(value),
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
        f.debug_struct("FileHeader")
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
        f.debug_struct("FileTrailer")
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

/// FSP Header
#[derive(Debug)]
pub struct FileSpaceHeader {
    pub space_id: u32,
}

impl FileSpaceHeader {
    pub fn new<B>(buffer: B) -> FileSpaceHeader
    where
        B: AsRef<[u8]>,
    {
        Self {
            space_id: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
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
    fn new(buffer: Bytes, fil_header: &FilePageHeader) -> Self;
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

pub struct UnknownPage {
    data: Bytes,
}

impl BasePageOperation for UnknownPage {
    fn new(buffer: Bytes, _fil_header: &FilePageHeader) -> Self {
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
    fn new(buffer: Bytes, _fil_header: &FilePageHeader) -> Self {
        let hdr = FileSpaceHeader::new(buffer.slice(..FSP_HEADER_SIZE));
        // todo: parse xdes_ents
        Self {
            fsp_hdr: hdr,
            xdes_list: Vec::new(),
        }
    }
}

// Extent Descriptor Entry
#[derive(Debug)]
pub struct XDesEntry {
    buffer: Bytes,
}

impl XDesEntry {
    pub fn new(buffer: Bytes) -> XDesEntry {
        Self { buffer }
    }
}
