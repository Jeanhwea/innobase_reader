use bytes::Bytes;
use std::fmt;
use std::fmt::Formatter;

pub const PAGE_SIZE: usize = 16 * 1024;

pub const FIL_HEADER_SIZE: usize = 38;
pub const FIL_TRAILER_SIZE: usize = 8;
pub const FSP_HEADER_SIZE: usize = 112;
pub const FSP_TRAILER_SIZE: usize = 8;
pub const XDES_ENTRY_SIZE: usize = 40;

/// FIL Header
#[derive(Clone)]
pub struct FilePageHeader {
    pub check_sum: u32,   // check_sum, FIL_PAGE_SPACE_OR_CHKSUM
    pub page_number: u32, // page_number/offset, FIL_PAGE_OFFSET
    pub prev_page: u32,   // Previous Page, FIL_PAGE_PREV
    pub next_page: u32,   // Next Page, FIL_PAGE_NEXT
    pub lsn: u64,         // LSN for last page modification, FIL_PAGE_LSN
    pub page_type: u16,   // Page Type, FIL_PAGE_TYPE
    pub flush_lsn: u64,   // Flush LSN, FIL_PAGE_FILE_FLUSH_LSN
    pub space_id: u32,    // Space ID, FIL_PAGE_SPACE_ID
}

impl fmt::Debug for FilePageHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHeader")
            .field("check_sum", &format!("0x{:08x}", self.check_sum))
            .field("page_number", &self.page_number)
            .field("prev_page", &format!("0x{:08x}", self.prev_page))
            .field("next_page", &format!("0x{:08x}", self.next_page))
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
            page_number: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
            prev_page: u32::from_be_bytes(buffer.as_ref()[8..12].try_into().unwrap()),
            next_page: u32::from_be_bytes(buffer.as_ref()[12..16].try_into().unwrap()),
            lsn: u64::from_be_bytes(buffer.as_ref()[16..24].try_into().unwrap()),
            page_type: u16::from_be_bytes(buffer.as_ref()[24..26].try_into().unwrap()),
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
