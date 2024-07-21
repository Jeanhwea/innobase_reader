use bytes::Bytes;

pub const PAGE_SIZE: usize = 16 * 1024;

pub const FIL_HEADER_SIZE: usize = 38;
pub const FIL_TRAILER_SIZE: usize = 8;
pub const FSP_HEADER_SIZE: usize = 112;
pub const FSP_TRAILER_SIZE: usize = 8;
pub const XDES_ENTRY_SIZE: usize = 40;

/// FIL Header
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub check_sum: u32,
    pub offset: u32,
}

impl FileHeader {
    pub fn new<B>(buffer: B) -> FileHeader
    where
        B: AsRef<[u8]>,
    {
        Self {
            check_sum: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
            offset: u32::from_be_bytes(buffer.as_ref()[4..8].try_into().unwrap()),
        }
    }
}

/// FIL Trailer
#[derive(Debug)]
pub struct FileTrailer {
    check_sum: u32,
}

impl FileTrailer {
    pub fn new<B>(buffer: B) -> FileTrailer
    where
        B: AsRef<[u8]>,
    {
        Self {
            check_sum: u32::from_be_bytes(buffer.as_ref()[..4].try_into().unwrap()),
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
    pub fil_hdr: FileHeader,
    pub data: P,
    pub fil_trl: FileTrailer,
}

pub trait BasePageOperation {
    fn new(buffer: Bytes, fil_header: &FileHeader) -> Self;
}

impl<P> BasePage<P>
where
    P: BasePageOperation,
{
    pub fn new(header: FileHeader, buffer: Bytes, trailer: FileTrailer) -> BasePage<P> {
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
    fn new(buffer: Bytes, _fil_header: &FileHeader) -> Self {
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
    fn new(buffer: Bytes, _fil_header: &FileHeader) -> Self {
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
