use bytes::Bytes;

pub const PAGE_SIZE: usize = 16 * 1024;

const FIL_HEADER_SIZE: usize = 38;
const FIL_TRAILER_SIZE: usize = 8;
const FSP_HEADER_SIZE: usize = 112;
const FSP_TRAILER_SIZE: usize = 8;
const XDES_ENTRY_SIZE: usize = 40;

/// FIL Header
#[derive(Debug)]
pub struct FileHeader<B> {
    buffer: B,
}

impl<B> FileHeader<B>
where
    B: AsRef<[u8]>,
{
    pub fn new(buffer: B) -> FileHeader<B> {
        assert_eq!(buffer.as_ref().len(), FIL_HEADER_SIZE);
        Self { buffer }
    }

    pub fn check_sum(&self) -> u32 {
        let data: [u8; 4] = self.buffer.as_ref()[..4].try_into().unwrap();
        u32::from_be_bytes(data)
    }

    pub fn offset(&self) -> u32 {
        let data: [u8; 4] = self.buffer.as_ref()[4..8].try_into().unwrap();
        u32::from_be_bytes(data)
    }
}

/// FIL Trailer
#[derive(Debug)]
pub struct FileTrailer<B> {
    buffer: B,
}

impl<B> FileTrailer<B>
where
    B: AsRef<[u8]>,
{
    pub fn new(buffer: B) -> FileTrailer<B> {
        assert_eq!(buffer.as_ref().len(), FIL_TRAILER_SIZE);
        Self { buffer }
    }
}

/// FSP Header
#[derive(Debug)]
pub struct FileSpaceHeader<B> {
    buffer: B,
}

impl<B> FileSpaceHeader<B>
where
    B: AsRef<[u8]>,
{
    pub fn new(buffer: B) -> FileSpaceHeader<B> {
        assert_eq!(buffer.as_ref().len(), FSP_HEADER_SIZE);
        Self { buffer }
    }

    pub fn space_id(&self) -> u32 {
        let data: [u8; 4] = self.buffer.as_ref()[..4].try_into().unwrap();
        u32::from_be_bytes(data)
    }
}

/// FSP Trailer
#[derive(Debug)]
pub struct FileSpaceTrailer<B> {
    buffer: B,
}

impl<B> FileSpaceTrailer<B>
where
    B: AsRef<[u8]>,
{
    pub fn new(buffer: B) -> FileSpaceTrailer<B> {
        assert_eq!(buffer.as_ref().len(), FSP_TRAILER_SIZE);
        Self { buffer }
    }
}

// Base Page Structure
#[derive(Debug)]
pub struct BasePage<P> {
    pub buffer: Bytes,
    pub fil_hdr: FileHeader<Bytes>,
    pub page: P,
    pub fil_trl: FileTrailer<Bytes>,
}

pub trait BasePageOperation {
    fn new(buffer: Bytes, fil_header: &FileHeader<Bytes>) -> Self;
}

impl<P> BasePage<P>
where
    P: BasePageOperation,
{
    pub fn new(buffer: Bytes) -> BasePage<P> {
        let len = buffer.len();
        let hdr = FileHeader::new(buffer.slice(..FIL_HEADER_SIZE));
        let page =
            BasePageOperation::new(buffer.slice(FIL_HEADER_SIZE..len - FIL_TRAILER_SIZE), &hdr);
        let trl = FileTrailer::new(buffer.slice(len - FIL_TRAILER_SIZE..));
        Self {
            buffer: buffer,
            fil_hdr: hdr,
            page: page,
            fil_trl: trl,
        }
    }
}

// Extent Descriptor Entry
#[derive(Debug)]
pub struct XDesEntry<B> {
    buffer: B,
}

impl<B> XDesEntry<B>
where
    B: AsRef<[u8]>,
{
    pub fn new(buffer: B) -> XDesEntry<B> {
        assert_eq!(buffer.as_ref().len(), XDES_ENTRY_SIZE);
        Self { buffer }
    }
}

// File Space Header Page
#[derive(Debug)]
pub struct FspHdrPage {
    pub fsp_hdr: FileSpaceHeader<Bytes>,
    pub xdes_ents: Vec<XDesEntry<Bytes>>,
}

impl BasePageOperation for FspHdrPage {
    fn new(buffer: Bytes, _fil_header: &FileHeader<Bytes>) -> Self {
        let hdr = FileSpaceHeader::new(buffer.slice(..FSP_HEADER_SIZE));
        // todo: parse xdes_ents
        Self {
            fsp_hdr: hdr,
            xdes_ents: Vec::new(),
        }
    }
}
