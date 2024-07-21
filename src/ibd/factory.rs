use super::page::{
    BasePage, BasePageOperation, FilePageHeader, FilePageTrailer, FIL_HEADER_SIZE, FIL_TRAILER_SIZE,
};
use bytes::Bytes;

#[derive(Debug)]
pub struct PageFactory {
    buf: Bytes,
    len: usize,
}

impl PageFactory {
    pub fn new(buffer: Bytes, length: usize) -> PageFactory {
        Self {
            buf: buffer,
            len: length,
        }
    }

    pub fn fil_hdr(&self) -> FilePageHeader {
        FilePageHeader::new(self.buf.slice(..FIL_HEADER_SIZE))
    }

    pub fn build<P>(&self) -> BasePage<P>
    where
        P: BasePageOperation,
    {
        BasePage::new(
            FilePageHeader::new(self.buf.slice(..FIL_HEADER_SIZE)),
            self.buf.slice(FIL_HEADER_SIZE..self.len - FIL_TRAILER_SIZE),
            FilePageTrailer::new(self.buf.slice(self.len - FIL_TRAILER_SIZE..)),
        )
    }
}
