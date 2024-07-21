use super::page::{
    BasePage, BasePageOperation, FileHeader, FileTrailer, UnknownPage, FIL_HEADER_SIZE,
    FIL_TRAILER_SIZE,
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

    pub fn build<P>(&self) -> BasePage<P>
    where
        P: BasePageOperation,
    {
        BasePage::new(
            FileHeader::new(self.buf.slice(..FIL_HEADER_SIZE)),
            self.buf.slice(FIL_HEADER_SIZE..self.len - FIL_TRAILER_SIZE),
            FileTrailer::new(self.buf.slice(self.len - FIL_TRAILER_SIZE..)),
        )
    }
}
