use super::page::{
    BasePage, BasePageOperation, FilePageHeader, FilePageTrailer, FIL_HEADER_SIZE, FIL_TRAILER_SIZE,
};
use crate::ibd::page;
use anyhow::{Error, Result};
use bytes::Bytes;
use log::info;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Debug, Default)]
pub struct DatafileFactory {
    target: PathBuf,    // Target innobase data file (*.idb)
    file: Option<File>, // Tablespace file descriptor
    size: usize,        // File size
}

impl DatafileFactory {
    pub fn new(target: PathBuf) -> Self {
        Self {
            target,
            ..DatafileFactory::default()
        }
    }

    pub fn init(&mut self) -> Result<(), Error> {
        if !self.target.exists() {
            return Err(Error::msg(format!("TargetFileNotFound: {:?}", self.target)));
        }

        self.do_open_file()?;

        Ok(())
    }

    fn do_open_file(&mut self) -> Result<(), Error> {
        let file = File::open(&self.target)?;
        let size = file.metadata().unwrap().len() as usize;

        info!("load {:?}, size = {}", file, size);

        self.file = Some(file);
        self.size = size;

        Ok(())
    }

    fn do_read_bytes(&self, page_no: usize) -> Result<Bytes> {
        let mut f = self.file.as_ref().unwrap();
        f.seek(SeekFrom::Start((page_no * page::PAGE_SIZE) as u64))?;
        let mut buf = vec![0; page::PAGE_SIZE];
        f.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    pub fn page_count(&self) -> usize {
        self.size / page::PAGE_SIZE
    }

    pub fn read_page(&self, page_no: usize) -> Result<Bytes> {
        self.do_read_bytes(page_no)
    }

    pub fn parse_fil_hdr(&self, page_no: usize) -> Result<page::FilePageHeader> {
        let buffer = self.do_read_bytes(page_no)?;
        Ok(PageFactory::new(buffer).fil_hdr())
    }

    pub fn first_fil_hdr(&self) -> Result<page::FilePageHeader> {
        let buffer = self.do_read_bytes(0)?;
        Ok(PageFactory::new(buffer).fil_hdr())
    }
}

#[derive(Debug, Default)]
pub struct PageFactory {
    buffer: Bytes,
    buflen: usize,
    page_no: usize,
}

impl PageFactory {
    pub fn new(buffer: Bytes) -> PageFactory {
        Self {
            buflen: buffer.len(),
            buffer,
            ..PageFactory::default()
        }
    }

    pub fn fil_hdr(&self) -> FilePageHeader {
        FilePageHeader::new(self.buffer.slice(..FIL_HEADER_SIZE))
    }

    pub fn build<P>(&self) -> BasePage<P>
    where
        P: BasePageOperation,
    {
        BasePage::new(
            FilePageHeader::new(self.buffer.slice(..FIL_HEADER_SIZE)),
            self.buffer
                .slice(FIL_HEADER_SIZE..self.buflen - FIL_TRAILER_SIZE),
            FilePageTrailer::new(self.buffer.slice(self.buflen - FIL_TRAILER_SIZE..)),
        )
    }
}
