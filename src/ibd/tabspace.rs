use crate::ibd::factory::PageFactory;

use crate::ibd::page;
use anyhow::Result;
use bytes::Bytes;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Debug, Default)]
pub struct Tablespace {
    target: PathBuf,    // Target innobase data file (*.idb)
    size: usize,        // File size
    file: Option<File>, // Tablespace file descriptor
}

impl Tablespace {
    pub fn new(target: PathBuf) -> Self {
        Self {
            target,
            ..Tablespace::default()
        }
    }

    pub fn open(&mut self) -> Result<()> {
        if !self.target.exists() {
            panic!("Tablespace target not exists");
        }
        let f = File::open(&self.target)?;
        self.size = f.metadata().unwrap().len() as usize;
        self.file = Some(f);
        Ok(())
    }

    pub fn page_count(&self) -> usize {
        self.size / page::PAGE_SIZE
    }

    pub fn do_read_bytes(&self, page_no: usize) -> Result<Bytes> {
        let mut f = self.file.as_ref().unwrap();
        f.seek(SeekFrom::Start((page_no * page::PAGE_SIZE) as u64))?;
        let mut buf = vec![0; page::PAGE_SIZE];
        f.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    pub fn parse_fil_hdr(&self, page_no: usize) -> Result<page::FilePageHeader> {
        let buffer = self.do_read_bytes(page_no)?;
        let buflen = buffer.len();
        Ok(PageFactory::new(buffer, buflen).fil_hdr())
    }

    pub fn init_page_factory(&self, page_no: usize) -> Result<PageFactory> {
        let buffer = self.do_read_bytes(page_no)?;
        let buflen = buffer.len();
        Ok(PageFactory::new(buffer, buflen))
    }
}