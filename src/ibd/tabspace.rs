use crate::ibd::page::{BasePage, FileSpaceHeaderPage, PAGE_SIZE};
use anyhow::Result;
use bytes::Bytes;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

#[derive(Debug, Default)]
pub struct Tablespace {
    target: PathBuf,    // target *.idb file
    file: Option<File>, // tablespace file descriptor
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
        self.file = Some(File::open(&self.target)?);
        Ok(())
    }

    pub fn read(&mut self, page_num: usize) -> Result<Bytes> {
        let mut f = self.file.as_ref().unwrap();
        f.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))?;
        let mut buf = vec![0; PAGE_SIZE];
        f.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    pub fn read_fsp_hdr_page(&mut self) -> Result<BasePage<FileSpaceHeaderPage>> {
        let buffer = self.read(0)?;
        Ok(BasePage::new(buffer))
    }
}
