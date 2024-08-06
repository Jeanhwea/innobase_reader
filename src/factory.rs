use crate::ibd::page::{
    SdiPageBody, BasePage, BasePageBody, FilePageHeader, FileSpaceHeaderPageBody, PageTypes, PAGE_SIZE, FIL_HEADER_SIZE,
};
use crate::meta::mgr::MetaDataManager;
use anyhow::{Error, Result};
use bytes::Bytes;
use log::debug;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

pub const SDI_META_INFO_MIN_VER: u32 = 80000;

#[derive(Debug)]
pub struct DatafileFactory {
    pub target: PathBuf, // Target datafile
    pub file: File,      // Tablespace file descriptor
    pub size: usize,     // File size
}

impl DatafileFactory {
    pub fn from_file(target: PathBuf) -> Result<Self> {
        if !target.exists() {
            return Err(Error::msg(format!("没有找到目标文件: {:?}", target)));
        }

        let file = File::open(&target)?;

        Ok(Self {
            target,
            size: file.metadata().unwrap().len() as usize,
            file,
        })
    }

    pub fn page_count(&self) -> usize {
        self.size / PAGE_SIZE
    }

    pub fn page_buffer(&mut self, page_no: usize) -> Result<Arc<Bytes>> {
        self.file.seek(SeekFrom::Start((page_no * PAGE_SIZE) as u64))?;
        let mut buffer = vec![0; PAGE_SIZE];
        self.file.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    pub fn fil_hdr_buffer(&mut self, page_no: usize) -> Result<Arc<Bytes>> {
        self.file.seek(SeekFrom::Start((page_no * PAGE_SIZE) as u64))?;
        let mut buffer = vec![0; FIL_HEADER_SIZE];
        self.file.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    pub fn parse_fil_hdr(&mut self, page_no: usize) -> Result<FilePageHeader> {
        let buf = self.fil_hdr_buffer(page_no)?;
        Ok(FilePageHeader::new(0, buf.clone()))
    }

    pub fn parse_page<P>(&self, buf: Arc<Bytes>) -> Result<BasePage<P>>
    where
        P: BasePageBody,
    {
        assert_eq!(buf.len(), PAGE_SIZE);
        Ok(BasePage::new(0, buf.clone()))
    }

    pub fn init_meta_mgr(&mut self) -> Result<MetaDataManager, Error> {
        let page_no = 0;

        let buf0 = self.page_buffer(page_no)?;
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = self.parse_page(buf0)?;

        assert_eq!(fsp_page.fil_hdr.page_type, PageTypes::FSP_HDR);
        let sdi_meta = fsp_page.page_body.sdi_meta();
        debug!("load sdi_meta = {:?}", &sdi_meta);

        let sdi_page_no = sdi_meta.sdi_page_no as usize;
        debug!("sdi_page_no = {}", sdi_page_no);
        assert_ne!(sdi_page_no, 0);

        let buf = self.page_buffer(sdi_page_no)?;
        let sdi_page: BasePage<SdiPageBody> = self.parse_page(buf)?;
        assert_eq!(sdi_page.fil_hdr.page_type, PageTypes::SDI);
        debug!("load sdi_page = {:?}", &sdi_page);

        Ok(MetaDataManager::new(sdi_page))
    }
}

#[cfg(test)]
mod factory_tests {

    use crate::util;

    use std::env::set_var;
    use std::path::PathBuf;
    use anyhow::Error;
    use log::info;
    use crate::factory::DatafileFactory;
    use crate::ibd::page::{BasePage, FileSpaceHeaderPageBody, PageTypes};

    const IBD_FILE: &str = "data/departments.ibd";

    fn setup() {
        set_var("RUST_LOG", "info");
        util::init();
    }

    #[test]
    fn test_load_buffer() -> Result<(), Error> {
        setup();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;
        let buf = fact.fil_hdr_buffer(0)?;
        assert!(buf.len() > 0);
        info!("{:?}", buf);
        Ok(())
    }

    #[test]
    fn test_load_sdi_meta() -> Result<(), Error> {
        setup();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;

        let buf0 = fact.page_buffer(0)?;
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = fact.parse_page(buf0)?;

        assert_eq!(fsp_page.fil_hdr.page_type, PageTypes::FSP_HDR);
        let sdi_meta = fsp_page.page_body.sdi_meta();
        info!("sdi_meta={:?}", sdi_meta);

        assert_eq!(sdi_meta.sdi_page_no, 3);
        Ok(())
    }
}
