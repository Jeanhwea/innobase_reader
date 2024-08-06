use crate::ibd::page::{
    SdiPageBody, BasePage, BasePageBody, FilePageHeader, FileSpaceHeaderPageBody, PageTypes, PAGE_SIZE, FIL_HEADER_SIZE,
};
use crate::meta::mgr::MetaDataManager;
use anyhow::{Error, Result};
use bytes::Bytes;
use log::{debug, info};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use crate::meta::cst::coll_find;
use crate::meta::def::{ColumnDef, IndexDef, IndexElementDef, TableDef};
use crate::util;

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

    pub fn read_fil_hdr(&mut self, page_no: usize) -> Result<FilePageHeader> {
        let buf = self.fil_hdr_buffer(page_no)?;
        Ok(FilePageHeader::new(0, buf.clone()))
    }

    pub fn read_page<P>(&mut self, page_no: usize) -> Result<BasePage<P>>
    where
        P: BasePageBody,
    {
        let buf = self.page_buffer(page_no)?;
        Ok(BasePage::new(0, buf.clone()))
    }

    pub fn parse_page<P>(&self, buf: Arc<Bytes>) -> Result<BasePage<P>>
    where
        P: BasePageBody,
    {
        assert_eq!(buf.len(), PAGE_SIZE);
        Ok(BasePage::new(0, buf.clone()))
    }

    pub fn load_sdi_table(&mut self) -> Result<String> {
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = self.read_page(0)?;
        let sdi_meta = fsp_page.page_body.sdi_meta();
        let sdi_page_no = sdi_meta.sdi_page_no as usize;
        let sdi_page: BasePage<SdiPageBody> = self.read_page(sdi_page_no)?;
        sdi_page.page_body.get_table_string()
    }

    pub fn load_table_def(&mut self) -> Result<TableDef> {
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = self.read_page(0)?;
        let sdi_meta = fsp_page.page_body.sdi_meta();
        let sdi_page_no = sdi_meta.sdi_page_no as usize;
        let sdi_page: BasePage<SdiPageBody> = self.read_page(sdi_page_no)?;

        let ddobj = sdi_page.page_body.get_table_sdiobj().dd_object;
        debug!("ddobj={:#?}", &ddobj);

        let coll = coll_find(ddobj.collation_id);
        info!("coll={:#?}", &coll);

        let coldefs = ddobj.columns.iter().map(ColumnDef::from).collect::<Vec<_>>();
        let idxdefs = ddobj
            .indexes
            .iter()
            .map(|idx| {
                let mut ele_defs: Vec<IndexElementDef> = idx
                    .elements
                    .iter()
                    .map(|ele| {
                        let ref_col = &coldefs[ele.column_opx as usize];
                        IndexElementDef::from(ele, ref_col)
                    })
                    .collect();

                let nullinfo = ele_defs.iter().filter(|e| e.isnil).map(|e| e.pos).collect::<Vec<_>>();
                debug!("nullinfo={:?}", nullinfo);

                for (off, pos) in nullinfo.iter().enumerate() {
                    ele_defs[pos - 1].null_offset = off;
                }
                let nil_size = util::align8(nullinfo.len());
                IndexDef::from(idx, ele_defs, nil_size)
            })
            .collect();
        info!("idxdefs={:?}", &idxdefs);

        Ok(TableDef {
            schema_ref: ddobj.schema_ref.clone(),
            tab_name: ddobj.name.clone(),
            collation_id: ddobj.collation_id,
            collation: coll.name.into(),
            charset: coll.charset.into(),
            col_defs: coldefs,
            idx_defs: idxdefs,
        })
    }

    pub fn init_meta_mgr(&mut self) -> Result<MetaDataManager, Error> {
        let page_no = 0;

        let fsp_page: BasePage<FileSpaceHeaderPageBody> = self.read_page(page_no)?;

        assert_eq!(fsp_page.fil_hdr.page_type, PageTypes::FSP_HDR);
        let sdi_meta = fsp_page.page_body.sdi_meta();
        debug!("load sdi_meta = {:?}", &sdi_meta);

        let sdi_page_no = sdi_meta.sdi_page_no as usize;
        debug!("sdi_page_no = {}", sdi_page_no);
        assert_ne!(sdi_page_no, 0);

        let sdi_page: BasePage<SdiPageBody> = self.read_page(sdi_page_no)?;
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

    #[test]
    fn test_read_fsp_hdr_page() -> Result<(), Error> {
        setup();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = fact.read_page(0)?;
        info!("fsp_page={:#?}", fsp_page);
        Ok(())
    }

    #[test]
    fn test_load_table_def() -> Result<(), Error> {
        setup();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;
        let tabdef = fact.load_table_def();
        info!("tabdef={:#?}", tabdef);
        Ok(())
    }
}
