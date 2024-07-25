use crate::ibd::page::SdiIndexPage;
use crate::util;

use super::page::{
    BasePage, BasePageOperation, FilePageHeader, FilePageTrailer, FileSpaceHeaderPage, PageTypes,
    FIL_HEADER_SIZE, FIL_TRAILER_SIZE, PAGE_SIZE,
};
use super::record::{ColumnTypes, HiddenTypes};
use super::tabspace::{ColumnDef, Datafile, TableDef};
use anyhow::{Error, Result};
use bytes::Bytes;
use log::{debug, info};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

pub const SDI_META_INFO_MIN_VER: u32 = 80000;

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

    pub fn parse<P>(&self) -> BasePage<P>
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

#[derive(Debug, Default)]
pub struct DatafileFactory {
    target: PathBuf,                                // Target innobase data file (*.idb)
    file: Option<File>,                             // Tablespace file descriptor
    filesize: usize,                                // File size
    datafile: Option<Datafile>,                     // Datafile Information
    fsppage: Option<BasePage<FileSpaceHeaderPage>>, // first FSP_HDR page
    sdipage: Option<BasePage<SdiIndexPage>>,        // SDI
    tabdef: Option<TableDef>,                       // Table Definition
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

        let hdr0 = self.first_fil_hdr()?;
        self.datafile = Some(Datafile::new(hdr0));

        Ok(())
    }

    fn do_open_file(&mut self) -> Result<(), Error> {
        let file = File::open(&self.target)?;
        let size = file.metadata().unwrap().len() as usize;

        info!("load {:?}, size = {}", file, size);

        self.file = Some(file);
        self.filesize = size;

        Ok(())
    }

    fn do_read_bytes(&self, page_no: usize) -> Result<Bytes> {
        let mut f = self.file.as_ref().unwrap();
        f.seek(SeekFrom::Start((page_no * PAGE_SIZE) as u64))?;
        let mut buf = vec![0; PAGE_SIZE];
        f.read_exact(&mut buf)?;
        Ok(Bytes::from(buf))
    }

    fn do_load_fsp_page(&mut self) -> Result<(), Error> {
        if self.filesize < PAGE_SIZE {
            return Err(Error::msg("datafile size less than one page"));
        }

        let buffer = self.do_read_bytes(0)?;
        let mut fsp_page: BasePage<FileSpaceHeaderPage> = PageFactory::new(buffer).parse();
        assert_eq!(fsp_page.fil_hdr.page_type, PageTypes::FSP_HDR);

        fsp_page.page_body.parse_sdi_meta();

        self.fsppage = Some(fsp_page);

        Ok(())
    }

    fn do_load_sdi_page(&mut self) -> Result<(), Error> {
        if let Some(ref sdi_info) = self
            .fsppage
            .as_ref()
            .expect("ERR_NO_FIRST_FSP_PAGE")
            .page_body
            .sdi_info
        {
            if sdi_info.sdi_page_no < 1 {
                return Ok(());
            }
            let buffer = self.do_read_bytes(sdi_info.sdi_page_no as usize)?;
            let sdi_page: BasePage<SdiIndexPage> = PageFactory::new(buffer).parse();
            assert_eq!(sdi_page.fil_hdr.page_type, PageTypes::SDI);
            self.sdipage = Some(sdi_page);
        }

        Ok(())
    }

    fn do_load_table_def(&mut self) -> Result<(), Error> {
        if let Some(sdipage) = &self.sdipage {
            let tabobj = sdipage.page_body.get_sdi_object().unwrap().dd_object;
            let mut coldefs = tabobj
                .columns
                .iter()
                .map(|e| ColumnDef {
                    ord_pos: e.ordinal_position,
                    col_name: e.col_name.clone(),
                    data_len: match e.hidden {
                        HiddenTypes::HT_HIDDEN_SE => e.char_length,
                        HiddenTypes::HT_VISIBLE => match e.dd_type {
                            ColumnTypes::TINY => 1,
                            ColumnTypes::SHORT => 2,
                            ColumnTypes::LONG => 4,
                            ColumnTypes::VARCHAR
                            | ColumnTypes::VAR_STRING
                            | ColumnTypes::STRING => e.char_length,
                            _ => todo!(
                                "Unsupported ColumType::{}, utf8_def={}",
                                e.dd_type,
                                e.column_type_utf8
                            ),
                        },
                        _ => todo!("Unsupported HiddenTypes::{}", e.hidden),
                    },
                    is_nullable: e.is_nullable,
                    is_varfield: matches!(
                        e.dd_type,
                        ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING
                    ),
                    dd_type: e.dd_type.clone(),
                    comment: e.comment.clone(),
                    hidden: e.hidden.clone(),
                    utf8_def: e.column_type_utf8.clone(),
                    null_offset: 0,
                    vfld_offset: 0,
                    vfld_bytes: 0,
                })
                .collect::<Vec<_>>();

            let mut vfldinfo = Vec::new();
            let mut nullinfo = Vec::new();
            for c in &coldefs {
                if c.is_varfield {
                    vfldinfo.push((
                        c.ord_pos as usize,
                        c.col_name.clone(),
                        // 字符数大于 255 , 使用 2 个字节存储; 否则用 1 个字节
                        if c.data_len > 255 { 2 } else { 1 },
                    ));
                }
                if c.is_nullable {
                    nullinfo.push((c.ord_pos as usize, c.col_name.clone()));
                }
            }
            debug!("varginfo = {:?}, nullinfo = {:?}", vfldinfo, nullinfo);

            for (off, ent) in nullinfo.iter().enumerate() {
                coldefs[ent.0 - 1].null_offset = off;
            }
            let nullflag_size = util::align8(nullinfo.len());

            let mut vfld_offset = nullflag_size;
            for ent in &vfldinfo {
                coldefs[ent.0 - 1].vfld_offset = vfld_offset;
                coldefs[ent.0 - 1].vfld_bytes = ent.2;
                vfld_offset += ent.2;
            }

            self.tabdef = Some(TableDef {
                tab_name: tabobj.name.clone(),
                varfield_size: vfld_offset,
                nullflag_size,
                col_defs: coldefs,
            });
        }
        Ok(())
    }

    pub fn load_tabdef(&mut self) -> Result<&TableDef, Error> {
        self.do_load_fsp_page()?;
        self.do_load_sdi_page()?;
        self.do_load_table_def()?;
        Ok(self.tabdef.as_ref().expect("ERR_LOAD_TABLE_DEFINITION"))
    }

    pub fn page_count(&self) -> usize {
        self.filesize / PAGE_SIZE
    }

    pub fn file_size(&self) -> usize {
        self.filesize
    }

    pub fn read_page(&self, page_no: usize) -> Result<Bytes> {
        self.do_read_bytes(page_no)
    }

    pub fn parse_fil_hdr(&self, page_no: usize) -> Result<FilePageHeader> {
        let buffer = self.do_read_bytes(page_no)?;
        Ok(PageFactory::new(buffer).fil_hdr())
    }

    pub fn first_fil_hdr(&self) -> Result<FilePageHeader> {
        let buffer = self.do_read_bytes(0)?;
        Ok(PageFactory::new(buffer).fil_hdr())
    }

    pub fn datafile(&self) -> Datafile {
        self.datafile.clone().unwrap()
    }
}

#[cfg(test)]
mod factory_tests {
    use super::*;
    use crate::util;
    use log::info;
    use std::{env::set_var, path::PathBuf};

    const IBD_FILE: &str = "data/departments.ibd";

    fn setup() {
        set_var("RUST_LOG", "info");
        util::init();
    }

    #[test]
    fn parse_table_definition() {
        setup();
        let mut factory = DatafileFactory::new(PathBuf::from(IBD_FILE));
        assert!(factory.init().is_ok());
        assert!(factory.do_load_fsp_page().is_ok());
        assert!(factory.do_load_sdi_page().is_ok());
        assert!(factory.do_load_table_def().is_ok());
        info!("factory = {:#?}", factory);
    }

    #[test]
    fn load_table_definition() {
        setup();
        let mut factory = DatafileFactory::new(PathBuf::from(IBD_FILE));
        assert!(factory.init().is_ok());
        assert!(factory.load_tabdef().is_ok());
        info!("tabdef = {:#?}", factory.tabdef);
    }
}
