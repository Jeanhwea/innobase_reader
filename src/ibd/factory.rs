use super::page::{
    BasePage, BasePageOperation, FilePageHeader, FilePageTrailer, FileSpaceHeaderPage, PageTypes,
    FIL_HEADER_SIZE, FIL_TRAILER_SIZE, PAGE_SIZE,
};
use super::tabspace::MetaDataManager;
use crate::ibd::page::SdiIndexPage;
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
    pub target: PathBuf,     // Target datafile
    pub file: Option<File>,  // Tablespace file descriptor
    pub filesize: usize,     // File size
    pub server_version: u32, // on page 0, FIL_PAGE_SRV_VERSION
    pub space_version: u32,  // on page 0, FIL_PAGE_SPACE_VERSION
    pub space_id: u32,       // Space Id
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
        debug!("hdr0 = {:?}", hdr0);

        self.server_version = hdr0.prev_page;
        self.space_version = hdr0.next_page;
        self.space_id = hdr0.space_id;

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

    pub fn init_meta_mgr(&self) -> Result<MetaDataManager, Error> {
        let buffer = self.do_read_bytes(0)?;
        let mut fsp_page: BasePage<FileSpaceHeaderPage> = PageFactory::new(buffer).parse();
        assert_eq!(fsp_page.fil_hdr.page_type, PageTypes::FSP_HDR);
        debug!("load fsg_page = {:?}", &fsp_page);

        fsp_page.page_body.parse_sdi_meta();
        let sdi_meta_data = fsp_page.page_body.sdi_meta_data.unwrap();

        let sdi_page_no = sdi_meta_data.sdi_page_no as usize;
        assert_ne!(sdi_page_no, 0);
        info!("sdi_page_no = {}", sdi_page_no);

        let buffer = self.do_read_bytes(sdi_page_no)?;
        let sdi_page: BasePage<SdiIndexPage> = PageFactory::new(buffer).parse();
        assert_eq!(sdi_page.fil_hdr.page_type, PageTypes::SDI);
        debug!("load sdi_page = {:?}", &sdi_page);

        Ok(MetaDataManager::new(sdi_page))
    }

    // fn do_load_table_def(&mut self) -> Result<(), Error> {
    //     if let Some(sdipage) = &self.sdipage {
    //         let tabobj = sdipage.page_body.get_sdi_object().unwrap().dd_object;
    //         let mut coldefs = tabobj
    //             .columns
    //             .iter()
    //             .map(|e| ColumnDef {
    //                 ord_pos: e.ordinal_position,
    //                 col_name: e.col_name.clone(),
    //                 col_key: e.column_key.clone(),
    //                 data_len: match e.hidden {
    //                     HiddenTypes::HT_HIDDEN_SE => e.char_length,
    //                     HiddenTypes::HT_VISIBLE => match e.dd_type {
    //                         ColumnTypes::TINY => 1,
    //                         ColumnTypes::SHORT => 2,
    //                         ColumnTypes::LONG => 4,
    //                         ColumnTypes::VARCHAR
    //                         | ColumnTypes::VAR_STRING
    //                         | ColumnTypes::STRING => e.char_length,
    //                         ColumnTypes::NEWDATE => 3,
    //                         ColumnTypes::ENUM => e.char_length,
    //                         _ => todo!(
    //                             "Unsupported data_len type: ColumType::{}, utf8_def={}",
    //                             e.dd_type,
    //                             e.column_type_utf8
    //                         ),
    //                     },
    //                     _ => todo!("Unsupported data_len type: HiddenTypes::{}", e.hidden),
    //                 },
    //                 is_nullable: e.is_nullable,
    //                 is_varfield: match &e.dd_type {
    //                     ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING => {
    //                         true
    //                     }
    //                     _ => e.ordinal_position == 1 && e.column_key == ColumnKeys::CK_PRIMARY,
    //                 },
    //                 dd_type: e.dd_type.clone(),
    //                 comment: e.comment.clone(),
    //                 hidden: e.hidden.clone(),
    //                 utf8_def: e.column_type_utf8.clone(),
    //                 null_offset: 0,
    //                 vfld_offset: 0,
    //                 vfld_bytes: 0,
    //             })
    //             .collect::<Vec<_>>();

    //         let mut vfldinfo = Vec::new();
    //         let mut nullinfo = Vec::new();
    //         for c in &coldefs {
    //             if c.is_varfield {
    //                 vfldinfo.push((
    //                     c.ord_pos as usize,
    //                     // 字符数大于 255 , 使用 2 个字节存储; 否则用 1 个字节
    //                     if c.data_len > 255 { 2 } else { 1 },
    //                 ));
    //             }
    //             if c.is_nullable {
    //                 nullinfo.push(c.ord_pos as usize);
    //             }
    //         }
    //         debug!("varginfo = {:?}, nullinfo = {:?}", vfldinfo, nullinfo);

    //         for (off, ord) in nullinfo.iter().enumerate() {
    //             coldefs[ord - 1].null_offset = off;
    //         }
    //         let nullflag_size = util::align8(nullinfo.len());

    //         let mut vfld_offset = nullflag_size;
    //         for ent in &vfldinfo {
    //             coldefs[ent.0 - 1].vfld_offset = vfld_offset;
    //             coldefs[ent.0 - 1].vfld_bytes = ent.1;
    //             vfld_offset += ent.1;
    //         }

    //         self.tabdef = Some(TableDef {
    //             tab_name: tabobj.name.clone(),
    //             varfield_size: vfld_offset,
    //             nullflag_size,
    //             col_defs: coldefs,
    //         });
    //     }
    //     Ok(())
    // }

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
}

#[cfg(test)]
mod factory_tests {

    use crate::util;

    use std::env::set_var;

    const IBD_FILE: &str = "data/departments.ibd";

    fn setup() {
        set_var("RUST_LOG", "info");
        util::init();
    }

    #[test]
    fn load_table_definition() {
        setup();
    }
}
