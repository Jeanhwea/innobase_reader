use crate::ibd::page::{BasePage, SdiIndexPage};
use crate::meta::def::{ColumnDef, IndexDef, TableDef};
use crate::util;
use anyhow::{Error, Result};
use log::debug;

#[derive(Debug, Default)]
pub struct MetaDataManager {
    pub sdi: Option<BasePage<SdiIndexPage>>, // SDI index page
}

impl MetaDataManager {
    pub fn new(sdi_page: BasePage<SdiIndexPage>) -> Self {
        Self { sdi: Some(sdi_page) }
    }

    pub fn raw_sdi_str(&self) -> Option<String> {
        self.sdi.as_ref().map(|pg| pg.page_body.uncomp_data.clone())
    }

    pub fn load_tabdef(&self) -> Result<TableDef, Error> {
        let ddobj = self.sdi.as_ref().unwrap().page_body.get_sdi_object().dd_object;
        debug!("ddobj = {:#?}", &ddobj);

        let mut coldefs = ddobj.columns.iter().map(ColumnDef::from).collect::<Vec<_>>();
        let idxdefs = ddobj.indexes.iter().map(IndexDef::from).collect::<Vec<_>>();

        let mut vfldinfo = Vec::new();
        let mut nullinfo = Vec::new();
        for c in &coldefs {
            if c.isvar {
                vfldinfo.push((
                    c.pos,
                    // 字符数大于 255 , 使用 2 个字节存储; 否则用 1 个字节
                    if c.data_len > 255 { 2 } else { 1 },
                ));
            }
            if c.isnil {
                nullinfo.push(c.pos);
            }
        }
        debug!("varginfo = {:?}, nullinfo = {:?}", vfldinfo, nullinfo);

        for (off, ord) in nullinfo.iter().enumerate() {
            coldefs[ord - 1].null_offset = off;
        }
        let nullflag_size = util::align8(nullinfo.len());

        let mut vfld_offset = 0usize;
        for ent in &vfldinfo {
            coldefs[ent.0 - 1].vfld_offset = vfld_offset;
            coldefs[ent.0 - 1].vfld_bytes = ent.1;
            vfld_offset += ent.1;
        }

        Ok(TableDef {
            schema_ref: ddobj.schema_ref.clone(),
            tab_name: ddobj.name.clone(),
            collation_id: ddobj.collation_id,
            col_defs: coldefs,
            idx_defs: idxdefs,
            vfld_size: vfld_offset,
            null_size: nullflag_size,
        })
    }
}
