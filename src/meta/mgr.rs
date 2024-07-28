use crate::ibd::page::{BasePage, SdiIndexPage};
use crate::ibd::record::REC_N_FIELDS_ONE_BYTE_MAX;
use crate::meta::def::{ColumnDef, IndexDef, TableDef};
use crate::util;
use anyhow::{Error, Result};
use log::debug;
use crate::meta::cst::get_collation;

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
                    // see function in mysql-server source code
                    // static inline uint8_t rec_get_n_fields_length(ulint n_fields) {
                    //   return (n_fields > REC_N_FIELDS_ONE_BYTE_MAX ? 2 : 1);
                    // }
                    if c.data_len > REC_N_FIELDS_ONE_BYTE_MAX as u32 {
                        2
                    } else {
                        1
                    },
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

        let coll = get_collation(ddobj.collation_id);

        Ok(TableDef {
            schema_ref: ddobj.schema_ref.clone(),
            tab_name: ddobj.name.clone(),
            collation_id: ddobj.collation_id,
            collation: coll.coll_name.clone(),
            charset: coll.charset_name.clone(),
            col_defs: coldefs,
            idx_defs: idxdefs,
            vfld_size: vfld_offset,
            null_size: nullflag_size,
        })
    }
}
