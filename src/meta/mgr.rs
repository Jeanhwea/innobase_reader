use crate::ibd::page::{BasePage, SdiIndexPage};
use crate::meta::def::{ColumnDef, IndexDef, IndexElementDef, TableDef};
use crate::util;
use anyhow::{Error, Result};
use log::debug;
use crate::ibd::record::REC_N_FIELDS_ONE_BYTE_MAX;
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

        let coldefs = ddobj.columns.iter().map(ColumnDef::from).collect::<Vec<_>>();

        let mut idxdefs = Vec::new();
        for idx in &ddobj.indexes {
            let mut ele_defs = Vec::new();
            for ele in &idx.elements {
                let ref_col = &coldefs[ele.column_opx as usize];
                ele_defs.push(IndexElementDef::from(ele, ref_col));
            }

            let mut vfldinfo = Vec::new();
            let mut nullinfo = Vec::new();
            for e in &ele_defs {
                if e.isvar {
                    vfldinfo.push((
                        e.pos,
                        // see function in mysql-server source code
                        // static inline uint8_t rec_get_n_fields_length(ulint n_fields) {
                        //   return (n_fields > REC_N_FIELDS_ONE_BYTE_MAX ? 2 : 1);
                        // }
                        if e.data_len > REC_N_FIELDS_ONE_BYTE_MAX as u32 {
                            2
                        } else {
                            1
                        },
                    ));
                }
                if e.isnil {
                    nullinfo.push(e.pos);
                }
            }

            for (off, pos) in nullinfo.iter().enumerate() {
                ele_defs[pos - 1].null_offset = off;
            }
            let nullflag_size = util::align8(nullinfo.len());

            let mut vfld_offset = 0usize;
            for ent in &vfldinfo {
                ele_defs[ent.0 - 1].vfld_bytes = ent.1;
                vfld_offset += ent.1;
            }

            idxdefs.push(IndexDef::from(idx, ele_defs, vfld_offset, nullflag_size));
        }

        let coll = get_collation(ddobj.collation_id);

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
}
