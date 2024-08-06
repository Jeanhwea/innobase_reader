use crate::ibd::page::{BasePage, SdiPageBody};
use crate::meta::def::{ColumnDef, IndexDef, IndexElementDef, TableDef};
use crate::util;
use anyhow::{Error, Result};
use log::{debug, info};
use crate::meta::cst::coll_find;

#[derive(Debug, Default)]
pub struct MetaDataManager {
    pub sdi: Option<BasePage<SdiPageBody>>, // SDI index page
}

impl MetaDataManager {
    pub fn new(sdi_page: BasePage<SdiPageBody>) -> Self {
        Self { sdi: Some(sdi_page) }
    }

    pub fn raw_sdi_str(&self) -> Result<String> {
        self.sdi.as_ref().unwrap().page_body.get_table_string()
    }

    pub fn load_tabdef(&self) -> Result<TableDef, Error> {
        let ddobj = self.sdi.as_ref().unwrap().page_body.get_table_sdiobj().dd_object;
        debug!("ddobj={:#?}", &ddobj);

        let coldefs = ddobj.columns.iter().map(ColumnDef::from).collect::<Vec<_>>();

        let mut idxdefs = Vec::new();
        for idx in &ddobj.indexes {
            let mut ele_defs = Vec::new();
            for ele in &idx.elements {
                let ref_col = &coldefs[ele.column_opx as usize];
                ele_defs.push(IndexElementDef::from(ele, ref_col));
            }

            let mut nullinfo = Vec::new();
            for e in &ele_defs {
                if e.isnil {
                    nullinfo.push(e.pos);
                }
            }

            for (off, pos) in nullinfo.iter().enumerate() {
                ele_defs[pos - 1].null_offset = off;
            }
            let nil_size = util::align8(nullinfo.len());

            idxdefs.push(IndexDef::from(idx, ele_defs, nil_size));
        }
        info!("idxdefs={:?}", &idxdefs);

        let coll = coll_find(ddobj.collation_id);

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
