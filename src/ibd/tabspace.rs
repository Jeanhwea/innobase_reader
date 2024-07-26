use anyhow::{Error, Result};

use super::{
    page::{BasePage, SdiIndexPage},
    record::{ColumnKeys, ColumnTypes},
};
use crate::ibd::record::HiddenTypes;

#[derive(Debug, Default)]
pub struct MetaDataManager {
    pub sdi: Option<BasePage<SdiIndexPage>>, // SDI index page
}

impl MetaDataManager {
    pub fn new(sdi_page: BasePage<SdiIndexPage>) -> Self {
        Self {
            sdi: Some(sdi_page),
        }
    }

    pub fn load_tabdef(&self) -> Result<TableDef, Error> {
        Ok(TableDef::default())
    }
}

#[derive(Debug, Default, Clone)]
pub struct TableDef {
    pub tab_name: String,         // table name
    pub varfield_size: usize,     // variadic field size
    pub nullflag_size: usize,     // nullable flag size
    pub col_defs: Vec<ColumnDef>, // column infomation
}

#[derive(Debug, Default, Clone)]
pub struct ColumnDef {
    pub ord_pos: u32,         // ordinal position
    pub col_name: String,     // column name
    pub data_len: u32,        // data lenght in bytes
    pub is_nullable: bool,    // is nullable
    pub is_varfield: bool,    // is variadic field
    pub dd_type: ColumnTypes, // data dictionary type
    pub hidden: HiddenTypes,  // hidden type
    pub col_key: ColumnKeys,  // column key type
    pub utf8_def: String,     // utf8 column definition
    pub comment: String,      // Comment
    pub null_offset: usize,   // nullable offset
    pub vfld_offset: usize,   // variadic field offset
    pub vfld_bytes: usize,    // variadic field size
}
