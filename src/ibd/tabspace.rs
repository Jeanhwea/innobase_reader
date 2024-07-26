use anyhow::{Error, Result};

use super::{
    page::{BasePage, SdiIndexPage},
    record::{ColumnKeys, ColumnTypes, DataDictColumn},
};
use crate::ibd::record::HiddenTypes;
use crate::util;
use log::{debug, info};

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

    pub fn raw_sdi_str(&self) -> Option<String> {
        match &self.sdi {
            Some(pg) => Some(pg.page_body.uncomp_data.clone()),
            None => None,
        }
    }

    pub fn load_tabdef(&self) -> Result<TableDef, Error> {
        let ddobj = self
            .sdi
            .as_ref()
            .unwrap()
            .page_body
            .get_sdi_object()
            .dd_object;
        info!("ddobj = {:?}", &ddobj);

        let mut coldefs = ddobj
            .columns
            .iter()
            .map(ColumnDef::from)
            .collect::<Vec<_>>();

        let mut vfldinfo = Vec::new();
        let mut nullinfo = Vec::new();
        for c in &coldefs {
            if c.is_varfield {
                vfldinfo.push((
                    c.ord_pos as usize,
                    // 字符数大于 255 , 使用 2 个字节存储; 否则用 1 个字节
                    if c.data_len > 255 { 2 } else { 1 },
                ));
            }
            if c.is_nullable {
                nullinfo.push(c.ord_pos as usize);
            }
        }
        debug!("varginfo = {:?}, nullinfo = {:?}", vfldinfo, nullinfo);

        for (off, ord) in nullinfo.iter().enumerate() {
            coldefs[ord - 1].null_offset = off;
        }
        let nullflag_size = util::align8(nullinfo.len());

        let mut vfld_offset = nullflag_size;
        for ent in &vfldinfo {
            coldefs[ent.0 - 1].vfld_offset = vfld_offset;
            coldefs[ent.0 - 1].vfld_bytes = ent.1;
            vfld_offset += ent.1;
        }

        Ok(TableDef {
            tab_name: ddobj.name.clone(),
            vfld_size: vfld_offset - nullflag_size,
            null_size: nullflag_size,
            col_defs: coldefs,
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct TableDef {
    pub tab_name: String,         // table name
    pub vfld_size: usize,         // variadic field size
    pub null_size: usize,         // nullable flag size
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

impl ColumnDef {
    pub fn from(ddc: &DataDictColumn) -> Self {
        Self {
            ord_pos: ddc.ordinal_position,
            col_name: ddc.col_name.clone(),
            col_key: ddc.column_key.clone(),
            data_len: match ddc.hidden {
                HiddenTypes::HT_HIDDEN_SE => ddc.char_length,
                HiddenTypes::HT_VISIBLE => match ddc.dd_type {
                    ColumnTypes::VAR_STRING | ColumnTypes::STRING | ColumnTypes::DECIMAL => {
                        ddc.char_length
                    }
                    ColumnTypes::VARCHAR => {
                        ddc.char_length + (if ddc.char_length < 256 { 1 } else { 2 })
                    }
                    ColumnTypes::YEAR | ColumnTypes::TINY => 1,
                    ColumnTypes::SHORT => 2,
                    ColumnTypes::INT24 | ColumnTypes::NEWDATE | ColumnTypes::TIME => 3,
                    ColumnTypes::LONG => 4,
                    ColumnTypes::LONGLONG => 8,
                    ColumnTypes::DATE | ColumnTypes::TIMESTAMP | ColumnTypes::TIMESTAMP2 => 4,
                    ColumnTypes::DATETIME => 8,
                    ColumnTypes::ENUM => {
                        if ddc.elements.len() < 256 {
                            1
                        } else {
                            2
                        }
                    }
                    _ => todo!(
                        "Unsupported data_len type: ColumType::{}, utf8_def={}",
                        ddc.dd_type,
                        ddc.column_type_utf8
                    ),
                },
                _ => todo!("Unsupported data_len type: HiddenTypes::{}", ddc.hidden),
            },
            is_nullable: ddc.is_nullable,
            is_varfield: match &ddc.dd_type {
                ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING => true,
                _ => ddc.ordinal_position == 1 && ddc.column_key == ColumnKeys::CK_PRIMARY,
            },
            dd_type: ddc.dd_type.clone(),
            comment: ddc.comment.clone(),
            hidden: ddc.hidden.clone(),
            utf8_def: ddc.column_type_utf8.clone(),
            null_offset: 0,
            vfld_offset: 0,
            vfld_bytes: 0,
        }
    }
}
