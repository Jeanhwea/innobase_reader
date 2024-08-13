use bytes::Bytes;

use super::cst::Collation;
use crate::{
    ibd::record::{
        ColumnKeys, ColumnTypes, DataDictColumn, DataDictIndex, DataDictIndexElement, DataDictObject, HiddenTypes,
        IndexAlgorithm, IndexOrder, IndexTypes,
    },
    meta::cst::coll_find,
    util::{self, conv_strdata_to_bytes},
};

#[derive(Debug, Default, Clone)]
pub struct TableDef {
    pub schema_ref: String,       // schema name
    pub tab_name: String,         // table name
    pub collation_id: u32,        // collation, see INFORMATION_SCHEMA.COLLATIONS
    pub collation: String,        // collation name
    pub charset: String,          // character set name
    pub col_defs: Vec<ColumnDef>, // column definitions
    pub idx_defs: Vec<IndexDef>,  // index definitions

    /// indicate how many columns exist before first instant ADD COLUMN in table level
    pub instant_col: i32,
}

impl TableDef {
    pub fn from(ddo: &DataDictObject, coll: &Collation, coldefs: Vec<ColumnDef>, idxdefs: Vec<IndexDef>) -> Self {
        let priv_data = util::conv_strdata_to_map(&ddo.se_private_data);
        Self {
            schema_ref: ddo.schema_ref.clone(),
            tab_name: ddo.name.clone(),
            collation_id: ddo.collation_id,
            collation: coll.name.into(),
            charset: coll.charset.into(),
            col_defs: coldefs,
            idx_defs: idxdefs,
            instant_col: priv_data
                .get("instant_col")
                .map(|v| v.parse::<i32>().unwrap_or(0))
                .unwrap_or(-1),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ColumnDef {
    pub pos: usize,            // ordinal position
    pub col_name: String,      // column name
    pub data_len: u32,         // data length in bytes
    pub isnil: bool,           // is nullable field
    pub isvar: bool,           // is variadic field
    pub dd_type: ColumnTypes,  // data dictionary type
    pub hidden: HiddenTypes,   // hidden type
    pub col_key: ColumnKeys,   // column key type
    pub utf8_def: String,      // utf8 column definition
    pub comment: String,       // comment
    pub coll_id: u32,          // collation
    pub coll_name: String,     // collation name
    pub charset: String,       // character set name
    pub version_added: u32,    // which version this column was added
    pub version_dropped: u32,  // which version this column waw dropped
    pub defval: Option<Bytes>, // default value in se_private_data
    pub phy_pos: i32,          // physical position
}

impl ColumnDef {
    pub fn from(ddc: &DataDictColumn) -> Self {
        let coll = coll_find(ddc.collation_id);
        let priv_data = util::conv_strdata_to_map(&ddc.se_private_data);
        let default_null = priv_data.get("default_null").map(|v| v == "1").unwrap_or(false);

        let default = if !default_null {
            priv_data
                .get("default")
                .map(|v| conv_strdata_to_bytes(v))
                .unwrap_or(None)
        } else {
            None
        };

        Self {
            pos: ddc.ordinal_position as usize,
            col_name: ddc.col_name.clone(),
            col_key: ddc.column_key.clone(),
            data_len: match ddc.hidden {
                HiddenTypes::HT_HIDDEN_SE => ddc.char_length,
                HiddenTypes::HT_VISIBLE => match ddc.dd_type {
                    ColumnTypes::VAR_STRING | ColumnTypes::STRING | ColumnTypes::DECIMAL => ddc.char_length,
                    ColumnTypes::VARCHAR => ddc.char_length + (if ddc.char_length < 256 { 1 } else { 2 }),
                    ColumnTypes::YEAR | ColumnTypes::TINY => 1,
                    ColumnTypes::SHORT => 2,
                    ColumnTypes::INT24 | ColumnTypes::NEWDATE | ColumnTypes::TIME => 3,
                    ColumnTypes::LONG => 4,
                    ColumnTypes::LONGLONG => 8,
                    ColumnTypes::DATE | ColumnTypes::TIMESTAMP | ColumnTypes::TIMESTAMP2 => 4,
                    ColumnTypes::DATETIME => 8,
                    ColumnTypes::DATETIME2 => 5,
                    ColumnTypes::ENUM => (if ddc.elements.len() < 256 { 1 } else { 2 }) as u32,
                    ColumnTypes::JSON => ddc.char_length,
                    _ => todo!(
                        "不支持的数据长度类型: ColumType::{}, utf8_def={}",
                        ddc.dd_type,
                        ddc.column_type_utf8
                    ),
                },
                _ => todo!("不支持的数据长度类型: HiddenTypes::{}", ddc.hidden),
            },
            isnil: ddc.is_nullable,
            isvar: match coll.charset {
                "latin1" | "binary" => matches!(&ddc.dd_type, ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING),
                "utf8mb4" => matches!(
                    &ddc.dd_type,
                    ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING
                ),
                _ => todo!("不支持的字符集: {:?}", &coll),
            },

            dd_type: ddc.dd_type.clone(),
            comment: ddc.comment.clone(),
            coll_id: ddc.collation_id,
            coll_name: coll.name.into(),
            charset: coll.charset.into(),
            hidden: ddc.hidden.clone(),
            utf8_def: ddc.column_type_utf8.clone(),
            phy_pos: priv_data
                .get("physical_pos")
                .map(|v| v.parse::<i32>().unwrap_or(0))
                .unwrap_or(-1),
            version_added: priv_data
                .get("version_added")
                .map(|v| v.parse::<u32>().unwrap_or(0))
                .unwrap_or(0),
            version_dropped: priv_data
                .get("version_dropped")
                .map(|v| v.parse::<u32>().unwrap_or(0))
                .unwrap_or(0),
            defval: default,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct IndexDef {
    pub pos: usize,                     // ordinal position
    pub idx_name: String,               // index name
    pub idx_id: u64,                    // index id
    pub hidden: bool,                   // hidden
    pub idx_type: IndexTypes,           // index type
    pub algorithm: IndexAlgorithm,      // index algorithm
    pub comment: String,                // Comment
    pub elements: Vec<IndexElementDef>, // index elememts
}

impl IndexDef {
    pub fn from(ddi: &DataDictIndex, ele_defs: Vec<IndexElementDef>) -> Self {
        let priv_data = util::conv_strdata_to_map(&ddi.se_private_data);
        let id: u64 = priv_data["id"].parse().unwrap_or(0);
        Self {
            pos: ddi.ordinal_position as usize,
            idx_name: ddi.name.clone(),
            idx_id: id,
            hidden: ddi.hidden,
            idx_type: ddi.idx_type.clone(),
            algorithm: ddi.algorithm.clone(),
            comment: ddi.comment.clone(),
            elements: ele_defs,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct IndexElementDef {
    pub col_name: String,  // referenced column name
    pub utf8_def: String,  // utf8 column definition
    pub pos: usize,        // ordinal position
    pub ele_len: i32,      // element length
    pub order: IndexOrder, // order, ASC/DESC
    pub hidden: bool,      // hidden
    /// see write_opx_reference(w, m_column, STRING_WITH_LEN("column_opx"));
    pub column_opx: usize, // opx: ordinal position index
    pub col_hidden: HiddenTypes, // hidden type
    pub data_len: u32,     // data length
}

impl IndexElementDef {
    pub fn from(ele: &DataDictIndexElement, col: &ColumnDef) -> Self {
        let len = ele.length as i32;
        Self {
            col_name: col.col_name.clone(),
            utf8_def: col.utf8_def.clone(),
            pos: ele.ordinal_position as usize,
            ele_len: len,
            order: ele.order,
            hidden: ele.hidden,
            column_opx: ele.column_opx as usize,
            col_hidden: col.hidden.clone(),
            data_len: col.data_len,
        }
    }
}
