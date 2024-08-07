use crate::ibd::record::ColumnKeys;
use crate::ibd::record::ColumnTypes;
use crate::ibd::record::DataDictColumn;
use crate::ibd::record::DataDictIndex;
use crate::ibd::record::DataDictIndexElement;
use crate::ibd::record::HiddenTypes;
use crate::ibd::record::IndexAlgorithm;
use crate::ibd::record::IndexOrder;
use crate::ibd::record::IndexTypes;
use crate::meta::cst::coll_find;
use crate::util;

#[derive(Debug, Default, Clone)]
pub struct TableDef {
    pub schema_ref: String,       // schema name
    pub tab_name: String,         // table name
    pub collation_id: u32,        // collation, see INFORMATION_SCHEMA.COLLATIONS
    pub collation: String,        // collation name
    pub charset: String,          // character set name
    pub col_defs: Vec<ColumnDef>, // column definitions
    pub idx_defs: Vec<IndexDef>,  // index definitions
}

#[derive(Debug, Default, Clone)]
pub struct ColumnDef {
    pub pos: usize,           // ordinal position
    pub col_name: String,     // column name
    pub data_len: u32,        // data length in bytes
    pub isnil: bool,          // is nullable field
    pub isvar: bool,          // is variadic field
    pub dd_type: ColumnTypes, // data dictionary type
    pub hidden: HiddenTypes,  // hidden type
    pub col_key: ColumnKeys,  // column key type
    pub utf8_def: String,     // utf8 column definition
    pub comment: String,      // Comment
    pub collation_id: u32,    // collation
    pub collation: String,    // collation name
    pub charset: String,      // character set name
}

impl ColumnDef {
    pub fn from(ddc: &DataDictColumn) -> Self {
        let coll = coll_find(ddc.collation_id);
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
                        "Unsupported data_len type: ColumType::{}, utf8_def={}",
                        ddc.dd_type,
                        ddc.column_type_utf8
                    ),
                },
                _ => todo!("Unsupported data_len type: HiddenTypes::{}", ddc.hidden),
            },
            isnil: ddc.is_nullable,
            isvar: matches!(
                &ddc.dd_type,
                ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING
            ),
            dd_type: ddc.dd_type.clone(),
            comment: ddc.comment.clone(),
            collation_id: ddc.collation_id,
            collation: coll.name.into(),
            charset: coll.charset.into(),
            hidden: ddc.hidden.clone(),
            utf8_def: ddc.column_type_utf8.clone(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct IndexDef {
    pub pos: usize,                     // ordinal position
    pub idx_name: String,               // index name
    pub idx_id: u64,                    // index id
    pub hidden: bool,                   // hidden
    pub comment: String,                // Comment
    pub idx_type: IndexTypes,           // index type
    pub algorithm: IndexAlgorithm,      // index algorithm
    pub elements: Vec<IndexElementDef>, // index elememts
    pub nil_area_size: usize,           // nullable flag size
}

impl IndexDef {
    pub fn from(ddi: &DataDictIndex, ele_defs: Vec<IndexElementDef>, nil_size: usize) -> Self {
        let priv_data = util::conv_strdata_to_map(&ddi.se_private_data);
        let id: u64 = priv_data["id"].parse().unwrap_or(0);
        Self {
            pos: ddi.ordinal_position as usize,
            idx_name: ddi.name.clone(),
            idx_id: id,
            hidden: ddi.hidden,
            comment: ddi.comment.clone(),
            idx_type: ddi.idx_type.clone(),
            algorithm: ddi.algorithm.clone(),
            elements: ele_defs,
            nil_area_size: nil_size,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct IndexElementDef {
    pub pos: usize,        // ordinal position
    pub ele_len: i32,      // element length
    pub order: IndexOrder, // order, ASC/DESC
    pub hidden: bool,      // hidden
    /// see write_opx_reference(w, m_column, STRING_WITH_LEN("column_opx"));
    pub column_opx: usize, // opx: ordinal position index
    pub col_name: String,  // referenced column name
    pub col_hidden: HiddenTypes, // hidden type
    pub data_len: u32,     // data length
    pub isnil: bool,       // is nullable field
    pub isvar: bool,       // is variadic field
    pub null_offset: usize, // nullable offset
}

impl IndexElementDef {
    pub fn from(ele: &DataDictIndexElement, col: &ColumnDef) -> Self {
        let len = ele.length as i32;
        Self {
            pos: ele.ordinal_position as usize,
            ele_len: len,
            order: ele.order,
            hidden: ele.hidden,
            column_opx: ele.column_opx as usize,
            col_name: col.col_name.clone(),
            col_hidden: col.hidden.clone(),
            data_len: col.data_len,
            isnil: col.isnil,
            isvar: col.isvar,
            ..IndexElementDef::default()
        }
    }
}
