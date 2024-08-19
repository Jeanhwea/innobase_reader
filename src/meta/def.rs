use bytes::Bytes;
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use super::cst::Collation;
use crate::{
    ibd::record::{DataDictColumn, DataDictIndex, DataDictIndexElement, DataDictObject},
    meta::cst::coll_find,
    util::{self, conv_strdata_to_bytes},
};

/// column type, see sql/dd/types/column.h, enum class enum_column_types
#[repr(u8)]
#[derive(
    Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone,
)]
pub enum ColumnTypes {
    DECIMAL = 1,
    TINY = 2,
    SHORT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    TYPE_NULL = 7,
    TIMESTAMP = 8,
    LONGLONG = 9,
    INT24 = 10,
    DATE = 11,
    TIME = 12,
    DATETIME = 13,
    YEAR = 14,
    NEWDATE = 15,
    VARCHAR = 16,
    BIT = 17,
    TIMESTAMP2 = 18,
    DATETIME2 = 19,
    TIME2 = 20,
    NEWDECIMAL = 21,
    ENUM = 22,
    SET = 23,
    TINY_BLOB = 24,
    MEDIUM_BLOB = 25,
    LONG_BLOB = 26,
    BLOB = 27,
    VAR_STRING = 28,
    STRING = 29,
    GEOMETRY = 30,
    JSON = 31,
    #[default]
    UNDEF,
}

/// column keys, see sql/dd/types/column.h
#[repr(u8)]
#[derive(
    Debug,
    Default,
    Deserialize_repr,
    Serialize_repr,
    EnumString,
    FromPrimitive,
    Eq,
    PartialEq,
    Clone,
)]
pub enum ColumnKeys {
    CK_NONE = 1,
    CK_PRIMARY = 2,
    CK_UNIQUE = 3,
    CK_MULTIPLE = 4,
    #[default]
    UNDEF,
}

/// column hidden type, see sql/dd/types/column.h, enum class enum_hidden_type
#[repr(u8)]
#[derive(
    Deserialize_repr,
    Serialize_repr,
    EnumString,
    FromPrimitive,
    Debug,
    Display,
    Default,
    Clone,
    PartialEq,
)]
pub enum HiddenTypes {
    /// The column is visible (a normal column)
    HT_VISIBLE = 1,
    /// The column is completely invisible to the server
    HT_HIDDEN_SE = 2,
    /// The column is visible to the server, but hidden from the user.
    /// This is used for i.e. implementing functional indexes.
    HT_HIDDEN_SQL = 3,
    /// User table column marked as INVISIBLE by using the column visibility
    /// attribute. Column is hidden from the user unless it is explicitly
    /// referenced in the statement. Column is visible to the server.
    HT_HIDDEN_USER = 4,
    #[default]
    UNDEF,
}

/// row format, see sql/dd/types/table.h, enum enum_row_format
#[repr(i8)]
#[derive(
    Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone,
)]
pub enum RowFormats {
    RF_FIXED = 1,
    RF_DYNAMIC = 2,
    RF_COMPRESSED = 3,
    RF_REDUNDANT = 4,
    RF_COMPACT = 5,
    RF_PAGED = 6,
    #[default]
    UNDEF,
}

/// index type, see sql/dd/types/index.h, enum class enum_index_type
#[repr(u8)]
#[derive(
    Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone,
)]
pub enum IndexTypes {
    IT_PRIMARY = 1,
    IT_UNIQUE = 2,
    IT_MULTIPLE = 3,
    IT_FULLTEXT = 4,
    IT_SPATIAL = 5,
    #[default]
    UNDEF,
}

/// index algorithm, see sql/dd/types/index.h, enum class enum_index_algorithm
#[repr(u8)]
#[derive(
    Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone,
)]
pub enum IndexAlgorithm {
    IA_SE_SPECIFIC = 1,
    IA_BTREE = 2,
    IA_RTREE = 3,
    IA_HASH = 4,
    IA_FULLTEXT = 5,
    #[default]
    UNDEF,
}

/// index order, see sql/dd/types/index.h, enum class enum_index_algorithm
#[repr(u8)]
#[derive(
    Deserialize_repr,
    Serialize_repr,
    EnumString,
    FromPrimitive,
    Debug,
    Display,
    Default,
    Clone,
    Copy,
)]
pub enum IndexOrder {
    #[default]
    ORDER_UNDEF = 1,
    ORDER_ASC = 2,
    ORDER_DESC = 3,
}

/// table definition
#[derive(Debug, Default, Clone)]
pub struct TableDef {
    /// schema name
    pub schema_ref: String,

    /// table name
    pub tab_name: String,

    /// collation, see INFORMATION_SCHEMA.COLLATIONS
    pub collation_id: u32,

    /// collation name
    pub collation: String,

    /// character set name
    pub charset: String,

    /// row format
    pub row_format: RowFormats,

    /// column definitions
    pub col_defs: Vec<ColumnDef>,

    /// index definitions
    pub idx_defs: Vec<IndexDef>,

    /// indicate how many columns exist before first instant ADD COLUMN in table level
    pub instant_col: i32,
}

impl TableDef {
    pub fn from(
        ddo: &DataDictObject,
        coll: &Collation,
        coldefs: Vec<ColumnDef>,
        idxdefs: Vec<IndexDef>,
    ) -> Self {
        let priv_data = util::conv_strdata_to_map(&ddo.se_private_data);
        Self {
            schema_ref: ddo.schema_ref.clone(),
            tab_name: ddo.name.clone(),
            collation_id: ddo.collation_id,
            collation: coll.name.into(),
            charset: coll.charset.into(),
            row_format: ddo.row_format.into(),
            col_defs: coldefs,
            idx_defs: idxdefs,
            instant_col: priv_data
                .get("instant_col")
                .map(|v| v.parse::<i32>().unwrap_or(0))
                .unwrap_or(-1),
        }
    }
}

/// column definition
#[derive(Debug, Default, Clone)]
pub struct ColumnDef {
    /// ordinal position
    pub pos: usize,

    /// column name
    pub col_name: String,

    /// data length in bytes
    pub data_len: u32,

    /// is nullable field
    pub isnil: bool,

    /// is variadic field
    pub isvar: bool,

    /// data dictionary type
    pub dd_type: ColumnTypes,

    /// hidden type
    pub hidden: HiddenTypes,

    /// column key type
    pub col_key: ColumnKeys,

    /// utf8 column definition
    pub utf8_def: String,

    /// comment
    pub comment: String,

    /// collation
    pub coll_id: u32,

    /// collation name
    pub coll_name: String,

    /// character set name
    pub charset: String,

    /// table id
    pub table_id: i32,

    /// which version this column was added
    pub version_added: u32,

    /// which version this column waw dropped
    pub version_dropped: u32,

    /// default value in se_private_data
    pub defval: Option<Bytes>,

    /// physical position
    pub phy_pos: i32,
}

impl ColumnDef {
    pub fn from(ddc: &DataDictColumn) -> Self {
        let coll = coll_find(ddc.collation_id);

        let priv_data = util::conv_strdata_to_map(&ddc.se_private_data);
        let default_null = priv_data
            .get("default_null")
            .map(|v| v == "1")
            .unwrap_or(false);

        let default = if !default_null {
            priv_data
                .get("default")
                .map(|v| conv_strdata_to_bytes(v))
                .unwrap_or(None)
        } else {
            None
        };

        let ddtype = ddc.dd_type.into();

        Self {
            pos: ddc.ordinal_position as usize,
            col_name: ddc.col_name.clone(),
            col_key: ddc.column_key.into(),
            data_len: match ddc.hidden {
                HiddenTypes::HT_HIDDEN_SE => ddc.char_length,
                HiddenTypes::HT_VISIBLE => match ddtype {
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
                    ColumnTypes::DATETIME2 => 5,
                    ColumnTypes::ENUM => (if ddc.elements.len() < 256 { 1 } else { 2 }) as u32,
                    ColumnTypes::JSON => ddc.char_length,
                    _ => todo!(
                        "不支持的数据长度类型: ColumType::{}, utf8_def={}",
                        ddtype,
                        ddc.column_type_utf8
                    ),
                },
                _ => todo!("不支持的数据长度类型: HiddenTypes::{}", ddc.hidden),
            },
            isnil: ddc.is_nullable,
            isvar: match coll.charset {
                "latin1" | "binary" => {
                    matches!(ddtype, ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING)
                }
                "utf8mb4" => matches!(
                    ddtype,
                    ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING
                ),
                _ => todo!("不支持的字符集: {:?}", &coll),
            },
            dd_type: ddtype,
            comment: ddc.comment.clone(),
            coll_id: ddc.collation_id,
            coll_name: coll.name.into(),
            charset: coll.charset.into(),
            table_id: priv_data
                .get("table_id")
                .map(|v| v.parse::<i32>().unwrap_or(0))
                .unwrap_or(-1),
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

/// index definition
#[derive(Debug, Default, Clone)]
pub struct IndexDef {
    /// ordinal position
    pub pos: usize,

    /// index name
    pub idx_name: String,

    /// table id
    pub table_id: i32,

    /// index id
    pub idx_id: i32,

    /// index root page_no
    pub idx_root: i32,

    /// hidden
    pub hidden: bool,

    /// index type
    pub idx_type: IndexTypes,

    /// index algorithm
    pub algorithm: IndexAlgorithm,

    /// comment
    pub comment: String,

    /// index elememts
    pub elements: Vec<IndexElementDef>,
}

impl IndexDef {
    pub fn from(ddi: &DataDictIndex, ele_defs: Vec<IndexElementDef>) -> Self {
        let priv_data = util::conv_strdata_to_map(&ddi.se_private_data);
        let index_id: i32 = priv_data
            .get("id")
            .map(|v| v.parse::<i32>().unwrap_or(0))
            .unwrap_or(-1);
        Self {
            pos: ddi.ordinal_position as usize,
            table_id: priv_data
                .get("table_id")
                .map(|v| v.parse::<i32>().unwrap_or(0))
                .unwrap_or(-1),
            idx_name: ddi.name.clone(),
            idx_id: index_id,
            idx_root: priv_data
                .get("root")
                .map(|v| v.parse::<i32>().unwrap_or(0))
                .unwrap_or(-1),
            hidden: ddi.hidden,
            idx_type: ddi.idx_type.into(),
            algorithm: ddi.algorithm.into(),
            comment: ddi.comment.clone(),
            elements: ele_defs,
        }
    }
}

/// index element definition
#[derive(Debug, Default, Clone)]
pub struct IndexElementDef {
    /// referenced column name
    pub col_name: String,

    /// utf8 column definition
    pub utf8_def: String,

    /// ordinal position
    pub pos: usize,

    /// element length
    pub ele_len: i32,

    /// order, ASC/DESC
    pub order: IndexOrder,

    /// hidden
    pub hidden: bool,

    /// see write_opx_reference(w, m_column, STRING_WITH_LEN("column_opx"));
    pub column_opx: usize, // opx: ordinal position index

    /// hidden type
    pub col_hidden: HiddenTypes,

    /// data length
    pub data_len: u32,
}

impl IndexElementDef {
    pub fn from(ele: &DataDictIndexElement, col: &ColumnDef) -> Self {
        let len = ele.length as i32;
        Self {
            col_name: col.col_name.clone(),
            utf8_def: col.utf8_def.clone(),
            pos: ele.ordinal_position as usize,
            ele_len: len,
            order: ele.order.into(),
            hidden: ele.hidden,
            column_opx: ele.column_opx as usize,
            col_hidden: col.hidden.clone(),
            data_len: col.data_len,
        }
    }
}
