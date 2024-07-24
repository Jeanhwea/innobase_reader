use num_enum::FromPrimitive;
use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

pub const PAGE_ADDR_INF: usize = 99;
pub const PAGE_ADDR_SUP: usize = 112;

#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecordStatus {
    REC_STATUS_ORDINARY = 0,
    REC_STATUS_NODE_PTR = 1,
    REC_STATUS_INFIMUM = 2,
    REC_STATUS_SUPREMUM = 3,
    #[num_enum(default)]
    UNDEF,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub info_bits: u8,            // 4 bits, MIN_REC/DELETED/VERSION/INSTANT, see rec.h
    pub n_owned: u8,              // 4 bits
    pub heap_no: u16,             // 13 bits
    pub rec_status: RecordStatus, // 3 bits, see rec.h
    pub next_rec_offset: i16,     // next record offset
}

impl RecordHeader {
    pub fn new(buffer: Bytes) -> Self {
        let b1 = u16::from_be_bytes(buffer.as_ref()[1..3].try_into().unwrap());
        let status = (b1 & 0x0007) as u8;
        Self {
            info_bits: (buffer[0] & 0xf0) >> 4,
            n_owned: (buffer[0] & 0x0f),
            heap_no: (b1 & 0xfff8) >> 3,
            rec_status: status.into(),
            next_rec_offset: i16::from_be_bytes(buffer.as_ref()[3..5].try_into().unwrap()),
        }
    }

    // Info bit denoting the predefined minimum record: this bit is set if and
    // only if the record is the first user record on a non-leaf B-tree page
    // that is the leftmost page on its level (PAGE_LEVEL is nonzero and
    // FIL_PAGE_PREV is FIL_NULL).
    const REC_INFO_MIN_REC_FLAG: u8 = 1;
    // The deleted flag in info bits; when bit is set to 1, it means the record
    // has been delete marked
    const REC_INFO_DELETED_FLAG: u8 = 2;
    // Use this bit to indicate record has version
    const REC_INFO_VERSION_FLAG: u8 = 4;
    // The instant ADD COLUMN flag. When it is set to 1, it means this record
    // was inserted/updated after an instant ADD COLUMN.
    const REC_INFO_INSTANT_FLAG: u8 = 8;

    pub fn is_min_rec(&self) -> bool {
        (self.info_bits & Self::REC_INFO_MIN_REC_FLAG) > 0
    }

    pub fn is_deleted(&self) -> bool {
        (self.info_bits & Self::REC_INFO_DELETED_FLAG) > 0
    }
}

#[derive(Debug)]
pub struct RecordInfo {
    // nullable list
    // vary field list
}

#[derive(Debug)]
pub struct Row {
    pub row_id: u64,   // 6 bytes
    pub trx_id: u64,   // 6 bytes
    pub roll_ptr: u64, // 7 bytes
}

#[derive(Debug)]
pub struct Record {
    pub rec_pre: Option<RecordInfo>, // record prefix information
    pub rec_hdr: RecordHeader,       // record header
    pub row: Option<Row>,            // row data
}

impl Record {
    pub fn new(hdr: RecordHeader) -> Self {
        Self {
            rec_hdr: hdr,
            rec_pre: None,
            row: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SdiObject {
    pub dd_object: DataDictObject,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DataDictObject {
    pub name: String,
    pub created: u64,
    pub last_altered: u64,
    pub hidden: u8,
    pub columns: Vec<DataDictColumn>,
    pub indexes: Vec<DataDictIndex>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

// see sql/dd/types/column.h
//     enum class enum_column_types
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Display, Clone, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
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
    #[num_enum(default)]
    UNDEF,
}

// see sql/dd/types/column.h
//     enum class enum_hidden_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Display, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
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
    #[num_enum(default)]
    UNDEF,
}

// see sql/dd/types/column.h
//     enum class enum_column_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum ColumnKeys {
    CK_NONE = 1,
    CK_PRIMARY = 2,
    CK_UNIQUE = 3,
    CK_MULTIPLE = 4,
    #[num_enum(default)]
    UNDEF,
}

// see sql/dd/impl/types/column_impl.h
//    class Column_impl : public Entity_object_impl, public Column {
#[derive(Debug, Deserialize, Serialize)]
pub struct DataDictColumn {
    #[serde(rename = "name")]
    pub col_name: String,
    #[serde(rename = "type")]
    pub dd_type: ColumnTypes,
    pub is_nullable: bool,
    pub is_zerofill: bool,
    pub is_unsigned: bool,
    pub is_auto_increment: bool,
    pub is_virtual: bool,
    pub hidden: HiddenTypes,
    pub ordinal_position: u32,
    pub char_length: u32,
    pub comment: String,
    pub column_key: ColumnKeys,
    pub column_type_utf8: String,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

// see sql/dd/types/index.h
//     enum class enum_index_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Display, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum IndexTypes {
    IT_PRIMARY = 1,
    IT_UNIQUE = 2,
    IT_MULTIPLE = 3,
    IT_FULLTEXT = 4,
    IT_SPATIAL = 5,
    #[num_enum(default)]
    UNDEF,
}

// see sql/dd/types/index.h
//     enum class enum_index_algorithm
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Display, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum IndexAlgorithm {
    IA_SE_SPECIFIC = 1,
    IA_BTREE = 2,
    IA_RTREE = 3,
    IA_HASH = 4,
    IA_FULLTEXT = 5,
    #[num_enum(default)]
    UNDEF,
}

// see sql/dd/impl/types/index_impl.h
//    class Index_impl : public Entity_object_impl, public Index {
#[derive(Debug, Deserialize, Serialize)]
pub struct DataDictIndex {
    pub name: String,
    pub hidden: bool,
    pub ordinal_position: u32,
    pub comment: String,
    #[serde(rename = "type")]
    pub idx_type: IndexTypes,
    pub algorithm: IndexAlgorithm,
    pub is_visible: bool,
    pub engine: String,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}
