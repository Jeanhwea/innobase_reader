use bytes::Bytes;
use serde::{Deserialize, Serialize};

use enum_display::EnumDisplay;
use serde_repr::{Deserialize_repr, Serialize_repr};

pub const PAGE_ADDR_INF: usize = 99;
pub const PAGE_ADDR_SUP: usize = 112;

#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, EnumDisplay, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecordStatus {
    REC_STATUS_ORDINARY = 0,
    REC_STATUS_NODE_PTR = 1,
    REC_STATUS_INFIMUM = 2,
    REC_STATUS_SUPREMUM = 3,
    MARKED(u8),
}

impl From<u8> for RecordStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => RecordStatus::REC_STATUS_ORDINARY,
            1 => RecordStatus::REC_STATUS_NODE_PTR,
            2 => RecordStatus::REC_STATUS_INFIMUM,
            3 => RecordStatus::REC_STATUS_SUPREMUM,
            _ => RecordStatus::MARKED(value),
        }
    }
}

#[derive(Debug)]
pub struct RecordHeader {
    pub info_bits: u8,            // 4 bits, MIN_REC/DELETED/VERSION/INSTANT, see rec.h
    pub n_owned: u8,              // 4 bits
    pub heap_no: u16,             // 13 bits
    pub rec_status: RecordStatus, // 3 bits, see rec.h
    pub next_rec_offset: u16,     // next record offset
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
            next_rec_offset: u16::from_be_bytes(buffer.as_ref()[3..5].try_into().unwrap()),
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
    pub mysqld_version_id: u32,
    pub dd_version: u32,
    pub sdi_version: u32,
    pub dd_object_type: String,
    pub dd_object: SdiDDObject,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SdiDDObject {
    pub name: String,
    pub mysql_version_id: u64,
    pub created: u64,
    pub last_altered: u64,
    pub hidden: u8,
    pub options: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
}

// see sql/dd/types/column.h
//     enum class enum_column_types
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Deserialize_repr, Serialize_repr, EnumDisplay)]
pub enum ColumnTypes {
    UNDEF = 0,
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
}

impl From<u8> for ColumnTypes {
    fn from(value: u8) -> Self {
        match value {
            1 => ColumnTypes::DECIMAL,
            2 => ColumnTypes::TINY,
            3 => ColumnTypes::SHORT,
            4 => ColumnTypes::LONG,
            5 => ColumnTypes::FLOAT,
            6 => ColumnTypes::DOUBLE,
            7 => ColumnTypes::TYPE_NULL,
            8 => ColumnTypes::TIMESTAMP,
            9 => ColumnTypes::LONGLONG,
            10 => ColumnTypes::INT24,
            11 => ColumnTypes::DATE,
            12 => ColumnTypes::TIME,
            13 => ColumnTypes::DATETIME,
            14 => ColumnTypes::YEAR,
            15 => ColumnTypes::NEWDATE,
            16 => ColumnTypes::VARCHAR,
            17 => ColumnTypes::BIT,
            18 => ColumnTypes::TIMESTAMP2,
            19 => ColumnTypes::DATETIME2,
            20 => ColumnTypes::TIME2,
            21 => ColumnTypes::NEWDECIMAL,
            22 => ColumnTypes::ENUM,
            23 => ColumnTypes::SET,
            24 => ColumnTypes::TINY_BLOB,
            25 => ColumnTypes::MEDIUM_BLOB,
            26 => ColumnTypes::LONG_BLOB,
            27 => ColumnTypes::BLOB,
            28 => ColumnTypes::VAR_STRING,
            29 => ColumnTypes::STRING,
            30 => ColumnTypes::GEOMETRY,
            31 => ColumnTypes::JSON,
            _ => ColumnTypes::UNDEF,
        }
    }
}

// see sql/dd/types/column.h
//     enum class enum_hidden_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Deserialize_repr, Serialize_repr, EnumDisplay)]
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
    // unknown hidden type
    HT_UNKNOWN = 0,
}

impl From<u8> for HiddenTypes {
    fn from(value: u8) -> Self {
        match value {
            1 => HiddenTypes::HT_VISIBLE,
            2 => HiddenTypes::HT_HIDDEN_SE,
            3 => HiddenTypes::HT_HIDDEN_SQL,
            4 => HiddenTypes::HT_HIDDEN_USER,
            _ => HiddenTypes::HT_UNKNOWN,
        }
    }
}

// see sql/dd/types/column.h
//     enum class enum_column_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Deserialize_repr, Serialize_repr, EnumDisplay)]
pub enum ColumnKeys {
    CK_NONE = 1,
    CK_PRIMARY = 2,
    CK_UNIQUE = 3,
    CK_MULTIPLE = 4,
    CK_UNKNOWN = 0,
}

impl From<u8> for ColumnKeys {
    fn from(value: u8) -> Self {
        match value {
            1 => ColumnKeys::CK_NONE,
            2 => ColumnKeys::CK_PRIMARY,
            3 => ColumnKeys::CK_UNIQUE,
            4 => ColumnKeys::CK_MULTIPLE,
            _ => ColumnKeys::CK_UNKNOWN,
        }
    }
}

// see sql/dd/impl/types/column_impl.h
//    class Column_impl : public Entity_object_impl, public Column {
#[derive(Debug, Deserialize, Serialize)]
pub struct Column {
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
    pub numeric_precision: u32,
    pub numeric_scale: u32,
    pub numeric_scale_null: bool,
    pub datetime_precision: u32,
    pub datetime_precision_null: u32,

    pub has_no_default: bool,

    pub default_value_null: bool,
    pub default_value: String,
    pub default_value_utf8_null: bool,
    pub default_value_utf8: String,

    pub default_option: String,
    pub update_option: String,
    pub comment: String,

    pub generation_expression: String,
    pub generation_expression_utf8: String,

    pub options: String,
    pub se_private_data: String,

    pub engine_attribute: String,
    pub secondary_engine_attribute: String,

    pub column_key: ColumnKeys,
    pub column_type_utf8: String,

    pub collation_id: u32,
    pub is_explicit_collation: bool,
}

// see sql/dd/types/index.h
//     enum class enum_index_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Deserialize_repr, Serialize_repr, EnumDisplay)]
pub enum IndexTypes {
    IT_PRIMARY = 1,
    IT_UNIQUE = 2,
    IT_MULTIPLE = 3,
    IT_FULLTEXT = 4,
    IT_SPATIAL = 5,
    IT_UNKNOWN = 0,
}

impl From<u8> for IndexTypes {
    fn from(value: u8) -> Self {
        match value {
            1 => IndexTypes::IT_PRIMARY,
            2 => IndexTypes::IT_UNIQUE,
            3 => IndexTypes::IT_MULTIPLE,
            4 => IndexTypes::IT_FULLTEXT,
            5 => IndexTypes::IT_SPATIAL,
            _ => IndexTypes::IT_UNKNOWN,
        }
    }
}

// see sql/dd/types/index.h
//     enum class enum_index_algorithm
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Deserialize_repr, Serialize_repr, EnumDisplay)]
pub enum IndexAlgorithm {
    IA_SE_SPECIFIC = 1,
    IA_BTREE = 2,
    IA_RTREE = 3,
    IA_HASH = 4,
    IA_FULLTEXT = 5,
    IA_UNKNOWN = 0,
}

impl From<u8> for IndexAlgorithm {
    fn from(value: u8) -> Self {
        match value {
            1 => IndexAlgorithm::IA_SE_SPECIFIC,
            2 => IndexAlgorithm::IA_BTREE,
            3 => IndexAlgorithm::IA_RTREE,
            4 => IndexAlgorithm::IA_HASH,
            5 => IndexAlgorithm::IA_FULLTEXT,
            _ => IndexAlgorithm::IA_UNKNOWN,
        }
    }
}

// see sql/dd/impl/types/index_impl.h
//    class Index_impl : public Entity_object_impl, public Index {
#[derive(Debug, Deserialize, Serialize)]
pub struct Index {
    pub name: String,
    pub hidden: bool,
    pub is_generated: bool,
    pub ordinal_position: u32,
    pub comment: String,
    pub options: String,
    #[serde(rename = "type")]
    pub dd_type: IndexTypes,
    pub algorithm: IndexAlgorithm,
    pub is_algorithm_explicit: bool,
    pub is_visible: bool,
    pub engine: String,
    pub engine_attribute: String,
    pub secondary_engine_attribute: String,
    pub tablespace_ref: String,
}
