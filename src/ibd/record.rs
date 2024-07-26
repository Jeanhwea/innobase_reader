use crate::ibd::tabspace::ColumnDef;
use crate::{ibd::tabspace::TableDef, util};
use bytes::Bytes;
use log::info;
use num_enum::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
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
    #[default]
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

pub struct RowInfo {
    pub vfld_arr: Vec<u8>, // variadic field array in reversed order
    pub null_arr: Vec<u8>, // nullable flag array in reversed order
    tabdef: TableDef,
}

impl fmt::Debug for RowInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowInfo")
            .field("vfld_arr", &self.vfld_arr)
            .field("null_arr", &self.null_arr)
            .finish()
    }
}

impl RowInfo {
    pub fn new(varr: Vec<u8>, narr: Vec<u8>, tabdef: TableDef) -> Self {
        Self {
            vfld_arr: varr,
            null_arr: narr,
            tabdef,
        }
    }

    pub fn isnull(&self, c: &ColumnDef) -> bool {
        if !c.is_nullable {
            return false;
        }

        let offset = c.null_offset;
        let noff = util::numoff(offset);
        let nidx = util::numidx(offset);
        let mask = 1 << noff;
        info!(
            "offset={}, noff={}, nidx={}, mask=0b{:08b}",
            offset, noff, nidx, mask
        );
        (self.null_arr[nidx] & mask) > 0
    }

    pub fn varlen(&self, c: &ColumnDef) -> usize {
        if !c.is_varfield {
            return c.data_len as usize;
        }

        let off = c.vfld_offset;
        match c.vfld_bytes {
            1 => self.vfld_arr[off] as usize,
            2 => u16::from_be_bytes(self.vfld_arr[off..off + 2].try_into().unwrap()) as usize,
            _ => 0,
        }
    }

    pub fn calc_rowsize(&self) -> usize {
        let mut rowsize = 0usize;
        for c in &self.tabdef.col_defs {
            if self.isnull(c) {
                continue;
            }
            if !c.is_varfield {
                rowsize += c.data_len as usize;
                continue;
            }
            rowsize += self.varlen(c);
        }
        rowsize
    }
}

#[derive(Debug)]
pub struct Row {
    // pub row_id: u64,   // 6 bytes
    // pub trx_id: u64,   // 6 bytes
    // pub roll_ptr: u64, // 7 bytes
    row_buffer: Bytes,
}

impl Row {
    pub fn new(rbuf: Bytes) -> Self {
        Self { row_buffer: rbuf }
    }
}

#[derive(Debug)]
pub struct Record {
    pub row_info: RowInfo,     // row information
    pub rec_hdr: RecordHeader, // record header
    pub row: Row,              // row data
}

impl Record {
    pub fn new(hdr: RecordHeader, rowinfo: RowInfo, row: Row) -> Self {
        Self {
            rec_hdr: hdr,
            row_info: rowinfo,
            row: row,
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
#[derive(
    Debug, Display, Default, Clone, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive,
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

// see sql/dd/types/column.h
//     enum class enum_hidden_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(
    Debug, Display, Default, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Clone,
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

// see sql/dd/types/column.h
//     enum class enum_column_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
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

// see sql/dd/impl/types/column_impl.h
//    class Column_impl : public Entity_object_impl, public Column {
#[derive(Debug, Default, Deserialize, Serialize)]
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
    #[default]
    UNDEF,
}

// see sql/dd/types/index.h
//     enum class enum_index_algorithm
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Display, Default, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum IndexAlgorithm {
    IA_SE_SPECIFIC = 1,
    IA_BTREE = 2,
    IA_RTREE = 3,
    IA_HASH = 4,
    IA_FULLTEXT = 5,
    #[default]
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
