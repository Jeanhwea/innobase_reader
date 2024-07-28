use crate::ibd::record::HiddenTypes::HT_HIDDEN_SE;
use crate::meta::def::{ColumnDef, TableDef};
use crate::util;
use bytes::Bytes;
use log::{trace, debug};
use num_enum::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;
use strum::{Display, EnumString};

pub const PAGE_ADDR_INF: usize = 99;
pub const PAGE_ADDR_SUP: usize = 112;

pub const REC_N_FIELDS_ONE_BYTE_MAX: u8 = 0x7f;

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
    table_def: Arc<TableDef>,
}

/// Row Dynamic Information, (pos, len, isnull, name)
///   1. pos: column ordinal position
///   2. len: row data length
///   3. isnull, row data is null
///   3. name: column name
#[derive(Debug)]
pub struct DynamicInfo(pub usize, pub usize, pub bool, pub String);

/// Row Data, (ord, len, buf),
///    1. opx: ordinal_position index
///    2. len: variadic field length
///    3. buf: row data buffer in bytes
#[derive(Debug)]
pub struct RowDatum(pub usize, pub usize, pub Option<Bytes>);

impl fmt::Debug for RowInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowInfo")
            .field("vfld_arr", &self.vfld_arr)
            .field("null_arr", &self.null_arr)
            .finish()
    }
}

impl RowInfo {
    pub fn new(varr: Vec<u8>, narr: Vec<u8>, tabdef: Arc<TableDef>) -> Self {
        Self {
            vfld_arr: varr,
            null_arr: narr,
            table_def: tabdef.clone(),
        }
    }

    fn isnull(&self, c: &ColumnDef) -> bool {
        if !c.isnil {
            return false;
        }

        let off = c.null_offset;
        let noff = util::numoff(off);
        let nidx = util::numidx(off);
        let mask = 1 << noff;
        trace!("offset={}, noff={}, nidx={}, mask=0b{:08b}", off, noff, nidx, mask);
        (self.null_arr[nidx] & mask) > 0
    }

    pub fn varlen(&self, c: &ColumnDef) -> usize {
        if !c.isvar {
            return c.data_len as usize;
        }

        let off = c.vfld_offset;
        match c.vfld_bytes {
            1 => self.vfld_arr[off] as usize,
            2 => {
                let b0 = self.vfld_arr[off + 1] as usize; // 0xb8
                let b1 = self.vfld_arr[off] as usize; // 0x8b
                let vlen = b0 + ((b1 & (REC_N_FIELDS_ONE_BYTE_MAX as usize)) << 8);
                debug!("{:02x} {:02x} => {}", b0, b1, vlen);
                vlen
            }
            _ => 0,
        }
    }

    pub fn dyninfo(&self) -> Vec<DynamicInfo> {
        self.table_def
            .col_defs
            .iter()
            .map(|c| {
                if self.isnull(c) {
                    DynamicInfo(c.pos, 0usize, self.isnull(c), c.col_name.clone())
                } else if !c.isvar {
                    DynamicInfo(c.pos, c.data_len as usize, self.isnull(c), c.col_name.clone())
                } else {
                    let vlen = self.varlen(c);
                    debug!("pos={}, vlen={}", c.pos, vlen);
                    DynamicInfo(c.pos, vlen, self.isnull(c), c.col_name.clone())
                }
            })
            .collect()
    }
}

#[derive(Default)]
pub struct Row {
    /// row address, the offset to the top of this page
    pub addr: usize,
    /// Row id, 6 bytes
    pub row_id: Option<u64>,
    /// transaction id, 6 bytes
    pub trx_id: u64,
    /// rollback pointer, 7 bytes
    pub roll_ptr: u64,
    /// row data list, (ord, vlen, buf),
    //    1. opx: ordinal_position index
    //    2. vlen: variadic field length
    //    3. buf: row data buffer in bytes
    pub row_data: Vec<RowDatum>,
    /// row buffer
    row_buffer: Bytes,
    table_def: Arc<TableDef>,
    row_dyn_info: Vec<DynamicInfo>,
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row")
            .field("addr", &self.addr)
            .field("row_id", &self.row_id)
            .field("trx_id", &self.trx_id)
            .field("roll_ptr", &self.roll_ptr)
            .field("row_dyn_info", &self.row_dyn_info)
            .finish()
    }
}

impl Row {
    pub fn new(addr: usize, rbuf: Bytes, tabdef: Arc<TableDef>, dyninfo: Vec<DynamicInfo>) -> Self {
        Self {
            addr,
            row_buffer: rbuf,
            table_def: tabdef,
            row_dyn_info: dyninfo,
            ..Row::default()
        }
    }
}

#[derive(Debug)]
pub struct Record {
    pub row_info: RowInfo,     // row information
    pub rec_hdr: RecordHeader, // record header
    pub row: Row,              // row data
}

impl Record {
    pub fn new(hdr: RecordHeader, rowinfo: RowInfo, data: Row) -> Self {
        Self {
            rec_hdr: hdr,
            row_info: rowinfo,
            row: data,
        }
    }

    pub fn unpack(&mut self) {
        let tabdef = self.row.table_def.clone();
        let rbuf = &self.row.row_buffer;

        // TODO: only read PRIMARY index data
        assert_eq!(&tabdef.idx_defs[0].idx_name, "PRIMARY");

        let mut end = 0usize;
        for e in &tabdef.idx_defs[0].elements {
            let di = &self.row.row_dyn_info[e.column_opx];
            let col = &tabdef.col_defs[e.column_opx];
            if di.2 {
                self.row.row_data.push(RowDatum(col.pos - 1, 0, None));
            } else {
                let len = di.1;
                self.row
                    .row_data
                    .push(RowDatum(col.pos - 1, len, Some(rbuf.slice(end..end + len))));
                end += len;
            }
        }

        for datum in &self.row.row_data {
            let col = &tabdef.col_defs[datum.0];
            if col.hidden != HT_HIDDEN_SE {
                continue;
            }
            match col.col_name.as_str() {
                "DB_ROW_ID" => {
                    self.row.row_id = Some(util::from_bytes6(datum.2.as_ref().unwrap().clone()));
                }
                "DB_TRX_ID" => {
                    self.row.trx_id = util::from_bytes6(datum.2.as_ref().unwrap().clone());
                }
                "DB_ROLL_PTR" => {
                    self.row.roll_ptr = util::from_bytes7(datum.2.as_ref().unwrap().clone());
                }
                _ => panic!("ERR_DB_META_COLUMN_NAME"),
            }
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
    pub schema_ref: String,
    pub created: u64,
    pub last_altered: u64,
    pub hidden: u8,
    pub collation_id: u32,
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
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone)]
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
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone, PartialEq)]
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
#[derive(Debug, Default, Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Eq, PartialEq, Clone)]
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
    pub ordinal_position: u32,
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
    pub char_length: u32,
    pub comment: String,
    pub collation_id: u32,
    pub column_key: ColumnKeys,
    pub column_type_utf8: String,
    pub elements: Vec<DataDictColumnElement>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DataDictColumnElement {
    pub index: u32,
    pub name: String,
}

// see sql/dd/types/index.h
//     enum class enum_index_type
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone)]
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
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone)]
pub enum IndexAlgorithm {
    IA_SE_SPECIFIC = 1,
    IA_BTREE = 2,
    IA_RTREE = 3,
    IA_HASH = 4,
    IA_FULLTEXT = 5,
    #[default]
    UNDEF,
}

// see sql/dd/types/index.h
//     enum class enum_index_algorithm
#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone, Copy)]
pub enum IndexOrder {
    #[default]
    ORDER_UNDEF = 1,
    ORDER_ASC = 2,
    ORDER_DESC = 3,
}

// see sql/dd/impl/types/index_impl.h
//    class Index_impl : public Entity_object_impl, public Index {
#[derive(Debug, Deserialize, Serialize)]
pub struct DataDictIndex {
    pub ordinal_position: u32,
    pub name: String,
    pub hidden: bool,
    pub comment: String,
    #[serde(rename = "type")]
    pub idx_type: IndexTypes,
    pub algorithm: IndexAlgorithm,
    pub is_visible: bool,
    pub engine: String,
    pub elements: Vec<DataDictIndexElement>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DataDictIndexElement {
    pub ordinal_position: u32,
    pub length: u32,
    pub order: IndexOrder,
    pub hidden: bool,
    pub column_opx: u32,
}
