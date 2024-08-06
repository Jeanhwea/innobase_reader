use crate::meta::def::{IndexDef, TableDef};
use crate::util;
use bytes::Bytes;
use derivative::Derivative;
use log::debug;
use num_enum::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::sync::Arc;
use strum::{Display, EnumString};
use crate::ibd::page::{RECORD_HEADER_SIZE, SDI_DATA_HEADER_SIZE};

pub const REC_N_FIELDS_ONE_BYTE_MAX: u8 = 0x7f;

// rec.h:152
#[repr(u8)]
#[derive(Debug, Display, EnumString, FromPrimitive, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecordStatus {
    ORDINARY = 0,
    NODE_PTR = 1,
    INFIMUM = 2,
    SUPREMUM = 3,
    #[default]
    UNDEF,
}

#[derive(Debug, Display, EnumString, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecInfoFlag {
    MIN_REC,
    DELETED,
    VERSION,
    INSTANT,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RecordHeader {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    pub info_bits: Vec<RecInfoFlag>, // 4 bits, MIN_REC/DELETED/VERSION/INSTANT, see rec.h
    pub n_owned: u8,                 // 4 bits
    pub heap_no: u16,                // 13 bits
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub rec_status: RecordStatus, // 3 bits, see rec.h
    pub next_rec_offset: i16,        // next record offset
}

impl RecordHeader {
    // Info bit denoting the predefined minimum record: this bit is set if and
    // only if the record is the first user record on a non-leaf B-tree page
    // that is the leftmost page on its level (PAGE_LEVEL is nonzero and
    // FIL_PAGE_PREV is FIL_NULL).
    const REC_INFO_MIN_REC_FLAG: u8 = 0x10;
    // The deleted flag in info bits; when bit is set to 1, it means the record
    // has been delete marked
    const REC_INFO_DELETED_FLAG: u8 = 0x20;
    // Use this bit to indicate record has version
    const REC_INFO_VERSION_FLAG: u8 = 0x40;
    // The instant ADD COLUMN flag. When it is set to 1, it means this record
    // was inserted/updated after an instant ADD COLUMN.
    const REC_INFO_INSTANT_FLAG: u8 = 0x80;

    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let b0 = buf[addr];
        let b1 = util::u16_val(&buf, addr + 1);
        debug!("rec_hdr, b0=0x{:0x?}, b1=0x{:0x?}", b0, b1);

        let mut flags = Vec::new();
        if (b0 & Self::REC_INFO_MIN_REC_FLAG) > 0 {
            flags.push(RecInfoFlag::MIN_REC);
        }
        if (b0 & Self::REC_INFO_DELETED_FLAG) > 0 {
            flags.push(RecInfoFlag::DELETED);
        }
        if (b0 & Self::REC_INFO_VERSION_FLAG) > 0 {
            flags.push(RecInfoFlag::VERSION);
        }
        if (b0 & Self::REC_INFO_INSTANT_FLAG) > 0 {
            flags.push(RecInfoFlag::INSTANT);
        }

        let status = ((b1 & 0x0007) as u8).into();

        Self {
            info_bits: flags,
            n_owned: (b0 & 0x0f),
            heap_no: (b1 & 0xfff8) >> 3,
            rec_status: status,
            next_rec_offset: util::i16_val(&buf, addr + 3),
            buf: buf.clone(),
            addr,
        }
    }

    pub fn next_addr(&self) -> usize {
        ((self.addr as i16) + self.next_rec_offset) as usize + RECORD_HEADER_SIZE
    }
}

/// Row Dynamic Information, (pos, len, isnull, name)
///   1. pos: index element ordinal position
///   2. len: row data length
///   3. isnull, row data is null
///   4. name: column name
///   5. opx: column ordinal position index (opx)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct DynamicInfo(pub usize, pub usize, pub bool, pub String, pub usize);

/// Row Data, (ord, len, buf),
///    1. opx: ordinal_position index
///    2. len: variadic field length
///    3. buf: row data buffer in bytes
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RowDatum(pub usize, pub usize, pub Option<Bytes>);

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RowInfo {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address, [nilfld, varfld], access in reverse order
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    pub var_area: Bytes, // variadic field size area
    pub nil_area: Bytes, // nullable field flag area

    // calculated dynamic info
    pub row_dyn_info: Vec<DynamicInfo>,
    #[derivative(Debug = "ignore")]
    pub table_def: Arc<TableDef>,
}

impl RowInfo {
    pub fn new(addr: usize, buf: Arc<Bytes>, tabdef: Arc<TableDef>, idxdef: &IndexDef) -> Self {
        let nilptr = addr;
        let mut varptr = addr - idxdef.nil_area_size;

        let row_dyn_info = idxdef
            .elements
            .iter()
            .map(|e| {
                // debug!("nilptr={}, varptr={}", nilptr, varptr);
                let isnull = if e.isnil {
                    let null_index = util::numidx(e.null_offset);
                    let null_mask = 1 << util::numoff(e.null_offset);
                    let null_flag = buf[nilptr - null_index - 1];
                    (null_flag & null_mask) > 0
                } else {
                    false
                };

                if isnull {
                    DynamicInfo(e.pos, 0, isnull, e.col_name.clone(), e.column_opx)
                } else if !e.isvar {
                    DynamicInfo(e.pos, e.data_len as usize, isnull, e.col_name.clone(), e.column_opx)
                } else {
                    // see function in mysql-server source code
                    // static inline uint8_t rec_get_n_fields_length(ulint n_fields) {
                    //   return (n_fields > REC_N_FIELDS_ONE_BYTE_MAX ? 2 : 1);
                    // }
                    let vfld_bytes = if e.data_len > REC_N_FIELDS_ONE_BYTE_MAX as u32 {
                        2
                    } else {
                        1
                    };

                    let vlen = match vfld_bytes {
                        1 => {
                            let b0 = buf[varptr - 1] as usize;
                            varptr -= 1;
                            b0
                        }
                        2 => {
                            let b0 = buf[varptr - 1] as usize;
                            varptr -= 1;

                            if b0 > REC_N_FIELDS_ONE_BYTE_MAX.into() {
                                let b1 = buf[varptr - 1] as usize;
                                varptr -= 1;
                                // debug!("b0=0x{:0x?}, b1=0x{:0x?}", b0, b1);
                                b1 + ((b0 & (REC_N_FIELDS_ONE_BYTE_MAX as usize)) << 8)
                            } else {
                                b0
                            }
                        }
                        _ => todo!("ERR_PROCESS_VAR_FILED_BYTES: {:?}", e),
                    };
                    DynamicInfo(e.pos, vlen, isnull, e.col_name.clone(), e.column_opx)
                }
            })
            .collect();
        debug!("row_dyn_info={:?}", row_dyn_info);

        Self {
            row_dyn_info,
            table_def: tabdef.clone(),
            nil_area: buf.clone().slice(nilptr - idxdef.nil_area_size..nilptr),
            var_area: buf.clone().slice(varptr..nilptr - idxdef.nil_area_size),
            buf: buf.clone(),
            addr,
        }
    }
}

#[derive(Clone, Derivative, Default)]
#[derivative(Debug)]
pub struct Row {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    /// row data tuple list
    pub row_tuple: Vec<RowDatum>,
    #[derivative(Debug = "ignore")]
    pub table_def: Arc<TableDef>,
}

impl Row {
    pub fn new(addr: usize, buf: Arc<Bytes>, tabdef: Arc<TableDef>) -> Self {
        Self {
            table_def: tabdef,
            buf: buf.clone(),
            addr,
            ..Row::default()
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Record {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub row_info: RowInfo, // row information
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub rec_hdr: RecordHeader, // record header
    pub row_data: Row, // row data
}

impl Record {
    pub fn new(addr: usize, buf: Arc<Bytes>, hdr: RecordHeader, row_info: RowInfo, mut row: Row) -> Self {
        let mut dataptr = addr;
        for x in &row_info.row_dyn_info {
            if x.2 {
                row.row_tuple.push(RowDatum(x.4, 0, None));
            } else {
                let len = x.1;
                let rbuf = buf.slice(dataptr..dataptr + len);
                row.row_tuple.push(RowDatum(x.4, len, Some(rbuf)));
                dataptr += len;
            }
        }

        Self {
            rec_hdr: hdr,
            row_info,
            row_data: row,
            buf: buf.clone(),
            addr,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiRecord {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub rec_hdr: RecordHeader, // record header
    pub sdi_hdr: SdiDataHeader, // SDI Data Header
    pub sdi_str: String,        // SDI Data String, uncompressed string
}

impl SdiRecord {
    pub fn new(addr: usize, buf: Arc<Bytes>, rec_hdr: RecordHeader, hdr: SdiDataHeader) -> Self {
        let beg = addr + SDI_DATA_HEADER_SIZE;
        let comped_data = buf.slice(beg..beg + (hdr.comp_len as usize));
        let uncomped_data = util::zlib_uncomp(comped_data).unwrap();
        assert_eq!(uncomped_data.len(), hdr.uncomp_len as usize);
        Self {
            rec_hdr,
            sdi_hdr: hdr,
            sdi_str: uncomped_data,
            buf: buf.clone(),
            addr,
        }
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiDataHeader {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    /// Length of TYPE field in record of SDI Index.
    pub data_type: u32, // 4 bytes
    /// Length of ID field in record of SDI Index.
    pub data_id: u64, // 8 bytes
    /// trx id
    pub trx_id: u64, // 6 bytes
    /// 7-byte roll-ptr.
    pub roll_ptr: u64, // 7 bytes
    /// Length of UNCOMPRESSED_LEN field in record of SDI Index.
    pub uncomp_len: u32, // 4 bytes
    /// Length of COMPRESSED_LEN field in record of SDI Index.
    pub comp_len: u32, // 4 bytes
}

impl SdiDataHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            data_type: util::u32_val(&buf, addr),
            data_id: util::u64_val(&buf, addr + 4),
            trx_id: util::u48_val(&buf, addr + 12),
            roll_ptr: util::u56_val(&buf, addr + 18),
            uncomp_len: util::u32_val(&buf, addr + 25),
            comp_len: util::u32_val(&buf, addr + 29),
            buf: buf.clone(),
            addr,
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
    pub se_private_data: String,
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
