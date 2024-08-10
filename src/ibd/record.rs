use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::Error;
use bytes::Bytes;
use colored::Colorize;
use derivative::Derivative;
use log::{debug, info};
use num_enum::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use crate::{
    ibd::page::{RECORD_HEADER_SIZE, SDI_DATA_HEADER_SIZE},
    meta::def::TableDef,
    util,
    util::align8,
};

pub const REC_N_FIELDS_ONE_BYTE_MAX: u8 = 0x7f;

/// Record Status, rec.h:152
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

/// Record Info Flag, total 4 bits
#[derive(Debug, Display, EnumString, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecInfoFlag {
    /// Mark current column is minimum record
    MIN_REC,

    /// Mark current column is deleted
    DELETED,

    /// Version flag,
    /// [1](https://blogs.oracle.com/mysql/post/mysql-80-instant-add-and-drop-columns-2)
    VERSION,

    /// Instant Column DDL flag
    ///
    /// WL#11250: Support Instant Add Column,
    /// [1](https://dev.mysql.com/worklog/task/?id=11250)
    ///
    /// INSTANT ADD and DROP Column,
    /// [1](https://blogs.oracle.com/mysql/post/mysql-80-instant-add-drop-columns),
    /// [2](https://blogs.oracle.com/mysql/post/mysql-80-instant-add-and-drop-columns-2)
    INSTANT,
}

/// Record Header
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RecordHeader {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug = "ignore")]
    pub info_byte: u8,
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
            info_byte: b0,
            info_bits: flags,
            n_owned: b0 & 0x0f,
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

    pub fn is_min_rec(&self) -> bool {
        (self.info_byte & Self::REC_INFO_MIN_REC_FLAG) > 0
    }

    pub fn is_deleted(&self) -> bool {
        (self.info_byte & Self::REC_INFO_DELETED_FLAG) > 0
    }

    pub fn is_version(&self) -> bool {
        (self.info_byte & Self::REC_INFO_VERSION_FLAG) > 0
    }

    pub fn is_instant(&self) -> bool {
        (self.info_byte & Self::REC_INFO_INSTANT_FLAG) > 0
    }
}

#[derive(Debug, Clone)]
pub struct IsNull(bool);

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FieldMeta {
    pub opx: usize,    // column opx
    pub null: bool,    // is null value
    pub offset: usize, // page offset
    pub length: usize, // row length
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FieldDatum {
    pub opx: usize,         // column opx
    pub buf: Option<Bytes>, // row length
}

/// Row Dynamic Information, (pos, len, isnull, name)
///   1. pos: index element ordinal position
///   2. len: row data length
///   3. isnull, row data is null
///   4. name: column name
///   5. opx: column ordinal position index (opx)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct DynamicInfo(pub usize, pub usize, pub IsNull, pub String, pub usize);

/// Row Data, (ord, len, buf),
///   1. opx: ordinal_position index
///   2. len: variadic field length
///   3. buf: row data buffer in bytes
///   4. name: column name
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RowDatum(pub usize, pub usize, pub Option<Bytes>, pub String);

/// Row Info, var_area and nil_area
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RowInfo {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address, [nilfld, varfld], access in reverse order
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    // /// Variadic field size area
    // #[derivative(Debug(format_with = "util::fmt_bytes_hex"))]
    // pub var_area: Bytes,
    // /// Nullable field flag area
    // #[derivative(Debug(format_with = "util::fmt_bytes_bin"))]
    // pub nil_area: Bytes,
    /// Row version
    pub row_version: u8, // instant add/drop column

    #[derivative(Debug = "ignore")]
    pub table_def: Arc<TableDef>,
    pub index_pos: usize, // &tabdef.clone().idx_defs[index_pos]
}

impl RowInfo {
    pub fn prepare(&self) -> Result<Vec<FieldMeta>, Error> {
        let row_ver = self.row_version as u32;
        let eles = &self.table_def.clone().idx_defs[self.index_pos].elements;
        let cols = &self.table_def.clone().col_defs;

        let has_phy_pos = cols.iter().any(|col| col.phy_pos >= 0);
        let phy_layout = if has_phy_pos {
            cols.iter()
                .enumerate()
                .map(|(col_pos, col)| {
                    let phy_exist = if col.version_dropped > 0 && row_ver >= col.version_dropped {
                        false
                    } else if col.version_added > 0 && row_ver < col.version_added {
                        false
                    } else {
                        true
                    };
                    (col.phy_pos as usize, (col_pos, phy_exist))
                })
                .collect::<BTreeMap<usize, _>>()
        } else {
            eles.iter()
                .enumerate()
                .map(|(phy_pos, ele)| (phy_pos, (ele.column_opx, true)))
                .collect::<BTreeMap<usize, _>>()
        };

        debug!(
            "row_version={}, has_phy_pos={}, phy_layout={:?}",
            row_ver,
            has_phy_pos.to_string().yellow(),
            &phy_layout,
        );

        // nil field number
        let nfld_num = phy_layout
            .iter()
            .map(|x| {
                let col = &cols[x.1 .0];
                let exist = x.1 .1;
                if exist && col.isnil {
                    1
                } else {
                    0
                }
            })
            .sum();
        let nfld_bytes = align8(nfld_num);

        let niladdr = self.addr + if row_ver > 0 { 1 } else { 0 };
        let varaddr = niladdr + nfld_bytes;
        info!(
            "niladdr={}, varaddr={}",
            niladdr.to_string().yellow(),
            varaddr.to_string().yellow()
        );

        let row_meta_list = Vec::new();
        for (phy_pos, (col_pos, phy_exist)) in phy_layout {
            let col = &cols[col_pos];
            info!(
                "[{}] {} => {}, phy_ver={}, row_version={}, version_added={}, version_dropped={}",
                phy_pos,
                if phy_exist { "Yes".green() } else { "No ".red() },
                col.col_name.magenta(),
                col.phy_pos,
                row_ver,
                col.version_added,
                col.version_dropped,
            );
        }
        Ok(row_meta_list)
    }

    pub fn get_vfld_len(&mut self, data_len: u32, varptr: usize) -> Result<usize, Error> {
        // see function in mysql-server source code
        // static inline uint8_t rec_get_n_fields_length(ulint n_fields) {
        //   return (n_fields > REC_N_FIELDS_ONE_BYTE_MAX ? 2 : 1);
        // }
        let vfld_bytes = if data_len > REC_N_FIELDS_ONE_BYTE_MAX as u32 {
            2
        } else {
            1
        };

        let vlen = match vfld_bytes {
            1 => {
                let b0 = self.buf[varptr - 1] as usize;
                b0
            }
            2 => {
                let b0 = self.buf[varptr - 1] as usize;
                if b0 > REC_N_FIELDS_ONE_BYTE_MAX.into() {
                    let b1 = self.buf[varptr - 2] as usize;
                    // debug!("b0=0x{:0x?}, b1=0x{:0x?}", b0, b1);
                    b1 + ((b0 & (REC_N_FIELDS_ONE_BYTE_MAX as usize)) << 8)
                } else {
                    b0
                }
            }
            _ => panic!("ERR_PROCESS_VAR_FILED_BYTES"),
        };

        Ok(vlen)
    }

    pub fn new(rec_hdr: &RecordHeader, tabdef: Arc<TableDef>, index_pos: usize) -> Self {
        let buf = rec_hdr.buf.clone();

        // Handle the row version
        let row_ver = if rec_hdr.is_version() { buf[rec_hdr.addr - 1] } else { 0 };
        let area_beg = rec_hdr.addr + if rec_hdr.is_version() { 1 } else { 0 };

        // Handle nil_area/var_area pointer
        // let nilptr = area_beg;
        // let mut varptr = area_beg - idxdef.nil_area_size;

        // let cols = &tabdef.clone().col_defs;
        // let has_physical_pos = cols.iter().any(|c| c.phy_pos >= 0);

        // let row_dyn_info = idxdef
        //     .elements
        //     .iter()
        //     .map(|e| {
        //         // debug!("nilptr={}, varptr={}", nilptr, varptr);
        //         let isnull = if e.isnil {
        //             let null_pos = util::numpos(e.null_offset);
        //             let null_mask = 1 << util::numoff(e.null_offset);
        //             let null_flag = buf[nilptr - null_pos - 1];
        //             (null_flag & null_mask) > 0
        //         } else {
        //             false
        //         };

        //         if isnull {
        //             DynamicInfo(e.pos, 0, IsNull(isnull), e.col_name.clone(), e.column_opx)
        //         } else if !e.isvar {
        //             DynamicInfo(
        //                 e.pos,
        //                 e.data_len as usize,
        //                 IsNull(isnull),
        //                 e.col_name.clone(),
        //                 e.column_opx,
        //             )
        //         } else {
        //             // see function in mysql-server source code
        //             // static inline uint8_t rec_get_n_fields_length(ulint n_fields) {
        //             //   return (n_fields > REC_N_FIELDS_ONE_BYTE_MAX ? 2 : 1);
        //             // }
        //             let vfld_bytes = if e.data_len > REC_N_FIELDS_ONE_BYTE_MAX as u32 {
        //                 2
        //             } else {
        //                 1
        //             };

        //             let vlen = match vfld_bytes {
        //                 1 => {
        //                     let b0 = buf[varptr - 1] as usize;
        //                     varptr -= 1;
        //                     b0
        //                 }
        //                 2 => {
        //                     let b0 = buf[varptr - 1] as usize;
        //                     varptr -= 1;

        //                     if b0 > REC_N_FIELDS_ONE_BYTE_MAX.into() {
        //                         let b1 = buf[varptr - 1] as usize;
        //                         varptr -= 1;
        //                         // debug!("b0=0x{:0x?}, b1=0x{:0x?}", b0, b1);
        //                         b1 + ((b0 & (REC_N_FIELDS_ONE_BYTE_MAX as usize)) << 8)
        //                     } else {
        //                         b0
        //                     }
        //                 }
        //                 _ => todo!("ERR_PROCESS_VAR_FILED_BYTES: {:?}", e),
        //             };
        //             DynamicInfo(e.pos, vlen, IsNull(isnull), e.col_name.clone(), e.column_opx)
        //         }
        //     })
        //     .collect();
        // debug!("row_dyn_info={:?}", row_dyn_info);

        Self {
            table_def: tabdef.clone(),
            index_pos,
            row_version: row_ver,
            buf: buf.clone(),
            addr: rec_hdr.addr,
        }
    }
}

/// Row data
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RowData {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    /// Row Info
    pub row_info: Arc<RowInfo>,
    /// Row metadata list
    pub meta_list: Vec<FieldMeta>,
    /// Row data list
    pub data_list: Vec<RowDatum>,
}

impl RowData {
    pub fn new(addr: usize, buf: Arc<Bytes>, row_info: Arc<RowInfo>) -> Self {
        let field_meta_list = row_info.prepare().unwrap();
        let field_data_list = Vec::new();
        Self {
            buf: buf.clone(),
            addr,
            row_info: row_info.clone(),
            meta_list: field_meta_list,
            data_list: field_data_list,
        }
    }
}

/// Record
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Record {
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize, // page address
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>, // page data buffer

    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub row_info: Arc<RowInfo>, // row information
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub rec_hdr: RecordHeader, // record header
    pub row_data: RowData, // row data
}

impl Record {
    pub fn new(addr: usize, buf: Arc<Bytes>, hdr: RecordHeader, row_info: Arc<RowInfo>, row: RowData) -> Self {
        // for e in &row.dyn_info {
        //     let mut rlen = 0; // row length
        //     let mut rbuf = None; // row buffer
        //
        //     // Common parse columns
        //     let col = &tabdef.col_defs[e.4];
        //     let isnull = e.2 .0;
        //     if !isnull {
        //         rlen = e.1;
        //         rbuf = Some(buf.slice(dataptr..dataptr + rlen));
        //         dataptr += rlen;
        //     }
        //
        //     // Ignore all the columns value with VERSION_DROPPED > 0
        //     if col.version_dropped > 0 {
        //         continue;
        //     }
        //
        //     // Use default value for columns with VERSION_ADDED > ROW_VERSION
        //     if col.version_added > row_info.row_version as u32 {
        //         rbuf = col.defval.clone();
        //         rlen = match &rbuf {
        //             None => 0,
        //             Some(b) => b.len(),
        //         }
        //     }
        //
        //     row.data_list.push(RowDatum(e.4, rlen, rbuf, col.col_name.clone()));
        // }

        Self {
            rec_hdr: hdr,
            row_info: row_info.clone(),
            row_data: row,
            buf: buf.clone(),
            addr,
        }
    }
}

/// SDI Record
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

/// SDI Data Header
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

/// SDI Object
#[derive(Debug, Deserialize, Serialize)]
pub struct SdiObject {
    pub dd_object: DataDictObject,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// Data Dictionary Object
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

/// Column Type, see sql/dd/types/column.h, enum class enum_column_types
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

/// see sql/dd/types/column.h, enum class enum_hidden_type
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

/// see sql/dd/types/column.h, enum class enum_column_type
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

/// see sql/dd/impl/types/column_impl.h, class Column_impl : public Entity_object_impl, public Column {
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
    pub se_private_data: String,
    pub elements: Vec<DataDictColumnElement>,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// Data Dictionary Column Elements
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DataDictColumnElement {
    pub index: u32,
    pub name: String,
}

/// see sql/dd/types/index.h, enum class enum_index_type
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

/// see sql/dd/types/index.h, enum class enum_index_algorithm
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

/// see sql/dd/types/index.h, enum class enum_index_algorithm
#[repr(u8)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive, Debug, Display, Default, Clone, Copy)]
pub enum IndexOrder {
    #[default]
    ORDER_UNDEF = 1,
    ORDER_ASC = 2,
    ORDER_DESC = 3,
}

/// see sql/dd/impl/types/index_impl.h, class Index_impl : public Entity_object_impl, public Index {
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

/// Data Dictionary Index Elements
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DataDictIndexElement {
    pub ordinal_position: u32,
    pub length: u32,
    pub order: IndexOrder,
    pub hidden: bool,
    pub column_opx: u32,
}
