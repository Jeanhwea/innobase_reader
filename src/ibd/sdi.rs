use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use derivative::Derivative;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::record::RecordHeader;
use crate::{meta::def::HiddenTypes, util};

// sdi
pub const SDI_DATA_HEADER_SIZE: usize = 33;

/// SDI Record
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiRecord {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

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
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) Length of TYPE field in record of SDI Index.
    pub data_type: u32,
    /// (8 bytes) Data ID
    pub data_id: u64,
    /// (6 bytes) Transaction ID
    pub trx_id: u64,
    /// (7 bytes) Rollback pointer
    pub roll_ptr: u64,
    /// (4 bytes) Length of UNCOMPRESSED_LEN field in record of SDI Index
    pub uncomp_len: u32,
    /// (4 bytes) Length of COMPRESSED_LEN field in record of SDI Index
    pub comp_len: u32,
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
    pub row_format: i8,
    pub columns: Vec<DataDictColumn>,
    pub indexes: Vec<DataDictIndex>,
    pub se_private_data: String,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// see sql/dd/impl/types/column_impl.h, class Column_impl : public Entity_object_impl, public Column {
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DataDictColumn {
    pub ordinal_position: u32,
    #[serde(rename = "name")]
    pub col_name: String,
    #[serde(rename = "type")]
    pub dd_type: u8,
    pub is_nullable: bool,
    pub is_zerofill: bool,
    pub is_unsigned: bool,
    pub is_auto_increment: bool,
    pub is_virtual: bool,
    pub hidden: HiddenTypes,
    pub char_length: u32,
    pub comment: String,
    pub collation_id: u32,
    pub column_key: u8,
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

/// see sql/dd/impl/types/index_impl.h, class Index_impl : public Entity_object_impl, public Index {
#[derive(Debug, Deserialize, Serialize)]
pub struct DataDictIndex {
    pub ordinal_position: u32,
    pub name: String,
    pub hidden: bool,
    pub comment: String,
    #[serde(rename = "type")]
    pub idx_type: u8,
    pub algorithm: u8,
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
    pub order: u8,
    pub hidden: bool,
    pub column_opx: u32,
}
