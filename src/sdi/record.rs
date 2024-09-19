use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::meta::def::HiddenTypes;

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
