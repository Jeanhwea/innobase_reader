use std::{collections::HashMap, fmt::Debug};

use num_enum::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use crate::meta::def::HiddenTypes;

#[repr(u32)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum EntryTypes {
    #[default]
    Unknown,

    Table = 1,

    Tablespace = 2,
}

/// SDI Entry
#[derive(Debug, Deserialize, Serialize)]
pub struct SdiEntry {
    #[serde(rename = "type")]
    pub entry_type: EntryTypes,

    #[serde(rename = "id")]
    pub entry_id: u64,

    #[serde(rename = "object")]
    pub entry_object: DataDictObjectTypes,
}

impl SdiEntry {
    pub fn form_str(s: &str) -> Vec<Self> {
        serde_json::from_str::<Vec<SdiEntry>>(s).expect("ERR_SDI_ENTRY_LIST")
    }
}

/// Entry Object Enums
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "dd_object_type")]
pub enum DataDictObjectTypes {
    Table(SdiTableObject),
    Tablespace(SdiTablespaceObject),
}

/// SDI Tablespace Object
#[derive(Debug, Deserialize, Serialize)]
pub struct SdiTablespaceObject {
    pub mysqld_version_id: u32,
    pub dd_version: u32,
    pub sdi_version: u32,
    pub dd_object: TablespaceDataDictObject,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// Data Dictionary Object
#[derive(Debug, Deserialize, Serialize)]
pub struct TablespaceDataDictObject {
    pub name: String,
    pub comment: String,
    pub options: String,
    pub se_private_data: String,
    pub engine: String,
    pub engine_attribute: String,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// SDI Table Object
#[derive(Debug, Deserialize, Serialize)]
pub struct SdiTableObject {
    pub mysqld_version_id: u32,
    pub dd_version: u32,
    pub sdi_version: u32,
    pub dd_object: TableDataDictObject,
    #[serde(flatten)]
    extra: HashMap<String, Value>,
}

/// Data Dictionary Object
#[derive(Debug, Deserialize, Serialize)]
pub struct TableDataDictObject {
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

#[cfg(test)]
mod sdi_tests_run {
    use std::{fs, path::PathBuf};

    use anyhow::Result;

    use crate::{sdi::record::SdiEntry, util};

    const SDI_01: &str = "data/employees_sdi.json";

    #[test]
    fn parse_sdi_info_00() -> Result<()> {
        util::init_unit_test();
        let text = fs::read_to_string(PathBuf::from(SDI_01))?;
        let objs = SdiEntry::form_str(&text);
        dbg!(&objs);
        Ok(())
    }
}
