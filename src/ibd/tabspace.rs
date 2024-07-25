use super::{page::FilePageHeader, record::ColumnTypes};

#[derive(Debug, Clone)]
pub struct Datafile {
    pub server_version: u32, // on page 0, FIL_PAGE_SRV_VERSION
    pub space_version: u32,  // on page 0, FIL_PAGE_SPACE_VERSION
    pub space_id: u32,       // Space Id
}

impl Datafile {
    pub fn new(fil_hdr: FilePageHeader) -> Self {
        Self {
            server_version: fil_hdr.prev_page,
            space_version: fil_hdr.next_page,
            space_id: fil_hdr.space_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableDef {
    pub tab_name: String,                      // table name
    pub varfield_size: usize,                  // variadic field size
    pub nullflag_size: usize,                  // nullable flag size
    pub vfldinfo: Vec<(usize, String, usize)>, // variadic field info (ord, name, size)
    pub nullinfo: Vec<(usize, String)>,        // nullable flag info  (ord, name)
    pub col_defs: Vec<ColumnDef>,              // column infomation
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub ord_pos: u32,         // ordinal position
    pub col_name: String,     // column name
    pub data_len: u32,        // data lenght in bytes
    pub is_nullable: bool,    // is nullable
    pub is_varfield: bool,    // is variadic field
    pub dd_type: ColumnTypes, // data dictionary type
    pub utf8_def: String,     // utf8 column definition
}
