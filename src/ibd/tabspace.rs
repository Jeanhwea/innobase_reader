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
    pub tab_name: String,
    pub col_defs: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub ord_pos: u32,
    pub col_name: String,
    pub byte_len: u32,
    pub is_nullable: bool,
    pub is_varlen: bool,
    pub dd_type: ColumnTypes,
    pub utf8_type: String,
}
