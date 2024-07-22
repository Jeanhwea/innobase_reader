use super::page::FilePageHeader;

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
