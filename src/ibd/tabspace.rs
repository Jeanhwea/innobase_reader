#[derive(Debug)]
pub struct Datafile {
    pub server_version: u32, // on page 0, FIL_PAGE_SRV_VERSION
    pub space_version: u32,  // on page 0, FIL_PAGE_SPACE_VERSION
}
