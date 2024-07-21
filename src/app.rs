use std::collections::{BTreeMap, HashMap};

use colored::Colorize;
use std::path::PathBuf;

use crate::ibd::factory::PageFactory;
use crate::ibd::page::{BasePage, FileSpaceHeaderPage, PageTypes};
use crate::ibd::tabspace::Tablespace;
use crate::Commands;
use anyhow::{Error, Result};
use log::{debug, error, info, warn};

#[derive(Debug, Default)]
pub struct App {
    pub input: PathBuf,
    pub tablespace: Option<Tablespace>,
}

impl App {
    pub fn new(input: PathBuf) -> Self {
        Self {
            input,
            ..Self::default()
        }
    }

    fn init(&mut self) -> Result<()> {
        let mut ts = Tablespace::new(self.input.clone());
        ts.open()?;
        self.tablespace = Some(ts);
        Ok(())
    }

    pub fn run(&mut self, command: Commands) -> Result<()> {
        debug!("{:?}, {:?}", command, self);
        self.init()?;
        if let Some(ref mut ts) = self.tablespace {
            match command {
                Commands::Info => Self::do_info(ts)?,
                Commands::List => Self::do_list(ts)?,
                Commands::View { page: page_no } => Self::do_view(ts, page_no)?,
            }
        }
        Ok(())
    }

    fn do_info(ts: &Tablespace) -> Result<()> {
        let mut stats: BTreeMap<PageTypes, u32> = BTreeMap::new();
        for page_no in 0..ts.page_count() {
            let fil_hdr = ts.parse_fil_hdr(page_no)?;
            *stats.entry(fil_hdr.page_type).or_insert(0) += 1;
        }
        info!("stat: {:#?}", stats);
        Ok(())
    }

    fn do_list(ts: &Tablespace) -> Result<()> {
        for page_no in 0..ts.page_count() {
            let fil_hdr = ts.parse_fil_hdr(page_no)?;
            let pt = &fil_hdr.page_type;
            match pt {
                PageTypes::ALLOCATED => {}
                PageTypes::MARKED(_) => {
                    warn!("{:?} page_no = {}", pt, page_no);
                }
                _ => {
                    println!(
                        "space_id={}, page_no={} => {} ",
                        fil_hdr.space_id.to_string().blue(),
                        &page_no.to_string().magenta(),
                        pt.to_string().yellow(),
                    );
                    info!("fil_hdr = {:?}", fil_hdr);
                }
            }
        }
        Ok(())
    }

    fn do_view(ts: &Tablespace, page_no: usize) -> Result<(), Error> {
        if page_no >= ts.page_count() {
            return Err(Error::msg("Page number out of range"));
        }
        let factory = ts.init_page_factory(page_no)?;
        let hdr = factory.fil_hdr();
        match hdr.page_type {
            PageTypes::ALLOCATED => {
                info!("allocated only page, hdr = {:#?}", hdr);
            }
            PageTypes::FSP_HDR => {
                assert_eq!(page_no, hdr.page_no as usize);
                let fsp_page: BasePage<FileSpaceHeaderPage> = factory.build();
                info!("fsp_page = {:#?}", fsp_page);
            }
            PageTypes::MARKED(_) => {
                warn!("page_no = {}, hdr = {:?}", page_no, hdr);
            }
            _ => {
                error!("unsupported page type, hdr = {:#?}", hdr);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::util;
    use std::env::set_var;

    static mut INITIALIZED: bool = false;

    fn init() {
        unsafe {
            if INITIALIZED {
                return;
            }
            INITIALIZED = true;
        }
        set_var("RUST_LOG", "info");
        util::init_logger();
    }

    #[test]
    fn it_works() {
        init();
        let f = PathBuf::from("data/departments.ibd");
        let mut app = App::new(f);
        assert!(app.run(Commands::View { page: 0 }).is_ok());
    }

    #[test]
    fn list_pages() {
        init();
        let f = PathBuf::from("data/departments.ibd");
        let mut app = App::new(f);
        assert!(app.run(Commands::List).is_ok());
    }
}
