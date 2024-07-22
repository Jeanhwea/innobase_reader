use std::collections::{BTreeMap};

use colored::Colorize;
use std::path::PathBuf;

use crate::ibd::factory::DatafileFactory;
use crate::ibd::page::{BasePage, FileSpaceHeaderPage, PageTypes};
use crate::Commands;
use anyhow::{Error, Result};
use log::{debug, error, info, warn};

#[derive(Debug, Default)]
pub struct App {
    pub input: PathBuf,
    pub datafile: Option<DatafileFactory>,
}

impl App {
    pub fn new(input: PathBuf) -> Self {
        Self {
            input,
            ..Self::default()
        }
    }

    fn init(&mut self) -> Result<()> {
        let mut df = DatafileFactory::new(self.input.clone());
        df.open()?;
        self.datafile = Some(df);
        Ok(())
    }

    pub fn run(&mut self, command: Commands) -> Result<()> {
        debug!("{:?}, {:?}", command, self);
        self.init()?;
        if let Some(ref mut df) = self.datafile {
            match command {
                Commands::Info => Self::do_info(df)?,
                Commands::List => Self::do_list(df)?,
                Commands::View { page: page_no } => Self::do_view(df, page_no)?,
            }
        }
        Ok(())
    }

    fn do_info(df: &DatafileFactory) -> Result<()> {
        let mut stats: BTreeMap<PageTypes, u32> = BTreeMap::new();
        for page_no in 0..df.page_count() {
            let fil_hdr = df.parse_fil_hdr(page_no)?;
            *stats.entry(fil_hdr.page_type).or_insert(0) += 1;
        }

        println!("PageTypes Statistics:");
        for e in &stats {
            println!(
                "{:>12} => {}",
                e.0.to_string().yellow(),
                e.1.to_string().blue()
            );
        }
        Ok(())
    }

    fn do_list(df: &DatafileFactory) -> Result<()> {
        for page_no in 0..df.page_count() {
            let fil_hdr = df.parse_fil_hdr(page_no)?;
            let pt = &fil_hdr.page_type;
            println!(
                "space_id={}, page_no={} => {} ",
                fil_hdr.space_id.to_string().blue(),
                &page_no.to_string().magenta(),
                pt.to_string().yellow(),
            );
            match pt {
                PageTypes::ALLOCATED => {}
                PageTypes::MARKED(_) => {
                    warn!("{:?} page_no = {}", pt, page_no);
                }
                _ => {
                    info!("fil_hdr = {:?}", fil_hdr);
                }
            }
        }
        Ok(())
    }

    fn do_view(df: &DatafileFactory, page_no: usize) -> Result<(), Error> {
        if page_no >= df.page_count() {
            return Err(Error::msg("Page number out of range"));
        }
        let factory = df.init_page_factory(page_no)?;
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
