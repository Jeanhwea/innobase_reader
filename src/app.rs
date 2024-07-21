use std::collections::{BTreeMap, HashMap};

use std::path::PathBuf;

use crate::ibd::page::PageTypes;
use crate::ibd::tabspace::Tablespace;
use crate::Commands;
use anyhow::Result;
use log::{info, warn};

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
        info!("{:?}, {:?}", command, self);
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
                PageTypes::TYPE_ALLOCATED => {}
                PageTypes::Unknown(_) => {
                    warn!("{:?} page_no = {}", pt, page_no);
                }
                _ => {
                    info!("fil_hdr = {:?}", fil_hdr);
                }
            }
        }
        Ok(())
    }

    fn do_view(ts: &Tablespace, page_no: usize) -> Result<()> {
        info!("page_no = {}", page_no);
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::util;
    use std::env::set_var;

    fn init() {
        set_var("RUST_LOG", "info");
        util::init_logger();
    }

    #[test]
    fn it_works() {
        init();
        let in1 = PathBuf::from("data/departments.ibd");
        let mut app = App::new(in1);
        assert!(app.run(Commands::List).is_ok());
    }
}
