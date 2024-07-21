use std::collections::HashMap;

use std::path::PathBuf;

use crate::ibd::page::PageTypes;
use crate::ibd::tabspace::Tablespace;
use crate::Commands;
use anyhow::Result;
use log::info;

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
                Commands::Info => {
                    let mut stats: HashMap<PageTypes, u32> = HashMap::new();
                    for page_no in 0..ts.page_count() {
                        let fil_hdr = ts.parse_fil_hdr(page_no)?;
                        *stats.entry(fil_hdr.page_type).or_insert(0) += 1;
                    }
                    info!("stat: {:#?}", stats);
                }
                Commands::List => {
                    for page_no in 0..ts.page_count() {
                        let fil_hdr = ts.parse_fil_hdr(page_no)?;
                        match fil_hdr.page_type {
                            PageTypes::Unknown(_) => {
                                // info!("fil_hdr = {:?}", fil_hdr);
                            }
                            _ => {
                                info!("fil_hdr = {:?}", fil_hdr);
                            }
                        }
                    }
                }
                Commands::View {
                    page_number: _page_no,
                } => {
                    info!("xxx");
                }
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
