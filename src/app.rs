use std::path::PathBuf;

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
            let fsp_hdr_page = ts.read_fsp_hdr_page()?;
            info!("FSP_HDR = {:#?}", fsp_hdr_page);
            // info!("check_sum = {:#x}", fsp_hdr_page.fil_hdr.check_sum);
            // info!("space_id = {:?}", fsp_hdr_page.data.fsp_hdr.space_id);
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
        assert!(app.run(Commands::Info).is_ok());
    }
}
