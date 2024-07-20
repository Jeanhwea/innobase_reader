use std::io::Read;

use std::path::PathBuf;

use crate::ibd::Tablespace;

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

    pub fn exec(&mut self, command: Commands) -> Result<()> {
        info!("{:?}, {:?}", command, self);
        self.init()?;
        if let Some(ref mut ts) = self.tablespace {
            let p0 = ts.read(0)?;
            info!("{:?}", p0.len());
            let p1 = ts.read(1)?;
            info!("{:?}", p1.len());
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
        assert!(app.exec(Commands::Info).is_ok());
    }
}
