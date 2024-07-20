use std::path::PathBuf;

use crate::Commands;
use anyhow::Result;
use log::info;

#[derive(Debug, Default)]
pub struct App {
    pub input: PathBuf,
}

impl App {
    pub fn new(input: PathBuf) -> Self {
        Self {
            input,
            ..Self::default()
        }
    }

    pub fn exec(&self, command: Commands) -> Result<()> {
        info!("{:?}, {:?}", command, self);
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
        let app = App::new(in1);
        assert!(app.exec(Commands::Info).is_ok());
    }
}
