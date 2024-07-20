use std::path::PathBuf;

use crate::Commands;

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

    pub fn exec(&self, command: Commands) {
        println!("{:?}, {:#?}", command, self);
    }
}
