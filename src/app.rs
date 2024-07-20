use std::path::PathBuf;

pub enum Action {
    List,
}

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

    pub fn exec(&self, action: Action) {
        println!("{:#?}", self);
    }
}
