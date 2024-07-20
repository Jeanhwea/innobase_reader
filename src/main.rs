use std::path::PathBuf;

use clap::Parser;

mod app;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    pub input: PathBuf,
}

fn main() {
    let args = Args::parse();
    let app = app::App::new(args.input);
    app.exec(app::Action::List);
}
