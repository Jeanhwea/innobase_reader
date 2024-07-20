use std::path::PathBuf;

use clap::{Parser, Subcommand};

mod app;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// input *.ibd file
    #[arg(short, long)]
    pub input: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print basic information
    Info,
    /// List all page data
    List,
    /// List page data with given page_number
    ViewPage {
        #[arg(short, long)]
        page_number: usize,
    },
}

fn main() {
    let args = Args::parse();
    let app = app::App::new(args.input);
    app.exec(args.command);
}
