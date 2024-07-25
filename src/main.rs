#![allow(dead_code)]
// #![allow(unused_imports)]

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use log::info;

mod app;
mod ibd;
mod util;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// input *.ibd file
    input: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print basic information.
    Info,
    /// List all page. page_type, page_number and more
    List,
    /// Describe Datafile Information by SDI page
    Desc,
    /// Print SDI Json
    Sdi,
    /// Dump Index Page User Records
    Dump {
        /// Which page number, which starts from zero. [0, 1, ...]
        #[arg(short, long)]
        page: usize,
    },
    /// View page data with given page_number.
    View {
        /// Which page number, which starts from zero. [0, 1, ...]
        #[arg(short, long)]
        page: usize,
    },
}

fn main() -> Result<()> {
    util::init();

    let args = Args::parse();
    let mut app = app::App::new(args.input);

    app.run(args.command)?;

    info!("time costs {:?}", app.time_costs());
    Ok(())
}
