#![allow(dead_code)]
// #![allow(unused_imports)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::info;
use std::path::PathBuf;

mod app;
mod ibd;
mod util;

#[derive(Debug, Parser)]
#[command(author, version, about = "The innobase datafile(*.ibd) reader", long_about = None)]
pub struct Args {
    /// Input data file. such as *.ibd
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
        page: usize,
        /// Limit the total row in the dump
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
    },
    /// View page data with given page_number.
    View {
        /// Which page number, which starts from zero. [0, 1, ...]
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
