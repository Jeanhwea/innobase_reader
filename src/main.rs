#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(clippy::upper_case_acronyms)]
// #![allow(unused_imports)]

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::info;

mod app;
mod factory;
mod ibd;
mod meta;
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
        #[arg(short, long, default_value_t = 9)]
        limit: usize,
        /// Dump the garbage list
        #[arg(short, long, default_value_t = false)]
        garbage: bool,
        /// Print more information
        #[arg(short, long, default_value_t = false)]
        verbose: bool,
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
