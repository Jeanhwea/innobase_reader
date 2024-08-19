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
    /// Input innodb data file. for example departments.ibd
    input: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print basic information.
    Info,
    /// List all page. page_type, page_number and more
    List {
        // list the index segments
        #[arg(short, long, default_value_t = false)]
        indexes: bool,
        // list the segments
        #[arg(short, long, default_value_t = false)]
        segments: bool,
        // list the extents
        #[arg(short, long, default_value_t = false)]
        extents: bool,
        // list the pages
        #[arg(short, long, default_value_t = false)]
        pages: bool,
        // list all map
        #[arg(short, long, default_value_t = false)]
        all: bool,
    },
    /// Describe Datafile Information by SDI page
    Desc,
    /// Print SDI Json
    Sdi {
        /// print parsed table definition
        #[arg(short, long, default_value_t = false)]
        table_define: bool,
        /// print index root segements
        #[arg(short, long, default_value_t = false)]
        root_segments: bool,
    },
    /// View page data with given page_no.
    View {
        /// Page number, starts from 0.
        page_no: usize,
    },
    /// Dump Index Page User Records
    Dump {
        /// Page number, starts from 0.
        page_no: usize,
        /// Limit the total row in the dump
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
        /// Dump the garbage list
        #[arg(short, long, default_value_t = false)]
        garbage: bool,
        /// Print more information
        #[arg(short, long, default_value_t = false)]
        verbose: bool,
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
