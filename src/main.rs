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
mod sdi;
mod util;

#[derive(Debug, Parser)]
#[command(author, version, about = "The innobase datafile(*.ibd) reader", long_about = None)]
pub struct Args {
    /// input innodb datafile. for example departments.ibd
    input: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// print basic information.
    Info,
    /// list all page. page_type, page_number and more
    List {
        /// list index data
        #[arg(short, long, default_value_t = false)]
        index: bool,
        /// list segment data
        #[arg(short, long, default_value_t = false)]
        segment: bool,
        /// list extent data
        #[arg(short, long, default_value_t = false)]
        extent: bool,
        /// list page data
        #[arg(short, long, default_value_t = false)]
        page: bool,
        /// list all: index, segment, extent, page, ...
        #[arg(short, long, default_value_t = false)]
        all: bool,
    },
    /// describe datafile information by sdi page
    Desc,
    /// print sdi json
    Sdi {
        /// print parsed table definition
        #[arg(short, long, default_value_t = false)]
        table_define: bool,
        /// print index root segements
        #[arg(short, long, default_value_t = false)]
        root_segments: bool,
    },
    /// view page data with given page_no.
    View {
        /// page number, starts from 0.
        page_no: usize,
    },
    /// dump index page user records
    Dump {
        /// page number, starts from 0.
        page_no: Option<usize>,
        /// limit the total row in the dump
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
        /// dump the garbage list
        #[arg(short, long, default_value_t = false)]
        garbage: bool,
        /// print more information
        #[arg(short, long, default_value_t = false)]
        verbose: bool,
        /// dump the b-tree root
        #[arg(short, long)]
        btree_root: Option<usize>,
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
