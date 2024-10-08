#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(clippy::upper_case_acronyms)]
// #![allow(unused_imports)]

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use ibd::redo::LogRecordTypes;
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
    /// Input innodb datafile. for example departments.ibd
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
        /// List index data
        #[arg(short, long, default_value_t = false)]
        index: bool,

        /// List segment data
        #[arg(short, long, default_value_t = false)]
        segment: bool,

        /// List extent data
        #[arg(short, long, default_value_t = false)]
        extent: bool,

        /// List page data
        #[arg(short, long, default_value_t = false)]
        page: bool,

        /// List all: index, segment, extent, page, ...
        #[arg(short, long, default_value_t = false)]
        all: bool,

        /// Limit the total data rows
        #[arg(short, long, default_value_t = 65535)]
        limit: usize,
    },

    /// Describe datafile information by sdi page
    Desc,

    /// Print SDI json
    Sdi {
        /// Print parsed table definition
        #[arg(short, long, default_value_t = false)]
        table_define: bool,
        /// Print index root segements
        #[arg(short, long, default_value_t = false)]
        root_segments: bool,
    },

    /// View page data with given page_no.
    View {
        /// The page number, starts from 0.
        page_no: usize,
    },

    /// Dump index page user records
    Dump {
        /// The page number, starts from 0.
        page_no: Option<usize>,

        /// Limit the total row in the dump
        #[arg(short, long, default_value_t = 10)]
        limit: usize,

        /// Dump the garbage list
        #[arg(short, long, default_value_t = false)]
        garbage: bool,

        /// Print more information
        #[arg(short, long, default_value_t = false)]
        verbose: bool,

        /// Dump the B+ tree root
        #[arg(short, long)]
        btree_root: Option<usize>,
    },

    /// Redo log print
    Redo {
        /// The block number, starts from 0.
        block_no: Option<usize>,

        /// Dump given log_type redo blocks, log_type like MLOG_xxx, MLOG_1BYTE,
        /// MLOG_REC_INSERT ...
        #[arg(short, long)]
        dump_log_type: Option<LogRecordTypes>,
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
