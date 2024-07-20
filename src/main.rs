#![warn(unused_imports)]

use std::{path::PathBuf, time::Instant};

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
    #[arg(short, long)]
    pub input: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Print basic information.
    Info,
    /// List all page. page_type, page_number and more
    List,
    /// View page data with given page_number.
    View {
        #[arg(short, long)]
        page_number: usize,
    },
}

fn main() -> Result<()> {
    util::init_logger();

    let args = Args::parse();
    let mut app = app::App::new(args.input);

    let start = Instant::now();
    app.exec(args.command)?;

    info!("done in {:?}", start.elapsed());
    Ok(())
}
