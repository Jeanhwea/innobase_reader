use crate::factory::{DatafileFactory, PageFactory, SDI_META_INFO_MIN_VER};
use crate::ibd::page::{BasePage, FileSpaceHeaderPageBody, INodePageBody, IndexPageBody, PageTypes, SdiPageBody};
use crate::Commands;
use anyhow::{Error, Result};
use colored::Colorize;
use log::{debug, error, info};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct App {
    pub timer: Instant,
    pub factory: DatafileFactory,
}

impl App {
    pub fn new(input: PathBuf) -> Self {
        Self {
            timer: Instant::now(),
            factory: DatafileFactory::new(input),
        }
    }

    pub fn time_costs(&self) -> Duration {
        self.timer.elapsed()
    }

    pub fn run(&mut self, command: Commands) -> Result<()> {
        debug!("{:?}, {:?}", command, self);
        self.factory.init()?;

        match command {
            Commands::Info => self.do_info()?,
            Commands::List => self.do_list()?,
            Commands::Desc => self.do_desc()?,
            Commands::Sdi => self.do_pretty_print_sdi_json()?,
            Commands::Dump { page: page_no, limit } => self.do_dump(page_no, limit)?,
            Commands::View { page: page_no } => self.do_view(page_no)?,
        }

        Ok(())
    }

    fn do_info(&self) -> Result<()> {
        let df_fact = &self.factory;
        let mut stats: BTreeMap<PageTypes, u32> = BTreeMap::new();
        for page_no in 0..df_fact.page_count() {
            let hdr = df_fact.parse_fil_hdr(page_no)?;
            *stats.entry(hdr.page_type).or_insert(0) += 1;
        }

        println!("Meta Information:");
        println!(
            "{:>12} => server({}), space({})",
            "version".green(),
            &df_fact.server_version.to_string().blue(),
            &df_fact.space_version.to_string().blue()
        );
        println!("{:>12} => {}", "space_id".green(), &df_fact.space_id.to_string().blue());
        println!(
            "{:>12} => {}",
            "page_count".green(),
            &df_fact.page_count().to_string().blue()
        );
        println!(
            "{:>12} => {}",
            "file_size".green(),
            df_fact.file_size().to_string().blue()
        );

        println!("PageTypes Statistics:");
        for entry in &stats {
            println!("{:>12} => {}", entry.0.to_string().yellow(), entry.1.to_string().blue());
        }
        Ok(())
    }

    fn do_list(&self) -> Result<()> {
        let df_fact = &self.factory;
        for page_no in 0..df_fact.page_count() {
            let fil_hdr = df_fact.parse_fil_hdr(page_no)?;
            let page_type = &fil_hdr.page_type;
            println!(
                "page_no={}, page_type={}, space_id={}, lsn={}",
                &page_no.to_string().magenta(),
                &page_type.to_string().yellow(),
                &fil_hdr.space_id.to_string().blue(),
                &fil_hdr.lsn.to_string().green(),
            );
        }
        Ok(())
    }

    fn do_desc(&mut self) -> Result<()> {
        let df_fact = &mut self.factory;
        let mgr = df_fact.init_meta_mgr()?;

        let tabdef = mgr.load_tabdef()?;

        for c in &tabdef.col_defs {
            println!(
                "COL{}: name={}, dd_type={}, nullable={}, data_len={}, utf8_def={}",
                c.pos,
                c.col_name.magenta(),
                c.dd_type.to_string().blue(),
                c.isnil.to_string().yellow(),
                c.data_len.to_string().cyan(),
                c.utf8_def.green(),
            );
            info!("{:?}", c);
        }

        for i in &tabdef.idx_defs {
            println!(
                "IDX{}: idx_name={}, idx_type={}, algorithm={}",
                i.pos,
                i.idx_name.magenta(),
                i.idx_type.to_string().blue(),
                i.algorithm.to_string().cyan(),
            );
            for e in &i.elements {
                let ref_col = &tabdef.col_defs[e.column_opx];
                println!(
                    " ({}-{}): column_opx={}, col_name={}, order={}, ele_len={}, hidden={}, isnil={}, isvar={}",
                    i.pos,
                    e.pos,
                    e.column_opx.to_string().green(),
                    ref_col.col_name.magenta(),
                    e.order.to_string().yellow(),
                    e.ele_len.to_string().blue(),
                    e.hidden.to_string().magenta(),
                    ref_col.isnil.to_string().red(),
                    ref_col.isvar.to_string().cyan(),
                );
            }
            info!("{:?}", i);
        }

        Ok(())
    }

    fn do_pretty_print_sdi_json(&self) -> Result<()> {
        let df_fact = &self.factory;
        let mgr = df_fact.init_meta_mgr()?;
        let json_str = mgr.raw_sdi_str().unwrap();
        let sdi_data = jsonxf::pretty_print(&json_str).unwrap();
        println!("{}", sdi_data);
        Ok(())
    }

    fn do_dump(&mut self, page_no: usize, limit: usize) -> Result<(), Error> {
        let df_fact = &mut self.factory;
        if page_no >= df_fact.page_count() {
            return Err(Error::msg("Page number out of range"));
        }

        let fil_hdr = df_fact.parse_fil_hdr(page_no)?;
        if fil_hdr.page_type != PageTypes::INDEX {
            return Err(Error::msg(format!(
                "Only support dump INDEX page, but found {:?}",
                fil_hdr.page_type
            )));
        }
        let buffer = df_fact.read_page(page_no)?;
        let mut index_page: BasePage<IndexPageBody> = PageFactory::new(buffer, page_no).parse();

        let mgr = df_fact.init_meta_mgr()?;
        let tabdef = Arc::new(mgr.load_tabdef()?);
        info!("tabdef = {:?}", &tabdef);

        index_page.page_body.parse_records(tabdef.clone())?;

        for (cur, urec) in index_page.page_body.records().iter().enumerate() {
            if cur >= limit {
                break;
            }
            let seq = cur + 1;
            info!(
                "seq={}, addr=@{}, {}={:?}, {}={:?}, {}={:?}",
                seq.to_string().red(),
                &urec.row_data.addr.to_string().yellow(),
                "hdr".cyan(),
                &urec.rec_hdr,
                "data".green(),
                &urec.row_data,
                "info".magenta(),
                &urec.row_info,
            );
            println!(
                "****************************** Row {} ******************************",
                seq.to_string().yellow()
            );
            for row in &urec.row_data.row_tuple {
                let col = &tabdef.clone().col_defs[row.0];
                println!("{:>12} => {:?}", &col.col_name.magenta(), row.2);
            }
        }

        Ok(())
    }

    fn do_view(&self, page_no: usize) -> Result<(), Error> {
        let df_fact = &self.factory;
        if page_no >= df_fact.page_count() {
            return Err(Error::msg("Page number out of range"));
        }

        let buffer = df_fact.read_page(page_no)?;
        let pg_fact = PageFactory::new(buffer, page_no);
        let fil_hdr = pg_fact.fil_hdr();
        match fil_hdr.page_type {
            PageTypes::ALLOCATED => {
                println!("allocated only page, fil_hdr = {:#?}", fil_hdr);
            }
            PageTypes::FSP_HDR => {
                assert_eq!(page_no, fil_hdr.page_no as usize);
                let mut fsp_page: BasePage<FileSpaceHeaderPageBody> = pg_fact.parse();
                if df_fact.server_version > SDI_META_INFO_MIN_VER {
                    fsp_page.page_body.parse_sdi_meta();
                }
                println!("{:#?}", fsp_page);
            }
            PageTypes::INODE => {
                let inode_page: BasePage<INodePageBody> = pg_fact.parse();
                println!("{:#?}", inode_page);
            }
            PageTypes::INDEX => {
                let index_page: BasePage<IndexPageBody> = pg_fact.parse();
                println!("{:#?}", index_page);
            }
            PageTypes::SDI => {
                let sdi_page: BasePage<SdiPageBody> = pg_fact.parse();
                println!("{:#?}", sdi_page);
            }
            _ => {
                error!("Bad PageType, hdr = {:#?}", fil_hdr);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod app_tests {
    use super::*;
    use crate::util;
    use std::env::set_var;
    use std::sync::Once;

    const IBD_01: &str = "data/departments.ibd";
    const IBD_02: &str = "data/dept_manager.ibd";
    static INIT_ONCE: Once = Once::new();

    fn setup() {
        INIT_ONCE.call_once(|| {
            set_var("RUST_LOG", "info");
            util::init();
        });
    }

    #[test]
    fn info_datafile() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::Info).is_ok());
    }

    #[test]
    fn list_pages() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::List).is_ok());
    }

    #[test]
    fn view_first_fsp_hdr_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 0 }).is_ok());
    }

    #[test]
    fn view_first_inode_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 2 }).is_ok());
    }

    #[test]
    fn view_first_index_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 4 }).is_ok());
    }

    #[test]
    fn view_first_sdi_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 3 }).is_ok());
    }

    #[test]
    fn view_dump_simple_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::Dump { page: 4, limit: 10 }).is_ok());
    }

    #[test]
    fn it_works() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_02));
        assert!(app.run(Commands::Desc).is_ok());
        assert!(app.run(Commands::Dump { page: 4, limit: 3 }).is_ok());
    }
}
