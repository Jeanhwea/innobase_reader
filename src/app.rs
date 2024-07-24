use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use colored::Colorize;
use std::path::PathBuf;

use crate::ibd::factory::{DatafileFactory, PageFactory, SDI_META_INFO_MIN_VER};
use crate::ibd::page::{
    BasePage, FileSpaceHeaderPage, INodePage, IndexPage, PageTypes, SdiIndexPage,
};

use crate::Commands;
use anyhow::{Error, Result};
use log::{debug, error, info, warn};

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

        let datafile = self.factory.datafile();
        info!("datafile = {:?}", datafile);

        match command {
            Commands::Info => self.do_info()?,
            Commands::List => self.do_list()?,
            Commands::Desc => self.do_desc()?,
            Commands::Sdi => self.do_print_sdi_json()?,
            Commands::Dump { page: page_no } => self.do_dump(page_no)?,
            Commands::View { page: page_no } => self.do_view(page_no)?,
        }

        Ok(())
    }

    fn do_info(&self) -> Result<()> {
        let factory = &self.factory;
        let mut stats: BTreeMap<PageTypes, u32> = BTreeMap::new();
        for page_no in 0..factory.page_count() {
            let hdr = factory.parse_fil_hdr(page_no)?;
            *stats.entry(hdr.page_type).or_insert(0) += 1;
        }

        let df = factory.datafile();
        println!("Meta Information:");
        println!(
            "{:>12} => server({}), space({})",
            "version".green(),
            df.server_version.to_string().blue(),
            df.space_version.to_string().blue()
        );
        println!(
            "{:>12} => {}",
            "space_id".green(),
            df.space_id.to_string().blue()
        );
        println!(
            "{:>12} => {}",
            "page_count".green(),
            factory.page_count().to_string().blue()
        );
        println!(
            "{:>12} => {}",
            "file_size".green(),
            factory.file_size().to_string().blue()
        );

        println!("PageTypes Statistics:");
        for s in &stats {
            println!(
                "{:>12} => {}",
                s.0.to_string().yellow(),
                s.1.to_string().blue()
            );
        }
        Ok(())
    }

    fn do_list(&self) -> Result<()> {
        let factory = &self.factory;
        for page_no in 0..factory.page_count() {
            let fil_hdr = factory.parse_fil_hdr(page_no)?;
            let pt = &fil_hdr.page_type;
            println!(
                "page_no={} => page_type={}, space_id={}, lsn={}",
                &page_no.to_string().magenta(),
                &pt.to_string().yellow(),
                &fil_hdr.space_id.to_string().blue(),
                &fil_hdr.lsn.to_string().green(),
            );
            match pt {
                PageTypes::ALLOCATED => {}
                PageTypes::UNDEF => {
                    warn!("{:?} page_no = {}", pt, page_no);
                }
                _ => {
                    info!("fil_hdr = {:?}", fil_hdr);
                }
            }
        }
        Ok(())
    }

    fn do_desc(&self) -> Result<()> {
        let factory = &self.factory;
        for page_no in 0..factory.page_count() {
            let fil_hdr = factory.parse_fil_hdr(page_no)?;
            if fil_hdr.page_type == PageTypes::SDI {
                let buffer = factory.read_page(page_no)?;
                let sdi_page: BasePage<SdiIndexPage> = PageFactory::new(buffer).parse();

                if let Some(obj) = sdi_page.page_body.get_sdi_object() {
                    debug!("sdi_obj = {:#?}", obj);
                    let mut cols = obj.dd_object.columns;
                    if !cols.is_empty() {
                        cols.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));
                        println!("Columns:");
                        for c in &cols {
                            println!(
                                    "{:>3}: name={}, dd_type={}, utf8_type={}, nullable={}, char_length={}, hidden={}, comment={}",
                                    c.ordinal_position,
                                    c.col_name.magenta(),
                                    c.dd_type.to_string().blue(),
                                    c.column_type_utf8.green(),
                                    c.is_nullable.to_string().yellow(),
                                    c.char_length.to_string().yellow(),
                                    c.hidden.to_string().cyan(),
                                    c.comment,
                                );
                            info!("{:#?}", c);
                        }
                    }

                    let mut idxs = obj.dd_object.indexes;
                    if !idxs.is_empty() {
                        idxs.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));
                        println!("Indexes:");
                        for i in &idxs {
                            println!(
                                "{:>3}: name={}, idx_type={}, algorithm={}, hidden={}, comment={}",
                                i.ordinal_position,
                                i.name.magenta(),
                                i.idx_type.to_string().cyan(),
                                i.algorithm.to_string().green(),
                                i.hidden.to_string().yellow(),
                                i.comment,
                            );
                            info!("{:#?}", i);
                        }
                    }
                }
                break;
            }
        }
        Ok(())
    }

    fn do_print_sdi_json(&self) -> Result<()> {
        let factory = &self.factory;
        for page_no in 0..factory.page_count() {
            let fil_hdr = factory.parse_fil_hdr(page_no)?;
            if fil_hdr.page_type == PageTypes::SDI {
                let buffer = factory.read_page(page_no)?;
                let sdi_page: BasePage<SdiIndexPage> = PageFactory::new(buffer).parse();
                let sdi_data = sdi_page.page_body.get_sdi_data();
                println!("{}", sdi_data);
                break;
            }
        }
        Ok(())
    }

    fn do_dump(&mut self, page_no: usize) -> Result<(), Error> {
        let factory = &mut self.factory;
        if page_no >= factory.page_count() {
            return Err(Error::msg("Page number out of range"));
        }

        let fil_hdr = factory.parse_fil_hdr(page_no)?;
        if fil_hdr.page_type != PageTypes::INDEX {
            return Err(Error::msg(format!(
                "Only support dump INDEX page, but found {:?}",
                fil_hdr.page_type
            )));
        }
        let buffer = factory.read_page(page_no)?;
        let mut index_page: BasePage<IndexPage> = PageFactory::new(buffer).parse();

        let tabdef = factory.load_tabdef()?;
        info!("tabdef = {:?}", &tabdef);

        index_page.page_body.parse_records(&tabdef)?;

        Ok(())
    }

    fn do_view(&self, page_no: usize) -> Result<(), Error> {
        let factory = &self.factory;
        if page_no >= factory.page_count() {
            return Err(Error::msg("Page number out of range"));
        }

        let buffer = factory.read_page(page_no)?;
        let pg_fact = PageFactory::new(buffer);
        let fil_hdr = pg_fact.fil_hdr();
        match fil_hdr.page_type {
            PageTypes::ALLOCATED => {
                println!("allocated only page, fil_hdr = {:#?}", fil_hdr);
            }
            PageTypes::FSP_HDR => {
                assert_eq!(page_no, fil_hdr.page_no as usize);
                let mut fsp_page: BasePage<FileSpaceHeaderPage> = pg_fact.parse();
                if factory.datafile().server_version > SDI_META_INFO_MIN_VER {
                    fsp_page.page_body.parse_sdi_meta();
                }
                println!("{:#?}", fsp_page);
            }
            PageTypes::INODE => {
                let inode_page: BasePage<INodePage> = pg_fact.parse();
                println!("{:#?}", inode_page);
            }
            PageTypes::INDEX => {
                let index_page: BasePage<IndexPage> = pg_fact.parse();
                println!("{:#?}", index_page);
            }
            PageTypes::SDI => {
                let sdi_page: BasePage<SdiIndexPage> = pg_fact.parse();
                println!("{:#?}", sdi_page);
            }
            PageTypes::UNDEF => {
                warn!("page_no = {}, hdr = {:?}", page_no, fil_hdr);
            }
            _ => {
                error!("unsupported page type, hdr = {:#?}", fil_hdr);
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

    const IBD_FILE: &str = "data/departments.ibd";
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
        let mut app = App::new(PathBuf::from(IBD_FILE));
        assert!(app.run(Commands::Info).is_ok());
    }

    #[test]
    fn list_pages() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_FILE));
        assert!(app.run(Commands::List).is_ok());
    }

    #[test]
    fn view_first_fsp_hdr_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_FILE));
        assert!(app.run(Commands::View { page: 0 }).is_ok());
    }

    #[test]
    fn view_first_inode_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_FILE));
        assert!(app.run(Commands::View { page: 2 }).is_ok());
    }

    #[test]
    fn view_first_index_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_FILE));
        assert!(app.run(Commands::View { page: 4 }).is_ok());
    }

    #[test]
    fn view_first_sdi_page() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_FILE));
        assert!(app.run(Commands::View { page: 3 }).is_ok());
    }

    #[test]
    fn it_works() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_FILE));
        assert!(app.run(Commands::Dump { page: 4 }).is_ok());
    }
}
