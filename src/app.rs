use std::{
    cmp::min,
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Error, Result};
use colored::Colorize;
use log::{debug, error, info};

use crate::{
    factory::DatafileFactory,
    ibd::page::{
        BasePage, FileSpaceHeaderPageBody, INodePageBody, IndexPageBody, PageTypes, SdiPageBody, PAGE_SIZE,
        RECORD_HEADER_SIZE,
    },
    Commands,
};

#[derive(Debug)]
pub struct App {
    pub timer: Instant,
    pub input: PathBuf,
}

impl App {
    pub fn new(input: PathBuf) -> Self {
        Self {
            timer: Instant::now(),
            input,
        }
    }

    pub fn time_costs(&self) -> Duration {
        self.timer.elapsed()
    }

    pub fn run(&mut self, command: Commands) -> Result<()> {
        debug!("{:?}, {:?}", command, self);

        match command {
            Commands::Info => self.do_info()?,
            Commands::List => self.do_list()?,
            Commands::Desc => self.do_desc()?,
            Commands::Sdi => self.do_sdi_print()?,
            Commands::View { page: page_no } => self.do_view(page_no)?,
            Commands::Dump {
                page: page_no,
                limit,
                garbage,
                verbose,
            } => self.do_dump(page_no, limit, garbage, verbose)?,
        }

        Ok(())
    }

    fn do_info(&self) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        let hdr0 = fact.read_fil_hdr(0)?;

        // 基础信息
        println!("Meta Information:");
        println!(
            "{:>12} => server({}), space({})",
            "version".green(),
            &hdr0.server_version().to_string().blue(),
            &hdr0.space_version().to_string().blue()
        );
        println!("{:>12} => {}", "space_id".green(), &hdr0.space_id.to_string().blue());
        println!(
            "{:>12} => {}",
            "page_count".green(),
            &fact.page_count().to_string().blue()
        );
        println!("{:>12} => {}", "file_size".green(), fact.size.to_string().blue());

        // 页面类型统计
        let mut stats: BTreeMap<PageTypes, u32> = BTreeMap::new();
        for page_no in 0..fact.page_count() {
            let hdr = fact.read_fil_hdr(page_no)?;
            *stats.entry(hdr.page_type).or_insert(0) += 1;
        }
        println!("PageTypes Statistics:");
        for entry in &stats {
            println!("{:>12} => {}", entry.0.to_string().yellow(), entry.1.to_string().blue());
        }
        Ok(())
    }

    fn do_list(&self) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        for page_no in 0..fact.page_count() {
            let fil_hdr = fact.read_fil_hdr(page_no)?;
            let page_type = &fil_hdr.page_type;
            let offset = page_no * PAGE_SIZE;
            println!(
                "page_no={}, page_type={}, space_id={}, lsn={}, offset=0x{:0x?}({})",
                &page_no.to_string().magenta(),
                &page_type.to_string().yellow(),
                &fil_hdr.space_id.to_string().blue(),
                &fil_hdr.lsn.to_string().green(),
                offset,
                offset.to_string().blue(),
            );
        }
        Ok(())
    }

    fn do_desc(&mut self) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        let tabdef = fact.load_table_def()?;
        for col in &tabdef.col_defs {
            println!(
                "COL{}: name={}, dd_type={}, nullable={}, data_len={}, utf8_def={}",
                col.pos,
                col.col_name.magenta(),
                col.dd_type.to_string().blue(),
                col.isnil.to_string().yellow(),
                col.data_len.to_string().cyan(),
                col.utf8_def.green(),
            );
            info!("{:?}", col);
        }

        for idx in &tabdef.idx_defs {
            println!(
                "IDX{}: idx_name={}, idx_type={}, algorithm={}",
                idx.pos,
                idx.idx_name.magenta(),
                idx.idx_type.to_string().blue(),
                idx.algorithm.to_string().cyan(),
            );
            for e in &idx.elements {
                let ref_col = &tabdef.col_defs[e.column_opx];
                println!(
                    " ({}-{}): column_opx={}, col_name={}, order={}, ele_len={}, hidden={}, isnil={}, isvar={}",
                    idx.pos,
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
            info!("{:?}", idx);
        }

        Ok(())
    }

    fn do_sdi_print(&self) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        for e in fact.load_sdi_string()?.iter().enumerate() {
            println!("[{}] = {}", e.0.to_string().yellow(), e.1);
        }
        Ok(())
    }

    fn do_view(&self, page_no: usize) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        let fil_hdr = fact.read_fil_hdr(page_no)?;
        assert_eq!(page_no, fil_hdr.page_no as usize);

        match fil_hdr.page_type {
            PageTypes::ALLOCATED => {
                println!("新分配未使用的页, fil_hdr = {:#?}", fil_hdr);
            }
            PageTypes::FSP_HDR => {
                let fsp_page: BasePage<FileSpaceHeaderPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", fsp_page);
            }
            PageTypes::INODE => {
                let inode_page: BasePage<INodePageBody> = fact.read_page(page_no)?;
                println!("{:#?}", inode_page);
            }
            PageTypes::INDEX => {
                let index_page: BasePage<IndexPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", index_page);
            }
            PageTypes::SDI => {
                let sdi_page: BasePage<SdiPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", sdi_page);
            }
            _ => {
                error!("不支持的页面类型, hdr = {:#?}", fil_hdr);
            }
        }
        Ok(())
    }

    fn do_dump(&mut self, page_no: usize, limit: usize, garbage: bool, verbose: bool) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        let fil_hdr = fact.read_fil_hdr(page_no)?;
        let page_type = fil_hdr.page_type;
        if page_type != PageTypes::INDEX {
            return Err(Error::msg(format!("不支持的页类型: {:?}", page_type)));
        }

        let rs = fact.unpack_index_page(page_no, garbage)?;
        for (i, tuple) in rs.tuples[..min(rs.tuples.len(), limit)].iter().enumerate() {
            let rec = &rs.records[i];
            let seq = i + 1;

            // 打印分割线
            for _ in 0..40 {
                print!("*");
            }
            print!(" Row {} ", seq);
            for _ in 0..40 {
                print!("*");
            }
            println!();

            info!(
                "seq={}, addr=@{}, {:?}",
                seq.to_string().red(),
                &rec.addr.to_string().yellow(),
                &rec,
            );

            // 打印一些关键信息
            if verbose {
                println!("row_info: {:?}", &rec.row_info);
                println!("rec_hdr : {:?}", &rec.rec_hdr);
                let mut data_size = 0;
                for row in &rec.row_data.data_list {
                    data_size += row.1;
                }
                let var_area_size = rec.row_info.var_area.len();
                let nil_area_size = rec.row_info.nil_area.len();
                let total_size = var_area_size + nil_area_size + RECORD_HEADER_SIZE + data_size;
                let rec_addr = rec.row_data.addr;
                let page_offset = rec_addr - RECORD_HEADER_SIZE - nil_area_size - var_area_size;
                println!(
                    "rec_stat: rec_addr=0x{:0x?}@({}), data_size={}, var_area_size={}, nil_area_size={}, total_size={}, page_offset={}",
                    rec_addr,
                    rec_addr.to_string().yellow(),
                    data_size.to_string().magenta(),
                    var_area_size.to_string().blue(),
                    nil_area_size.to_string().blue(),
                    total_size.to_string().green(),
                    page_offset.to_string().yellow(),
                );
            }

            // 打印记录
            for ent in tuple {
                println!("{:>12} => {:?}", &ent.0.to_string().magenta(), &ent.1);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod app_tests {

    use super::*;
    use crate::util;

    const IBD_01: &str = "data/departments.ibd";
    const IBD_02: &str = "data/dept_manager.ibd";

    #[test]
    fn info_datafile() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::Info).is_ok());
    }

    #[test]
    fn list_pages() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::List).is_ok());
    }

    #[test]
    fn view_first_fsp_hdr_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 0 }).is_ok());
    }

    #[test]
    fn view_first_inode_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 2 }).is_ok());
    }

    #[test]
    fn view_first_index_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 4 }).is_ok());
    }

    #[test]
    fn view_first_sdi_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page: 3 }).is_ok());
    }

    #[test]
    fn view_dump_simple_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app
            .run(Commands::Dump {
                page: 4,
                limit: 10,
                garbage: false,
                verbose: false,
            })
            .is_ok());
    }

    #[test]
    fn it_works() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_02));
        assert!(app.run(Commands::Desc).is_ok());
        assert!(app
            .run(Commands::Dump {
                page: 4,
                limit: 3,
                garbage: false,
                verbose: false
            })
            .is_ok());
    }
}
