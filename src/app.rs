use crate::factory::DatafileFactory;
use crate::ibd::page::{
    BasePage, FileSpaceHeaderPageBody, IndexPageBody, INodePageBody, PAGE_SIZE, PageTypes, RECORD_HEADER_SIZE,
    SdiPageBody,
};
use crate::{Commands, util};
use anyhow::{Error, Result};
use colored::Colorize;
use log::{debug, error, info};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::ibd::record::{ColumnTypes, HiddenTypes, Record};
use crate::meta::def::TableDef;

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
                verbose,
            } => self.do_dump(page_no, limit, verbose)?,
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
        let json_str = fact.load_sdi_string()?;
        let sdi_data = jsonxf::pretty_print(&json_str).unwrap();
        println!("{}", sdi_data);
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

    fn do_dump(&mut self, page_no: usize, limit: usize, verbose: bool) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        let fil_hdr = fact.read_fil_hdr(page_no)?;
        let page_type = fil_hdr.page_type;
        if page_type != PageTypes::INDEX {
            return Err(Error::msg(format!("不支持的页类型: {:?}", page_type)));
        }

        let index_page: BasePage<IndexPageBody> = fact.read_page(page_no)?;
        let page_level = index_page.page_body.idx_hdr.page_level;
        if page_level != 0 {
            return Err(Error::msg(format!("不支持查看非叶子节点: page_level={:?}", page_level)));
        }

        let tabdef = fact.load_table_def()?;
        debug!("tabdef = {:?}", &tabdef);

        let index_id = index_page.page_body.idx_hdr.page_index_id;
        let idxdef = match tabdef.idx_defs.iter().find(|i| i.idx_id == index_id) {
            None => {
                return Err(Error::msg(format!("未找到索引的元信息: index_id={:?}", index_id)));
            }
            Some(v) => {
                info!("index={}, {:?}", v.idx_name.to_string().green(), &v);
                v
            }
        };

        let data_rec_list = index_page.page_body.read_user_records(tabdef.clone(), idxdef)?;
        for (i, urec) in data_rec_list.iter().enumerate() {
            if i >= limit {
                break;
            }
            Self::dump_record_data(i + 1, urec, tabdef.clone(), verbose);
        }

        let free_rec_list = index_page.page_body.read_free_records(tabdef.clone(), idxdef)?;
        for (i, frec) in free_rec_list.iter().enumerate() {
            Self::dump_record_data(i + 1, frec, tabdef.clone(), verbose);
        }

        Ok(())
    }

    fn dump_record_data(seq: usize, rec: &Record, tabdef: Arc<TableDef>, verbose: bool) {
        info!(
            "seq={}, addr=@{}, {}={:?}, {}={:?}, {}={:?}",
            seq.to_string().red(),
            &rec.row_data.addr.to_string().yellow(),
            "hdr".cyan(),
            &rec.rec_hdr,
            "info".magenta(),
            &rec.row_info,
            "data".green(),
            &rec.row_data,
        );
        println!(
            "****************************** Row {} ******************************",
            seq
        );
        if verbose {
            println!("rec_hdr: {:?}", rec.rec_hdr);
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
        for row in &rec.row_data.data_list {
            let col = &tabdef.clone().col_defs[row.0];
            match &row.2 {
                Some(datum) => {
                    if col.hidden == HiddenTypes::HT_HIDDEN_SE {
                        match col.col_name.as_str() {
                            "DB_ROW_ID" | "DB_TRX_ID" => {
                                println!(
                                    "{:>12} => {:?} [{}]",
                                    &col.col_name.magenta(),
                                    &datum,
                                    &format!("0x{:012x?}", util::unpack_u48_val(datum)).green()
                                );
                            }
                            "DB_ROLL_PTR" => {
                                println!(
                                    "{:>12} => {:?} [{}]",
                                    &col.col_name.magenta(),
                                    &datum,
                                    &format!("0x{:014x?}", util::unpack_u56_val(datum)).green()
                                );
                            }
                            _ => todo!("ERR_HIDDEN_SE_COL: {}", col.col_name),
                        }
                        continue;
                    }

                    match &col.dd_type {
                        ColumnTypes::LONG => {
                            println!(
                                "{:>12} => {:?} [{}]",
                                &col.col_name.magenta(),
                                datum,
                                util::unpack_i32_val(datum).to_string().blue(),
                            );
                        }
                        ColumnTypes::LONGLONG => {
                            println!(
                                "{:>12} => {:?} [{}]",
                                &col.col_name.magenta(),
                                datum,
                                util::unpack_i64_val(datum).to_string().blue(),
                            );
                        }
                        ColumnTypes::NEWDATE => {
                            println!(
                                "{:>12} => {:?} [{}]",
                                &col.col_name.magenta(),
                                datum,
                                util::unpack_newdate_val(datum).unwrap().to_string().cyan(),
                            );
                        }
                        ColumnTypes::DATETIME2 => {
                            println!(
                                "{:>12} => {:?} [{}]",
                                &col.col_name.magenta(),
                                datum,
                                util::unpack_datetime2_val(datum).unwrap().to_string().cyan(),
                            );
                        }
                        ColumnTypes::TIMESTAMP2 => {
                            println!(
                                "{:>12} => {:?} [{}]",
                                &col.col_name.magenta(),
                                datum,
                                util::unpack_timestamp2_val(datum).to_string().blue(),
                            );
                        }
                        ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING => {
                            let barr = &datum.to_vec();
                            let text = std::str::from_utf8(barr).unwrap();
                            println!("{:>12} => {:?} [{}]", &col.col_name.magenta(), &datum, text.yellow());
                        }
                        _ => {
                            println!("{:>12} => {:?}", &col.col_name.magenta(), datum);
                        }
                    }
                }
                None => {
                    println!("{:>12} => {}", &col.col_name.magenta(), "NULL".red());
                }
            }
        }
    }
}

#[cfg(test)]
mod app_tests {
    use super::*;
    use crate::util;
    use std::env::set_var;

    const IBD_01: &str = "data/departments.ibd";
    const IBD_02: &str = "data/dept_manager.ibd";

    fn setup() {
        set_var("RUST_LOG", "debug");
        util::init();
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
        assert!(app
            .run(Commands::Dump {
                page: 4,
                limit: 10,
                verbose: false
            })
            .is_ok());
    }

    #[test]
    fn it_works() {
        setup();
        let mut app = App::new(PathBuf::from(IBD_02));
        assert!(app.run(Commands::Desc).is_ok());
        assert!(app
            .run(Commands::Dump {
                page: 4,
                limit: 3,
                verbose: false
            })
            .is_ok());
    }
}
