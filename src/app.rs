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
        BasePage, FileSpaceHeaderPageBody, INodePageBody, IndexPageBody, PageTypes, SdiPageBody, XDesPageBody,
        PAGE_SIZE,
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
            Commands::Info {
                inode_list,
                xdes_bitmap,
                all,
            } => self.do_info(inode_list, xdes_bitmap, all)?,
            Commands::List => self.do_list()?,
            Commands::Desc => self.do_desc()?,
            Commands::Sdi => self.do_sdi_print()?,
            Commands::View { page_no } => self.do_view(page_no)?,
            Commands::Dump {
                page_no,
                limit,
                garbage,
                verbose,
            } => self.do_dump(page_no, limit, garbage, verbose)?,
        }

        Ok(())
    }

    fn do_info(&self, flag_inode_list: bool, flag_xdes_bitmap: bool, flag_all: bool) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        // 基础信息
        self.do_info_metadata(&mut fact)?;

        // 页面类型统计
        self.do_info_page_stat(&mut fact)?;

        // 打印 INode 项
        if flag_inode_list || flag_all {
            self.do_info_inode(&mut fact)?;
        }

        // 打印 XDES bitmap 数据
        if flag_xdes_bitmap || flag_all {
            self.do_info_xdes_bitmap(&mut fact)?;
        }

        Ok(())
    }

    /// basic meta information
    fn do_info_metadata(&self, fact: &mut DatafileFactory) -> Result<()> {
        let hdr0 = fact.read_fil_hdr(0)?;

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
        println!("{:>12} => {}", "file_size".green(), fact.file_size.to_string().blue());
        Ok(())
    }

    /// page type statistic
    fn do_info_page_stat(&self, fact: &mut DatafileFactory) -> Result<()> {
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

    fn do_info_inode(&self, fact: &mut DatafileFactory) -> Result<()> {
        println!("INode entry list:");
        let inode_page: BasePage<INodePageBody> = fact.read_page(2)?;

        let inodes = &inode_page.page_body.inode_ent_list;
        for (seq, inode) in inodes.iter().enumerate() {
            println!(
                "{}: free={}, not-full={}, full={}, frag={:?}",
                seq.to_string().blue(),
                inode.fseg_free.len,
                inode.fseg_not_full.len,
                inode.fseg_full.len,
                inode.fseg_frag_arr,
            );
            println!("  fseg_full:");

            let mut faddr = inode.fseg_full.first.clone();
            let mut fseq = 0;
            loop {
                if faddr.page.is_none() {
                    break;
                }

                let xdes = fact.read_flst_node(faddr.page_no as usize, faddr.boffset)?;
                println!("  {}: xdes={:?}", fseq, &xdes);

                fseq += 1;
                faddr = xdes.flst_node.next;
            }
        }

        Ok(())
    }

    /// XDES bitmap
    fn do_info_xdes_bitmap(&self, fact: &mut DatafileFactory) -> Result<()> {
        println!("XDES bitmap: [F => free, X => non-free], [C => clean, D => dirty]");

        let mut counter = (0, 0, 0, 0); // F, X, C, D

        let mut n_xdes = 0;
        loop {
            let page_no = n_xdes * 16 * 1024;
            if page_no > fact.page_count() {
                break;
            }

            let xdes_page: BasePage<XDesPageBody> = fact.read_page(page_no)?;
            let xdes_list = &xdes_page.page_body.xdes_ent_inited;

            for (seq, xdes) in xdes_list.iter().enumerate() {
                print!("{}-{:03}: ", n_xdes, seq);
                for nth in 0..8 {
                    for shf in 0..8 {
                        let bits = &xdes.bitmap[nth * 8 + shf];
                        print!(
                            "{}",
                            if bits.1.free() {
                                counter.0 += 1;
                                "F".on_green()
                            } else {
                                counter.1 += 1;
                                "X".on_magenta()
                            }
                        );
                    }
                }

                print!(" ");

                for nth in 0..8 {
                    for shf in 0..8 {
                        let bits = &xdes.bitmap[nth * 8 + shf];
                        print!(
                            "{}",
                            if bits.2.clean() {
                                counter.2 += 1;
                                "C".on_cyan()
                            } else {
                                counter.3 += 1;
                                "D".on_red()
                            }
                        );
                    }
                }

                println!();
            }
            n_xdes += 1;
        }

        println!(
            "XDES bitmap count: free={}, non-free={}, clean={}, dirty={}",
            counter.0, counter.1, counter.2, counter.3
        );

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
        if fil_hdr.page_type != PageTypes::ALLOCATED {
            assert_eq!(
                page_no, fil_hdr.page_no as usize,
                "输入的页码和文件头的页码不一致, page_no={}, fil_hdr.page_no={}",
                page_no, fil_hdr.page_no
            );
        }

        match fil_hdr.page_type {
            PageTypes::ALLOCATED => {
                println!("新分配未使用的页, fil_hdr = {:#?}", fil_hdr);
            }
            PageTypes::FSP_HDR => {
                let fsp_page: BasePage<FileSpaceHeaderPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", fsp_page);
            }
            PageTypes::XDES => {
                let xdes_page: BasePage<XDesPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", xdes_page);
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
        let n_dump_rows = min(rs.tuples.len(), limit);
        for (i, tuple) in rs.tuples[..n_dump_rows].iter().enumerate() {
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
                println!("rec_stat: {:?}", &rec.calc_layout());
            }

            // 打印记录
            for ent in tuple {
                println!("{:>12} => {:?}", &ent.0.to_string().magenta(), &ent.1);
            }
        }

        if n_dump_rows < rs.tuples.len() {
            println!(
                "ONLY dump {} of {} rows, use `--limit num' to dump more",
                n_dump_rows,
                rs.tuples.len()
            )
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
        assert!(app
            .run(Commands::Info {
                inode_list: false,
                xdes_bitmap: false,
                all: true,
            })
            .is_ok());
    }

    #[test]
    fn list_datafile() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::List).is_ok());
    }

    #[test]
    fn view_fsp_hdr_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page_no: 0 }).is_ok());
    }

    #[test]
    fn view_inode_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page_no: 2 }).is_ok());
    }

    #[test]
    fn view_index_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page_no: 4 }).is_ok());
    }

    #[test]
    fn view_sdi_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::View { page_no: 3 }).is_ok());
    }

    #[test]
    fn view_dump_data_page() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        let ans = app.run(Commands::Dump {
            page_no: 4,
            limit: 3,
            garbage: false,
            verbose: false,
        });
        assert!(ans.is_ok());
    }

    #[test]
    fn it_works() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_02));
        assert!(app.run(Commands::Desc).is_ok());
        assert!(app
            .run(Commands::Dump {
                page_no: 4,
                limit: 3,
                garbage: false,
                verbose: false
            })
            .is_ok());
    }
}
