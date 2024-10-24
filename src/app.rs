use std::{
    cmp::min,
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Error, Result};
use colored::Colorize;
use log::{debug, error, info, warn};

use crate::{
    factory::DatafileFactory,
    ibd::{
        page::{
            BasePage, FileSpaceHeaderPageBody, FlstBaseNode, INodeEntry, INodePageBody,
            IndexPageBody, PageNumber, PageTypes, RSegArrayPageBody, RSegHeaderPageBody,
            SdiPageBody, SpaceId, TrxSysPageBody, UndoLogPageBody, XDesPageBody, EXTENT_PAGE_NUM,
            FSP_DICT_HDR_PAGE_NO, FSP_TRX_SYS_PAGE_NO, UNIV_PAGE_SIZE, XDES_ENTRY_MAX_COUNT,
            XDES_PAGE_COUNT,
        },
        record::DataValue,
        redo::{Blocks, LogFile, LogRecordTypes, RedoRecordPayloads},
        undo::UndoRecord,
    },
    util::{colored_extent_number, colored_page_number},
    Commands,
};

/// number of element per line
const N_ELE_PER_LINE: usize = 8;

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
            Commands::List {
                index,
                segment,
                extent,
                page,
                all,
                limit,
            } => {
                let mut fact = DatafileFactory::from_file(self.input.clone())?;
                let mut show_meta = true;

                if all || index {
                    show_meta = false;
                    self.do_list_indexes(&mut fact, limit)?
                }

                if all || segment {
                    show_meta = false;
                    self.do_list_inodes(&mut fact, limit)?
                }

                if all || extent {
                    show_meta = false;
                    self.do_list_extents(&mut fact)?
                }

                if all || page {
                    show_meta = false;
                    self.do_list_pages(&mut fact, limit)?
                }

                if show_meta {
                    self.do_list_metadata(&mut fact, limit)?
                }
            }
            Commands::Desc => self.do_desc()?,
            Commands::Sdi {
                table_define,
                root_segments,
            } => self.do_sdi_print(table_define, root_segments)?,
            Commands::View { page_no } => self.do_view_page(page_no)?,
            Commands::Dump {
                page_no,
                limit,
                garbage,
                verbose,
                btree_root: root,
            } => match page_no {
                Some(page_no) => self.do_dump_index_record(page_no, limit, garbage, verbose)?,
                None => match root {
                    Some(root_page_no) => {
                        debug!("root_page_no={:?}", root_page_no);
                        self.do_dump_btree(root_page_no)?;
                    }
                    None => {
                        debug!("dump all index header");
                        self.do_dump_index_header()?
                    }
                },
            },
            Commands::Undo {
                page_no,
                boffset,
                n_uniq,
            } => {
                let mut fact = DatafileFactory::from_file(self.input.clone())?;
                let buf = fact.page_buffer(page_no)?;
                let addr = 0;
                let undo_rec = UndoRecord::read(addr, buf, boffset, n_uniq);
                println!("{:#?}", undo_rec)
            }
            Commands::Redo {
                block_no,
                dump_log_type,
            } => match dump_log_type {
                Some(log_type) => self.do_dump_log_records(log_type)?,
                None => {
                    match block_no {
                        Some(block_no) => self.do_view_block(block_no)?,
                        None => self.do_view_log_file()?,
                    };
                }
            },
        };

        Ok(())
    }

    fn do_info(&self) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        // 基础信息
        self.do_info_metadata(&mut fact)?;

        // 页面类型统计
        self.do_info_page_stat(&mut fact)?;

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
        println!(
            "{:>12} => {}",
            "space_id".green(),
            &hdr0.space_id.to_string().blue()
        );
        println!(
            "{:>12} => {}",
            "page_count".green(),
            &fact.page_count().to_string().blue()
        );
        println!(
            "{:>12} => {}",
            "file_size".green(),
            fact.file_size.to_string().blue()
        );
        Ok(())
    }

    /// page type statistic
    fn do_info_page_stat(&self, fact: &mut DatafileFactory) -> Result<()> {
        let mut stats = BTreeMap::new();
        for page_no in 0..fact.page_count() {
            let hdr = fact.read_fil_hdr(page_no)?;
            *stats.entry(hdr.page_type).or_insert(0) += 1;
        }
        println!("PageTypes Statistics:");
        for entry in &stats {
            println!(
                "{:>12} => {}",
                entry.0.to_string().yellow(),
                entry.1.to_string().blue()
            );
        }

        Ok(())
    }

    /// list page metadata, page_type, page_no, space_id, etc.
    fn do_list_metadata(&self, fact: &mut DatafileFactory, limit: usize) -> Result<()> {
        for page_no in 0..fact.page_count() {
            if page_no >= limit {
                break;
            }
            let fil_hdr = fact.read_fil_hdr(page_no)?;
            let page_type = &fil_hdr.page_type;
            let offset = page_no * UNIV_PAGE_SIZE;
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

    fn do_list_indexes(&self, fact: &mut DatafileFactory, limit: usize) -> Result<()> {
        let tabdef = fact.load_table_def()?;
        for (i, idxdef) in tabdef.idx_defs.iter().enumerate() {
            if i >= limit {
                break;
            }
            debug!("idxdef={:?}", idxdef);
            if idxdef.idx_root <= 0 {
                return Err(Error::msg(format!(
                    "无法找到索引的 root 字段: {:?}",
                    &idxdef
                )));
            }

            let page_no = idxdef.idx_root as usize;
            let index_page: BasePage<IndexPageBody> = fact.read_page(page_no)?;
            let nonleaf_fseg_hdr = &index_page.page_body.fseg_hdr_1;
            println!(
                "{}(non-leaf), space_id={}, page_no={}, boffset={}",
                &idxdef.idx_name.to_string().yellow(),
                nonleaf_fseg_hdr.space_id,
                nonleaf_fseg_hdr.page_no,
                nonleaf_fseg_hdr.offset,
            );

            let inode_nonleaf =
                fact.read_inode_entry(nonleaf_fseg_hdr.page_no.into(), nonleaf_fseg_hdr.offset)?;
            self.do_list_inode(fact, &inode_nonleaf)?;

            let leaf_fseg_hdr = &index_page.page_body.fseg_hdr_0;
            println!(
                "{}(leaf), space_id={}, page_no={}, boffset={}",
                &idxdef.idx_name.to_string().yellow(),
                leaf_fseg_hdr.space_id,
                leaf_fseg_hdr.page_no,
                leaf_fseg_hdr.offset,
            );

            let inode_leaf =
                fact.read_inode_entry(leaf_fseg_hdr.page_no.into(), leaf_fseg_hdr.offset)?;
            self.do_list_inode(fact, &inode_leaf)?;
        }
        Ok(())
    }

    fn do_list_inodes(&self, fact: &mut DatafileFactory, limit: usize) -> Result<()> {
        println!("INode:");
        let inode_page: BasePage<INodePageBody> = fact.read_page(2)?;

        let inodes = &inode_page.page_body.inode_ent_list;
        for (i, inode) in inodes.iter().enumerate() {
            if i >= limit {
                break;
            }
            self.do_list_inode(fact, inode)?;
        }

        Ok(())
    }

    fn do_list_inode(&self, fact: &mut DatafileFactory, inode: &INodeEntry) -> Result<()> {
        println!(
            " iseq={}: fseg_id={}, free={}, not-full={}, full={}, frag={}, addr={}",
            inode.inode_seq.to_string().blue(),
            inode.fseg_id,
            inode.fseg_free.len,
            inode.fseg_not_full.len,
            inode.fseg_full.len,
            inode.fseg_frag_arr.len(),
            inode.addr,
        );
        if inode.fseg_free.len > 0 {
            println!("  {}", "fseg_free:".green());
            self.do_walk_xdes_flst(fact, &inode.fseg_free)?;
        }
        if inode.fseg_not_full.len > 0 {
            println!("  {}", "fseg_not_full:".yellow());
            self.do_walk_xdes_flst(fact, &inode.fseg_not_full)?;
        }
        if inode.fseg_full.len > 0 {
            println!("  {}", "fseg_full:".red());
            self.do_walk_xdes_flst(fact, &inode.fseg_full)?;
        }
        if !inode.fseg_frag_arr.is_empty() {
            println!("  {}", "fseg_frag_arr:".cyan());
            self.do_walk_page_frag(&inode.fseg_frag_arr)?;
        }
        Ok(())
    }

    fn do_walk_xdes_flst(&self, fact: &mut DatafileFactory, base: &FlstBaseNode) -> Result<()> {
        let mut faddr = base.first.clone();
        let mut i = 1;
        loop {
            if matches!(faddr.page_no, PageNumber::None) {
                break;
            }

            let page_no: usize = faddr.page_no.into();
            let xdes = fact.read_xdes_entry(page_no, faddr.boffset)?;

            if i % N_ELE_PER_LINE == 1 {
                print!("   {:>3} => ", i);
            }

            let xdes_no = page_no / EXTENT_PAGE_NUM * XDES_ENTRY_MAX_COUNT + xdes.xdes_seq;
            print!("{:>7}", colored_extent_number(xdes_no));

            if i % N_ELE_PER_LINE == 0 {
                println!();
            }

            faddr = xdes.flst_node.next;
            i += 1;
        }

        if i % N_ELE_PER_LINE != 1 {
            println!();
        }

        Ok(())
    }

    fn do_walk_page_frag(&self, arr: &[u32]) -> Result<()> {
        for (i, page_no) in arr.iter().enumerate() {
            if i % N_ELE_PER_LINE == 0 {
                print!("   {:>3} => ", i);
            }
            print!("{:>7}", colored_page_number(*page_no as usize));
            if (i + 1) % N_ELE_PER_LINE == 0 {
                println!();
            }
        }
        if arr.len() % N_ELE_PER_LINE != 0 {
            println!();
        }
        Ok(())
    }

    fn do_list_extents(&self, fact: &mut DatafileFactory) -> Result<()> {
        println!("XDES: ");

        self.do_list_ext_free_map(fact)?;
        self.do_list_ext_clean_map(fact)?;

        Ok(())
    }

    fn do_list_ext_free_map(&self, fact: &mut DatafileFactory) -> Result<()> {
        println!(" free bitmap: F => free, X => non-free");
        let mut counter = (0, 0);

        let mut i = 0;
        loop {
            let xdes_page_no = i * EXTENT_PAGE_NUM;
            if xdes_page_no > fact.page_count() {
                break;
            }

            let xdes_page: BasePage<XDesPageBody> = fact.read_page(xdes_page_no)?;
            let xdes_list = &xdes_page.page_body.xdes_ent_inited;

            for xdes in xdes_list {
                let xdes_no = i * XDES_ENTRY_MAX_COUNT + xdes.xdes_seq;
                print!(" {:>5} ", colored_extent_number(xdes_no));

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

                println!();
            }

            i += 1;
        }

        println!(
            " free bits count: free={}, non-free={}",
            counter.0, counter.1
        );

        Ok(())
    }

    fn do_list_ext_clean_map(&self, fact: &mut DatafileFactory) -> Result<()> {
        println!(" clean bitmap: C => clean, D => dirty");
        let mut counter = (0, 0);

        let mut i = 0;
        loop {
            let xdes_page_no = i * EXTENT_PAGE_NUM;
            if xdes_page_no > fact.page_count() {
                break;
            }

            let xdes_page: BasePage<XDesPageBody> = fact.read_page(xdes_page_no)?;
            let xdes_list = &xdes_page.page_body.xdes_ent_inited;

            // Print Clean Bit Map
            for xdes in xdes_list {
                let xdes_no = i * XDES_ENTRY_MAX_COUNT + xdes.xdes_seq;
                print!(" {:>5} ", colored_extent_number(xdes_no));

                for nth in 0..8 {
                    for shf in 0..8 {
                        let bits = &xdes.bitmap[nth * 8 + shf];
                        print!(
                            "{}",
                            if bits.2.clean() {
                                counter.0 += 1;
                                "C".on_cyan()
                            } else {
                                counter.1 += 1;
                                "D".on_red()
                            }
                        );
                    }
                }

                println!();
            }

            i += 1;
        }

        println!(
            " clean bits count: clean={}, dirty={}",
            counter.0, counter.1
        );
        Ok(())
    }

    fn do_list_pages(&self, fact: &mut DatafileFactory, limit: usize) -> Result<()> {
        println!("Page: H:FSH_HDR, X:XDES, I:INode, D:Index, S:SDI");
        println!("      Y:SYS, T:TRX_SYS, R:RSEG_ARRAY, U:UNDO_LOG");
        println!("      B:IBUF_BITMAP, A:Allocated, ?:Unknown");

        let mut page_types_vec = Vec::with_capacity(fact.page_count());
        for page_no in 0..fact.page_count() {
            let hdr = fact.read_fil_hdr(page_no)?;
            page_types_vec.push(hdr.page_type);
        }

        for (i, page_type) in page_types_vec.iter().enumerate() {
            if i >= limit {
                break;
            }
            let page_type_rept = match page_type {
                PageTypes::FSP_HDR => "H".on_purple(),
                PageTypes::XDES => "X".on_purple(),
                PageTypes::SYS => "Y".on_purple(),
                PageTypes::TRX_SYS => "T".on_purple(),
                PageTypes::RSEG_ARRAY => "R".on_yellow(),
                PageTypes::INODE => "I".on_blue(),
                PageTypes::INDEX => "D".on_cyan(),
                PageTypes::SDI => "S".on_purple(),
                PageTypes::IBUF_BITMAP => "B".on_blue(),
                PageTypes::UNDO_LOG => "U".on_cyan(),
                PageTypes::ALLOCATED => "A".on_green(),
                _ => "?".on_red(),
            };
            if i % XDES_PAGE_COUNT == 0 {
                let xdes_no = i / XDES_PAGE_COUNT;
                print!(" {:>5} ", colored_extent_number(xdes_no));
            }
            print!("{}", page_type_rept);
            if i % XDES_PAGE_COUNT == XDES_PAGE_COUNT - 1 {
                println!();
            }
        }

        if page_types_vec.len() % XDES_PAGE_COUNT != 0 {
            println!();
        }

        Ok(())
    }

    fn do_desc(&mut self) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        let tabdef = fact.load_table_def()?;
        for col in &tabdef.col_defs {
            println!(
                "COL{}: name={}, type={}, nullable={}, data_len={}, utf8_def={}",
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
                "IDX{}: name={}, type={}, id={}, root={}, algorithm={}",
                idx.pos,
                idx.idx_name.magenta(),
                idx.idx_type.to_string().blue(),
                idx.idx_id,
                idx.idx_root,
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

    fn do_sdi_print(&self, table_define: bool, root_segments: bool) -> Result<()> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        if table_define {
            let tabledef = fact.load_table_def()?;
            println!("{:#?}", tabledef);
            return Ok(());
        }

        if root_segments {
            let tabledef = fact.load_table_def()?;
            for idxdef in &tabledef.idx_defs {
                let root = idxdef.idx_root;
                if root <= 0 {
                    error!("错误的索引根页码: {:?}", &idxdef);
                    continue;
                }
                let index_page: BasePage<IndexPageBody> = fact.read_page(root as usize)?;
                println!(
                    "index={}, root={}, fseg(leaf)={:?}, fseg(non-leaf)={:?}",
                    idxdef.idx_name.to_string().magenta(),
                    root.to_string().blue(),
                    index_page.page_body.fseg_hdr_0,
                    index_page.page_body.fseg_hdr_1
                );
            }
            return Ok(());
        }

        for e in fact.load_sdi_string()?.iter().enumerate() {
            println!("[{}] = {}", e.0.to_string().yellow(), e.1);
        }

        Ok(())
    }

    fn do_view_page(&self, page_no: usize) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        let fil_hdr = fact.read_fil_hdr(page_no)?;
        if !matches!(fil_hdr.page_type, PageTypes::ALLOCATED) {
            let curr_page_no: usize = fil_hdr.page_no.into();
            if curr_page_no != page_no {
                panic!("输入的页码和文件头的页码不一致");
            }
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
            PageTypes::TRX_SYS => {
                assert_eq!(page_no, FSP_TRX_SYS_PAGE_NO);
                let trx_sys_page: BasePage<TrxSysPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", trx_sys_page);
            }
            PageTypes::RSEG_ARRAY => {
                let rseg_array_page: BasePage<RSegArrayPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", rseg_array_page);
            }
            PageTypes::UNDO_LOG => {
                let undo_log_page: BasePage<UndoLogPageBody> = fact.read_page(page_no)?;
                println!("{:#?}", undo_log_page);
            }
            PageTypes::SYS => {
                let fil_hdr = fact.read_fil_hdr(page_no)?;
                match fil_hdr.space_id {
                    SpaceId::UndoSpace(_) | SpaceId::InnoTempSpace => {
                        let rsa_hdr_page: BasePage<RSegHeaderPageBody> = fact.read_page(page_no)?;
                        println!("{:#?}", rsa_hdr_page);
                    }
                    SpaceId::SystemSpace if page_no != FSP_DICT_HDR_PAGE_NO => {
                        let rsa_hdr_page: BasePage<RSegHeaderPageBody> = fact.read_page(page_no)?;
                        println!("{:#?}", rsa_hdr_page);
                    }
                    _ => todo!("不支持的 SYS 页面类型, hdr = {:#?}", fil_hdr),
                }
            }
            _ => {
                error!("不支持的页面类型, hdr = {:#?}", fil_hdr);
            }
        }
        Ok(())
    }

    fn do_dump_btree(&self, root_page_no: usize) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        let fil_hdr = fact.read_fil_hdr(root_page_no)?;
        let page_type = fil_hdr.page_type;
        if page_type != PageTypes::INDEX {
            return Err(Error::msg(format!("不支持的页类型: {:?}", page_type)));
        }

        Self::do_traverse_index(&mut fact, root_page_no, 0)?;

        Ok(())
    }

    fn do_traverse_index(fact: &mut DatafileFactory, page_no: usize, indent: usize) -> Result<()> {
        let curr: BasePage<IndexPageBody> = fact.read_page(page_no)?;
        let idx_hdr = &curr.page_body.idx_hdr;
        for _ in 0..indent {
            print!("  ");
        }
        let result_set = fact.unpack_index_page(page_no, false)?;

        let first = &result_set.tuples[0];
        let sep_idx = first
            .iter()
            .position(|(_, val)| {
                matches!(
                    val,
                    DataValue::PageNo(_) | DataValue::TrxId(_) | DataValue::RbPtr(_)
                )
            })
            .unwrap_or(first.len());
        let key = &first[0..sep_idx];
        println!(
            "{}: level={}, min_key={:?}, n_rec={}",
            colored_page_number(page_no),
            idx_hdr.page_level,
            &key,
            idx_hdr.page_n_recs,
        );

        if idx_hdr.page_level > 0 {
            for tuple in &result_set.tuples {
                let node_ptr = tuple.last().unwrap();
                match node_ptr.1 {
                    DataValue::PageNo(child_page_no) => {
                        Self::do_traverse_index(fact, child_page_no as usize, indent + 1)?;
                    }
                    _ => panic!("错误的节点: {:?}", tuple),
                }
            }
        }

        Ok(())
    }

    fn do_dump_index_header(&self) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        for page_no in 0..fact.page_count() {
            let fil_hdr = fact.read_fil_hdr(page_no)?;

            if page_no % 8 == 0 {
                print!("{:>7} ", colored_page_number(page_no));
            }

            if fil_hdr.page_type == PageTypes::INDEX {
                let idx_hdr = fact.read_idx_hdr(page_no)?;
                print!("[{:>1},{:>4}]", idx_hdr.page_level, idx_hdr.page_n_recs);
            } else {
                print!("[{:>6.6}]", fil_hdr.page_type);
            }

            if page_no % 8 == 7 {
                println!();
            } else {
                print!(" ");
            }
        }
        Ok(())
    }

    fn do_dump_index_record(
        &mut self,
        page_no: usize,
        limit: usize,
        garbage: bool,
        verbose: bool,
    ) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;

        let fil_hdr = fact.read_fil_hdr(page_no)?;
        let page_type = fil_hdr.page_type;
        if page_type != PageTypes::INDEX {
            return Err(Error::msg(format!("不支持的页类型: {:?}", page_type)));
        }

        let result_set = fact.unpack_index_page(page_no, garbage)?;
        let n_dump_rows = min(result_set.tuples.len(), limit);
        for (i, tuple) in result_set.tuples[..n_dump_rows].iter().enumerate() {
            let rec = &result_set.records[i];
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

        if n_dump_rows < result_set.tuples.len() {
            println!(
                "ONLY dump {} of {} rows, use `--limit num' to dump more",
                n_dump_rows,
                result_set.tuples.len()
            )
        }

        Ok(())
    }

    fn do_dump_log_records(&self, log_rec_type: LogRecordTypes) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        let buf = fact.file_buffer()?;
        let log_file = LogFile::new(0, buf);

        for blk in &log_file.log_block_list {
            if let Blocks::Block(block) = blk {
                let rec_type = if let Some(rec) = &block.log_record {
                    rec.log_rec_hdr.log_rec_type.clone()
                } else {
                    LogRecordTypes::UNDEF
                };
                if rec_type != log_rec_type {
                    continue;
                }
                if let Some(rec) = &block.log_record {
                    println!("{:>6} => {:?}", block.block_no, &rec.log_rec_hdr);
                }
            }
        }

        Ok(())
    }

    fn do_view_block(&self, block_no: usize) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        let block = fact.read_block(block_no)?;
        match block {
            Blocks::FileHeader(log_fil_hdr) => {
                println!("{:#?}", log_fil_hdr);
            }
            Blocks::Block(log_block) => {
                println!("{:#?}", log_block);
            }
            Blocks::Checkpoint(checkpoint) => {
                println!("{:#?}", checkpoint);
            }
            Blocks::Unused => {
                println!("Unused block");
            }
        }
        Ok(())
    }

    fn do_view_log_file(&self) -> Result<(), Error> {
        let mut fact = DatafileFactory::from_file(self.input.clone())?;
        let buf = fact.file_buffer()?;
        let log_file = LogFile::new(0, buf);

        let mut stats = BTreeMap::new();
        let mut unknown = HashSet::new();
        for blk in &log_file.log_block_list {
            if let Blocks::Block(block) = blk {
                let rec_type = if let Some(rec) = &block.log_record {
                    if matches!(rec.redo_rec_data, RedoRecordPayloads::Unknown) {
                        unknown.insert(rec.log_rec_hdr.log_rec_type.clone());
                    }
                    rec.log_rec_hdr.log_rec_type.clone()
                } else {
                    LogRecordTypes::UNDEF
                };
                *stats.entry(rec_type).or_insert(0) += 1;
            }
        }

        println!("RedoRecordTypes Statistics:");
        for entry in &stats {
            println!(
                "{:>28} {}> {}",
                entry.0.to_string().yellow(),
                if !unknown.contains(entry.0) {
                    "=".to_string().green()
                } else {
                    "=".to_string().red()
                },
                entry.1.to_string().blue()
            );
        }

        if !unknown.is_empty() {
            warn!("Unknown type: {}", &format!("{:?}", &unknown).yellow());
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
    const REDO_1: &str = "data/redo_block_01";
    const UNDO_1: &str = "data/undo_log_01";

    #[test]
    fn info_datafile() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app.run(Commands::Info {}).is_ok());
    }

    #[test]
    fn list_datafile() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_01));
        assert!(app
            .run(Commands::List {
                index: false,
                segment: false,
                extent: false,
                page: false,
                all: true,
                limit: 32,
            })
            .is_ok());
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
            page_no: Some(4),
            limit: 3,
            garbage: false,
            verbose: false,
            btree_root: None,
        });
        assert!(ans.is_ok());
    }

    #[test]
    fn view_redo_log_file() {
        util::init_unit_test();
        let app = App::new(PathBuf::from(REDO_1));
        let ans = app.do_view_log_file();
        assert!(ans.is_ok());
    }

    #[test]
    fn view_redo_log_block_0() {
        util::init_unit_test();
        let app = App::new(PathBuf::from(REDO_1));
        let ans = app.do_view_block(0);
        assert!(ans.is_ok());
    }

    #[test]
    fn it_works() {
        util::init_unit_test();
        let mut app = App::new(PathBuf::from(IBD_02));
        assert!(app.run(Commands::Desc).is_ok());
        assert!(app
            .run(Commands::Dump {
                page_no: Some(4),
                limit: 3,
                garbage: false,
                verbose: false,
                btree_root: None,
            })
            .is_ok());
    }
}
