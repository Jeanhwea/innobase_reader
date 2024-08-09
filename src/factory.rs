use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Error, Result};
use bytes::Bytes;
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime};
use derivative::Derivative;
use log::{debug, info, warn};

use crate::{
    ibd::{
        page::{
            BasePage, BasePageBody, FilePageHeader, FileSpaceHeaderPageBody, IndexPageBody, SdiPageBody,
            FIL_HEADER_SIZE, PAGE_SIZE,
        },
        record::{ColumnTypes, HiddenTypes, Record},
    },
    meta::{
        cst::coll_find,
        def::{ColumnDef, IndexDef, IndexElementDef, TableDef},
    },
    util::{
        self, unpack_datetime2_val, unpack_enum_val, unpack_i32_val, unpack_i64_val, unpack_newdate_val,
        unpack_timestamp2_val, unpack_u48_val, unpack_u56_val,
    },
};

pub const SDI_META_INFO_MIN_VER: u32 = 80000;

#[derive(Clone, Derivative, Eq, PartialEq)]
#[derivative(Debug)]
pub enum DataValue {
    RowId(#[derivative(Debug(format_with = "util::fmt_hex48"))] u64),
    TrxId(#[derivative(Debug(format_with = "util::fmt_hex48"))] u64),
    RollPtr(#[derivative(Debug(format_with = "util::fmt_hex56"))] u64),
    I32(i32),
    I64(i64),
    Str(String),
    Enum(u16),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Timestamp(DateTime<Local>),
    Unknown(Bytes),
    Null,
}

#[derive(Debug)]
pub struct ResultSet {
    pub garbage: bool,
    pub tabdef: Arc<TableDef>,
    pub records: Vec<Record>,
    pub tuples: Vec<Vec<(String, DataValue)>>,
}

#[derive(Debug)]
pub struct DatafileFactory {
    pub target: PathBuf, // Target datafile
    pub file: File,      // Data file descriptor
    pub size: usize,     // Data file size
}

impl DatafileFactory {
    pub fn from_file(target: PathBuf) -> Result<Self> {
        if !target.exists() {
            return Err(Error::msg(format!("没有找到目标文件: {:?}", target)));
        }

        let file = File::open(&target)?;
        let size = file.metadata()?.len() as usize;

        info!("加载数据文件: {:?}", &file);

        Ok(Self { target, size, file })
    }

    pub fn page_count(&self) -> usize {
        self.size / PAGE_SIZE
    }

    pub fn page_buffer(&mut self, page_no: usize) -> Result<Arc<Bytes>> {
        if page_no >= self.page_count() {
            return Err(Error::msg(format!("页码范围溢出: page_no={}", page_no)));
        }

        let offset = (page_no * PAGE_SIZE) as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0; PAGE_SIZE];
        self.file.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    pub fn fil_hdr_buffer(&mut self, page_no: usize) -> Result<Arc<Bytes>> {
        if page_no >= self.page_count() {
            return Err(Error::msg(format!("页码范围溢出: page_no={}", page_no)));
        }

        let offset = (page_no * PAGE_SIZE) as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0; FIL_HEADER_SIZE];
        self.file.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    pub fn read_fil_hdr(&mut self, page_no: usize) -> Result<FilePageHeader> {
        let buf = self.fil_hdr_buffer(page_no)?;
        Ok(FilePageHeader::new(0, buf.clone()))
    }

    pub fn read_page<P>(&mut self, page_no: usize) -> Result<BasePage<P>>
    where
        P: BasePageBody,
    {
        let buf = self.page_buffer(page_no)?;
        Ok(BasePage::new(0, buf.clone()))
    }

    fn read_sdi_page(&mut self) -> Result<BasePage<SdiPageBody>, Error> {
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = self.read_page(0)?;
        if fsp_page.fil_hdr.server_version() < SDI_META_INFO_MIN_VER {
            return Err(Error::msg("数据文件版本过低，没有表元信息"));
        }
        let sdi_meta = fsp_page.page_body.sdi_meta();
        let sdi_page_no = sdi_meta.sdi_page_no as usize;
        self.read_page(sdi_page_no)
    }

    pub fn load_sdi_string(&mut self) -> Result<Vec<String>, Error> {
        let sdi_page = self.read_sdi_page()?;
        let ret: Vec<String> = sdi_page
            .page_body
            .read_sdi_objects()?
            .iter()
            .map(|obj| jsonxf::pretty_print(&obj.sdi_str).unwrap_or("".into()))
            .collect();
        Ok(ret)
    }

    pub fn load_table_def(&mut self) -> Result<Arc<TableDef>> {
        let sdi_page = self.read_sdi_page()?;

        let ddobj = sdi_page.page_body.get_tabdef_sdiobj()?.dd_object;
        debug!("ddobj={:#?}", &ddobj);

        let coll = coll_find(ddobj.collation_id);
        info!("当前文件字符集: {:?}", &coll);

        let coldefs = ddobj.columns.iter().map(ColumnDef::from).collect::<Vec<_>>();
        let idxdefs = ddobj
            .indexes
            .iter()
            .map(|idx| {
                let mut ele_defs: Vec<IndexElementDef> = idx
                    .elements
                    .iter()
                    .map(|ele| {
                        let ref_col = &coldefs[ele.column_opx as usize];
                        IndexElementDef::from(ele, ref_col)
                    })
                    .collect();

                let nullinfo = ele_defs.iter().filter(|e| e.isnil).map(|e| e.pos).collect::<Vec<_>>();
                debug!("nullinfo={:?}", nullinfo);

                for (off, pos) in nullinfo.iter().enumerate() {
                    ele_defs[pos - 1].null_offset = off;
                }
                let nil_size = util::align8(nullinfo.len());
                IndexDef::from(idx, ele_defs, nil_size)
            })
            .collect();
        debug!("idxdefs={:?}", &idxdefs);

        Ok(Arc::from(TableDef {
            schema_ref: ddobj.schema_ref.clone(),
            tab_name: ddobj.name.clone(),
            collation_id: ddobj.collation_id,
            collation: coll.name.into(),
            charset: coll.charset.into(),
            col_defs: coldefs,
            idx_defs: idxdefs,
        }))
    }

    pub fn unpack_index_page(&mut self, page_no: usize, garbage: bool) -> Result<ResultSet, Error> {
        let page: BasePage<IndexPageBody> = self.read_page(page_no)?;
        let page_level = page.page_body.idx_hdr.page_level;
        if page_level != 0 {
            return Err(Error::msg(format!("不支持查看非叶子节点: page_level={:?}", page_level)));
        }

        let tabdef = self.load_table_def()?;
        let index_id = page.page_body.idx_hdr.page_index_id;
        let idxdef = match tabdef.idx_defs.iter().find(|i| i.idx_id == index_id) {
            None => {
                return Err(Error::msg(format!("未找到索引的元信息: index_id={:?}", index_id)));
            }
            Some(val) => val,
        };
        info!("当前页所引用的索引: index_name={}", idxdef.idx_name);

        let rec_list = if garbage {
            page.page_body.read_free_records(tabdef.clone(), idxdef)?
        } else {
            page.page_body.read_user_records(tabdef.clone(), idxdef)?
        };
        debug!("rec_list={:?}", rec_list);

        let tuples = rec_list
            .iter()
            .map(|rec| {
                rec.row_data
                    .data_list
                    .iter()
                    .map(|c| {
                        let col = &tabdef.col_defs[c.0];
                        let val = match &c.2 {
                            Some(b) => match col.hidden {
                                HiddenTypes::HT_VISIBLE => match col.dd_type {
                                    ColumnTypes::LONG => DataValue::I32(unpack_i32_val(b)),
                                    ColumnTypes::LONGLONG => DataValue::I64(unpack_i64_val(b)),
                                    ColumnTypes::NEWDATE => DataValue::Date(
                                        unpack_newdate_val(b).unwrap_or_else(|| panic!("日期格式错误: {:?}", &c)),
                                    ),
                                    ColumnTypes::DATETIME2 => DataValue::DateTime(
                                        unpack_datetime2_val(b).unwrap_or_else(|| panic!("时间格式错误: {:?}", &c)),
                                    ),
                                    ColumnTypes::TIMESTAMP2 => DataValue::Timestamp(unpack_timestamp2_val(b)),
                                    ColumnTypes::VARCHAR | ColumnTypes::VAR_STRING | ColumnTypes::STRING => {
                                        let barr = b.to_vec();
                                        let text = std::str::from_utf8(&barr)
                                            .unwrap_or_else(|_| panic!("字符串格式错误: {:?}", &c));
                                        DataValue::Str(text.into())
                                    }
                                    ColumnTypes::ENUM => DataValue::Enum(unpack_enum_val(b)),
                                    _ => {
                                        warn!("不支持解析的类型: {:?}", &col);
                                        DataValue::Unknown(b.clone())
                                    }
                                },
                                HiddenTypes::HT_HIDDEN_SE => match col.col_name.as_str() {
                                    "DB_ROW_ID" => DataValue::RowId(unpack_u48_val(b)),
                                    "DB_TRX_ID" => DataValue::TrxId(unpack_u48_val(b)),
                                    "DB_ROLL_PTR" => DataValue::RollPtr(unpack_u56_val(b)),
                                    _ => todo!("不支持的隐藏字段名称: {:?}", col),
                                },
                                _ => todo!("不支持的隐藏字段类型: {:?}", col),
                            },

                            None => DataValue::Null,
                        };
                        (col.col_name.clone(), val)
                    })
                    .collect()
            })
            .collect();

        Ok(ResultSet {
            garbage,
            tabdef: tabdef.clone(),
            records: rec_list,
            tuples,
        })
    }
}

#[cfg(test)]
mod factory_tests {

    use std::path::PathBuf;

    use anyhow::Error;
    use log::{debug, info};

    use crate::{
        factory::{DataValue, DatafileFactory},
        ibd::page::{BasePage, FileSpaceHeaderPageBody, PageTypes, FIL_HEADER_SIZE},
        util,
    };

    // employee schema
    const IBD_DEPT: &str = "data/departments.ibd";
    const IBD_DEPT_MGR: &str = "data/dept_manager.ibd";

    // tb_row_version.sql
    const IBD_RV_0: &str = "data/tb_row_version_0.ibd";
    const IBD_RV_1: &str = "data/tb_row_version_1.ibd";
    const IBD_RV_2: &str = "data/tb_row_version_2.ibd";
    const IBD_RV_3: &str = "data/tb_row_version_3.ibd";
    const IBD_RV_4: &str = "data/tb_row_version_4.ibd";

    #[test]
    fn load_buffer() -> Result<(), Error> {
        util::init_unit_test();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_DEPT))?;
        let buf = fact.fil_hdr_buffer(0)?;
        assert_eq!(buf.len(), FIL_HEADER_SIZE);
        Ok(())
    }

    #[test]
    fn read_fsp_hdr_page() -> Result<(), Error> {
        util::init_unit_test();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_DEPT))?;
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = fact.read_page(0)?;
        // info!("fsp_page={:#?}", fsp_page);
        assert_eq!(fsp_page.fil_hdr.page_type, PageTypes::FSP_HDR);
        assert_eq!(fsp_page.fil_hdr.server_version(), 80037);
        assert_eq!(fsp_page.fil_hdr.space_version(), 1);
        Ok(())
    }

    #[test]
    fn table_revision_01() -> Result<(), Error> {
        util::init_unit_test();
        let mut fact0 = DatafileFactory::from_file(PathBuf::from(IBD_RV_0))?;
        let ret01 = fact0.load_table_def();
        assert!(ret01.is_ok());

        let ret02 = ret01.unwrap();
        let cols = &ret02.col_defs;
        assert_eq!(cols[0].col_name, "c1");
        for col in cols {
            info!("{:?}", col);
        }

        Ok(())
    }

    #[test]
    fn check_unpack_data_01() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_DEPT))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 9);

        let tuples = rs.tuples;

        debug!("tuples={:#?}", tuples);

        // first row
        assert_eq!(tuples[0][0].1, DataValue::Str("d001".into()));
        assert!(matches!(tuples[0][1].1, DataValue::TrxId(_)));
        assert!(matches!(tuples[0][2].1, DataValue::RollPtr(_)));
        assert_eq!(tuples[0][3].1, DataValue::Str("Marketing".into()));

        // last row
        assert_eq!(tuples[8][0].1, DataValue::Str("d009".into()));
        assert!(matches!(tuples[8][1].1, DataValue::TrxId(_)));
        assert!(matches!(tuples[8][2].1, DataValue::RollPtr(_)));
        assert_eq!(tuples[8][3].1, DataValue::Str("Customer Service".into()));

        Ok(())
    }

    #[test]
    fn check_unpack_data_02() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_DEPT_MGR))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 24);

        let tuples = rs.tuples;

        info!("first={:?}", tuples[0]);

        // check row name
        assert_eq!(tuples[0][0].0, "emp_no");
        assert_eq!(tuples[0][1].0, "dept_no");
        assert_eq!(tuples[0][2].0, "DB_TRX_ID");
        assert_eq!(tuples[0][3].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][4].0, "from_date");
        assert_eq!(tuples[0][5].0, "to_date");

        // first row
        assert_eq!(tuples[0][0].1, DataValue::I32(110022));
        assert_eq!(tuples[0][1].1, DataValue::Str("d001".into()));
        assert!(matches!(tuples[0][2].1, DataValue::TrxId(_)));
        assert!(matches!(tuples[0][3].1, DataValue::RollPtr(_)));
        assert_eq!(tuples[0][4].1, DataValue::Date(util::dateval("1985-01-01")));
        assert_eq!(tuples[0][5].1, DataValue::Date(util::dateval("1991-10-01")));

        Ok(())
    }
}

#[cfg(test)]
mod factory_tests_run {

    use std::path::PathBuf;

    use anyhow::Error;
    use colored::Colorize;
    use log::info;

    use crate::{
        factory::DatafileFactory,
        ibd::page::{BasePage, FileSpaceHeaderPageBody, INodePageBody, IndexPageBody},
        util,
    };

    // const IBD_FILE: &str = "/opt/mysql/data/employees/employees.ibd";
    const IBD_FILE: &str = "/opt/mysql/data/rtc/test01.ibd";

    // #[test]
    fn btr_traverse() -> Result<(), Error> {
        util::init_unit_test();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;

        let root_page: BasePage<IndexPageBody> = fact.read_page(4)?;
        let fseg_hdr = root_page.page_body.fseg_hdr;
        info!("fseg_hdr={:?}", &fseg_hdr);

        let inode_page_no = fseg_hdr.leaf_page_no as usize;
        let inode_page: BasePage<INodePageBody> = fact.read_page(inode_page_no)?;

        let offset = fseg_hdr.leaf_offset as usize;
        let head_inode = inode_page
            .page_body
            .inode_ent_list
            .iter()
            .find(|node| node.addr == offset)
            .unwrap();
        info!("head_inode={:#?}", head_inode);

        Ok(())
    }

    // #[test]
    fn leaf_walk_full() -> Result<(), Error> {
        util::init_unit_test();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;

        let page0: BasePage<FileSpaceHeaderPageBody> = fact.read_page(0)?;
        // info!("xdes={:#?}", &page0.page_body);

        let inode_free_first = page0.page_body.fsp_hdr.inodes_free.first.clone();

        let inode_page_no = inode_free_first.page as usize;
        let page2: BasePage<INodePageBody> = fact.read_page(inode_page_no)?;
        // info!("inode={:#?}", &page2.page_body);

        let inode_nonleaf = &page2.page_body.inode_ent_list[2];
        info!("inode_nonleaf={:#?}", &inode_nonleaf);
        let inode_leaf = &page2.page_body.inode_ent_list[3];
        info!("inode_leaf={:#?}", &inode_leaf);

        let mut faddr = &inode_leaf.fseg_full.first;
        let mut seq = 1;
        loop {
            assert_eq!(faddr.page, 0);

            let boffset = faddr.boffset as usize;
            let xdes = page0
                .page_body
                .xdes_ent_list
                .iter()
                .find(|xdes| xdes.flst_node.addr == boffset);
            if xdes.is_none() {
                break;
            }
            info!("seq={}, xdes={:?}", &seq, &xdes);

            faddr = &xdes.unwrap().flst_node.next;
            if faddr.boffset == 0 {
                break;
            }

            seq += 1;
        }

        Ok(())
    }

    // #[test]
    fn leaf_walk_frag() -> Result<(), Error> {
        util::init_unit_test();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;

        let page0: BasePage<FileSpaceHeaderPageBody> = fact.read_page(0)?;
        // info!("xdes={:#?}", &page0.page_body);

        let inode_free_first = page0.page_body.fsp_hdr.inodes_free.first.clone();

        let inode_page_no = inode_free_first.page as usize;
        let page2: BasePage<INodePageBody> = fact.read_page(inode_page_no)?;
        // info!("inode={:#?}", &page2.page_body);

        let inode_leaf = &page2.page_body.inode_ent_list[3];
        info!("inode_leaf={:#?}", &inode_leaf);

        let mut faddr = &inode_leaf.fseg_not_full.first;
        let mut seq = 1;
        loop {
            assert_eq!(faddr.page, 0);

            let boffset = faddr.boffset as usize;
            let xdes = page0
                .page_body
                .xdes_ent_list
                .iter()
                .find(|xdes| xdes.flst_node.addr == boffset);
            if xdes.is_none() {
                break;
            }
            info!("seq={}, xdes={:?}", &seq, &xdes);

            faddr = &xdes.unwrap().flst_node.next;
            if faddr.boffset == 0 {
                break;
            }

            seq += 1;
        }

        Ok(())
    }

    // #[test]
    fn nonleaf_walk_full() -> Result<(), Error> {
        util::init_unit_test();
        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;

        let page0: BasePage<FileSpaceHeaderPageBody> = fact.read_page(0)?;
        // info!("xdes={:#?}", &page0.page_body);

        let inode_free_first = page0.page_body.fsp_hdr.inodes_free.first.clone();

        let inode_page_no = inode_free_first.page as usize;
        let page2: BasePage<INodePageBody> = fact.read_page(inode_page_no)?;
        // info!("inode={:#?}", &page2.page_body);

        let inode_nonleaf = &page2.page_body.inode_ent_list[2];
        info!("inode_nonleaf={:#?}", &inode_nonleaf);
        let inode_leaf = &page2.page_body.inode_ent_list[3];
        info!("inode_leaf={:#?}", &inode_leaf);

        let mut faddr = &inode_nonleaf.fseg_full.first;
        let mut seq = 1;
        loop {
            assert_eq!(faddr.page, 0);

            let boffset = faddr.boffset as usize;
            let xdes = page0
                .page_body
                .xdes_ent_list
                .iter()
                .find(|xdes| xdes.flst_node.addr == boffset);
            if xdes.is_none() {
                break;
            }
            info!("seq={}, xdes={:?}", &seq, &xdes);

            faddr = &xdes.unwrap().flst_node.next;
            if faddr.boffset == 0 {
                break;
            }

            seq += 1;
        }

        Ok(())
    }

    #[test]
    fn unpack_5th_index_page() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;
        let ans = fact.unpack_index_page(4, false);
        info!("{:?}", ans);
        assert!(ans.is_ok());

        for (ith, tuple) in ans.unwrap().tuples[..5].iter().enumerate() {
            info!("[{}]=> {:?}", ith.to_string().green(), tuple);
        }

        Ok(())
    }
}
