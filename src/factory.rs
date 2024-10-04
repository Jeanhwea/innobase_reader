use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Error, Result};
use bytes::Bytes;
use log::{debug, info, warn};

use crate::{
    ibd::{
        page::{
            BasePage, BasePageBody, FilePageHeader, FileSpaceHeaderPageBody, INodeEntry,
            INodePageBody, IndexHeader, IndexPageBody, SdiPageBody, XDesEntry, XDesPageBody,
            FIL_HEADER_SIZE, INDEX_HEADER_SIZE, PAGE_NONE, PAGE_SIZE,
        },
        record::{DataValue, ResultSet},
        redo::{Blocks, LogBlock, LogCheckpoint, LogFileHeader, OS_FILE_LOG_BLOCK_SIZE},
        undo::RollPtr,
    },
    meta::{
        cst::coll_find,
        def::{ColumnDef, ColumnTypes, HiddenTypes, IndexDef, IndexElementDef, TableDef},
    },
    sdi::record::SdiTableObject,
    util::{
        u32_val, unpack_datetime2_val, unpack_enum_val, unpack_i32_val, unpack_i64_val,
        unpack_newdate_val, unpack_timestamp2_val, unpack_u48_val, unpack_u56_val,
    },
};

pub const SDI_META_INFO_MIN_VER: u32 = 80000;

#[derive(Debug)]
pub struct DatafileFactory {
    /// target datafile
    pub target: PathBuf,

    /// data file handler
    pub file_handler: File,

    /// data file size
    pub file_size: usize,

    /// segment descriptor cache, the inode cache, map[page_no, boffset] => INodeEntry
    pub inode_cache: HashMap<usize, HashMap<u16, INodeEntry>>,

    /// extent descriptor cache, map[page_no, boffset] => XDesEntry
    pub extent_cache: HashMap<usize, HashMap<u16, XDesEntry>>,
}

impl DatafileFactory {
    /// construct the datafile factory
    pub fn from_file(target: PathBuf) -> Result<Self> {
        if !target.exists() {
            return Err(Error::msg(format!("没有找到目标文件: {:?}", target)));
        }

        let file = File::open(&target)?;
        let size = file.metadata()?.len() as usize;

        info!("加载数据文件: {:?}", &file);

        Ok(Self {
            target,
            file_size: size,
            file_handler: file,
            inode_cache: HashMap::new(),
            extent_cache: HashMap::new(),
        })
    }

    /// get file buffer
    pub fn file_buffer(&mut self) -> Result<Arc<Bytes>> {
        let mut buffer = vec![];
        self.file_handler.read_to_end(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    /// count the log block
    pub fn block_count(&self) -> usize {
        self.file_size / OS_FILE_LOG_BLOCK_SIZE
    }

    /// get block buffer
    pub fn block_buffer(&mut self, block_no: usize) -> Result<Arc<Bytes>> {
        if block_no >= self.block_count() {
            return Err(Error::msg(format!("块号范围溢出: block_no={}", block_no)));
        }

        let offset = (block_no * OS_FILE_LOG_BLOCK_SIZE) as u64;
        self.file_handler.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0; OS_FILE_LOG_BLOCK_SIZE];
        self.file_handler.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    /// count the page
    pub fn page_count(&self) -> usize {
        self.file_size / PAGE_SIZE
    }

    /// get page buffer
    pub fn page_buffer(&mut self, page_no: usize) -> Result<Arc<Bytes>> {
        if page_no >= self.page_count() {
            return Err(Error::msg(format!("页码范围溢出: page_no={}", page_no)));
        }

        let offset = (page_no * PAGE_SIZE) as u64;
        self.file_handler.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0; PAGE_SIZE];
        self.file_handler.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    /// get file header buffer
    pub fn fil_hdr_buffer(&mut self, page_no: usize) -> Result<Arc<Bytes>> {
        if page_no >= self.page_count() {
            return Err(Error::msg(format!("页码范围溢出: page_no={}", page_no)));
        }

        let offset = (page_no * PAGE_SIZE) as u64;
        self.file_handler.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0; FIL_HEADER_SIZE];
        self.file_handler.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    pub fn idx_hdr_buffer(&mut self, page_no: usize) -> Result<Arc<Bytes>> {
        if page_no >= self.page_count() {
            return Err(Error::msg(format!("页码范围溢出: page_no={}", page_no)));
        }

        let offset = (page_no * PAGE_SIZE + FIL_HEADER_SIZE) as u64;
        self.file_handler.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0; INDEX_HEADER_SIZE];
        self.file_handler.read_exact(&mut buffer)?;
        Ok(Arc::new(Bytes::from(buffer)))
    }

    pub fn read_fil_hdr(&mut self, page_no: usize) -> Result<FilePageHeader> {
        let buf = self.fil_hdr_buffer(page_no)?;
        Ok(FilePageHeader::new(0, buf.clone()))
    }

    pub fn read_idx_hdr(&mut self, page_no: usize) -> Result<IndexHeader> {
        let buf = self.idx_hdr_buffer(page_no)?;
        Ok(IndexHeader::new(0, buf.clone()))
    }

    pub fn read_page<P>(&mut self, page_no: usize) -> Result<BasePage<P>>
    where
        P: BasePageBody,
    {
        let buf = self.page_buffer(page_no)?;
        Ok(BasePage::new(0, buf.clone()))
    }

    pub fn read_block(&mut self, block_no: usize) -> Result<Blocks> {
        let buf = self.block_buffer(block_no)?;
        let data = match block_no {
            0 => Blocks::FileHeader(LogFileHeader::new(0, buf)),
            2 => Blocks::Unused,
            1 | 3 => {
                let chk = LogCheckpoint::new(0, buf);
                if chk.checksum > 0 {
                    Blocks::Checkpoint(chk)
                } else {
                    Blocks::Unused
                }
            }
            _ => {
                let blk = LogBlock::new(0, buf);
                if blk.checksum > 0 {
                    Blocks::Block(blk)
                } else {
                    Blocks::Unused
                }
            }
        };
        Ok(data)
    }

    pub fn read_inode_entry(&mut self, page_no: usize, boffset: u16) -> Result<INodeEntry> {
        let inode = match self.inode_cache.get(&page_no) {
            Some(inode_map) => inode_map
                .get(&boffset)
                .expect("未找到 INodeEntry 数据项")
                .clone(),
            None => {
                let inode_map = self
                    .read_page::<INodePageBody>(page_no)?
                    .page_body
                    .inode_ent_list
                    .iter()
                    .map(|ent| (ent.addr as u16, ent.clone()))
                    .collect::<HashMap<_, _>>();
                let inode_entry = inode_map
                    .get(&boffset)
                    .expect("未找到 INodeEntry 数据项")
                    .clone();
                self.inode_cache.insert(page_no, inode_map);
                inode_entry
            }
        };

        Ok(inode)
    }

    pub fn read_xdes_entry(&mut self, page_no: usize, boffset: u16) -> Result<XDesEntry> {
        let xdes = match self.extent_cache.get(&page_no) {
            Some(xdes_map) => xdes_map
                .get(&boffset)
                .expect("未找到 XDesEntry 数据项")
                .clone(),
            None => {
                let xdes_map = self
                    .read_page::<XDesPageBody>(page_no)?
                    .page_body
                    .xdes_ent_list
                    .iter()
                    .map(|ent| (ent.flst_node.addr as u16, ent.clone()))
                    .collect::<HashMap<_, _>>();
                let xdes_entry = xdes_map
                    .get(&boffset)
                    .expect("未找到 XDesEntry 数据项")
                    .clone();
                self.extent_cache.insert(page_no, xdes_map);
                xdes_entry
            }
        };

        Ok(xdes)
    }

    fn read_sdi_page(&mut self) -> Result<BasePage<SdiPageBody>, Error> {
        let fsp_page: BasePage<FileSpaceHeaderPageBody> = self.read_page(0)?;
        if fsp_page.fil_hdr.server_version() < SDI_META_INFO_MIN_VER {
            return Err(Error::msg("数据文件版本过低，没有表元信息"));
        }
        let sdi_meta = fsp_page.page_body.sdi_meta();
        let sdi_page_no: usize = sdi_meta.sdi_page_no.into();
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

        let sdi_str = sdi_page.page_body.get_tabdef_str()?;
        let dd_object = SdiTableObject::from_str(&sdi_str)?.dd_object;
        debug!("dd_object={:#?}", &dd_object);

        let coll = coll_find(dd_object.collation_id);
        info!("当前文件字符集: {:?}", &coll);

        let coldefs = dd_object
            .columns
            .iter()
            .map(ColumnDef::from)
            .collect::<Vec<_>>();
        let idxdefs = dd_object
            .indexes
            .iter()
            .map(|idx| {
                let ele_defs: Vec<IndexElementDef> = idx
                    .elements
                    .iter()
                    .map(|ele| {
                        let ref_col = &coldefs[ele.column_opx as usize];
                        IndexElementDef::from(ele, ref_col)
                    })
                    .collect();

                IndexDef::from(idx, ele_defs)
            })
            .collect();
        debug!("idxdefs={:?}", &idxdefs);

        Ok(Arc::from(TableDef::from(
            &dd_object, coll, coldefs, idxdefs,
        )))
    }

    pub fn unpack_index_page(&mut self, page_no: usize, garbage: bool) -> Result<ResultSet, Error> {
        let page: BasePage<IndexPageBody> = self.read_page(page_no)?;
        // let page_level = page.page_body.idx_hdr.page_level;
        // if page_level != 0 {
        //     return Err(Error::msg(format!(
        //         "不支持查看非叶子节点: page_level={:?}",
        //         page_level
        //     )));
        // }

        let tabdef = self.load_table_def()?;
        let index_id = page.page_body.idx_hdr.page_index_id;
        let index = match tabdef
            .idx_defs
            .iter()
            .enumerate()
            .find(|idx| idx.1.idx_id == index_id as i32)
        {
            Some(val) => val,
            None => {
                return Err(Error::msg(format!(
                    "未找到索引的元信息: index_id={}",
                    index_id
                )));
            }
        };
        info!("当前页所引用的索引: index_name={}", index.1.idx_name);

        let rec_list = if garbage {
            page.page_body.read_free_records(tabdef.clone(), index.0)?
        } else {
            page.page_body.read_user_records(tabdef.clone(), index.0)?
        };
        debug!("rec_list={:?}", rec_list);

        let tuples = rec_list
            .iter()
            .map(|rec| {
                rec.row_data
                    .data_list
                    .iter()
                    .map(|d| {
                        if d.opx == PAGE_NONE as usize {
                            return (
                                "NODE_PTR".to_string(),
                                DataValue::PageNo(u32_val(&d.rbuf.clone().unwrap(), 0)),
                            );
                        }
                        let col = &tabdef.col_defs[d.opx];
                        let val = match &d.rbuf {
                            Some(b) => match col.hidden {
                                HiddenTypes::HT_VISIBLE => match col.dd_type {
                                    ColumnTypes::LONG => DataValue::I32(unpack_i32_val(b)),
                                    ColumnTypes::LONGLONG => DataValue::I64(unpack_i64_val(b)),
                                    ColumnTypes::NEWDATE => DataValue::Date(
                                        unpack_newdate_val(b)
                                            .unwrap_or_else(|| panic!("日期格式错误: {:?}", &d)),
                                    ),
                                    ColumnTypes::DATETIME2 => DataValue::DateTime(
                                        unpack_datetime2_val(b)
                                            .unwrap_or_else(|| panic!("时间格式错误: {:?}", &d)),
                                    ),
                                    ColumnTypes::TIMESTAMP2 => {
                                        DataValue::Timestamp(unpack_timestamp2_val(b))
                                    }
                                    ColumnTypes::VARCHAR
                                    | ColumnTypes::VAR_STRING
                                    | ColumnTypes::STRING => {
                                        let barr = b.to_vec();
                                        let text = std::str::from_utf8(&barr)
                                            .unwrap_or_else(|_| panic!("字符串格式错误: {:?}", &d));
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
                                    "DB_ROLL_PTR" => {
                                        DataValue::RbPtr(RollPtr::new(unpack_u56_val(b)))
                                    }
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
    use bytes::Bytes;
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

    // tb_instant_col.sql
    const IBD_IC_0: &str = "data/tb_instant_col_0.ibd";
    const IBD_IC_1: &str = "data/tb_instant_col_1.ibd";
    const IBD_IC_2: &str = "data/tb_instant_col_2.ibd";

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

        // Initial 0: columns [c1, c2, c3, c4]
        let rv0 = &DatafileFactory::from_file(PathBuf::from(IBD_RV_0))?
            .load_table_def()?
            .col_defs;
        assert_eq!(rv0[0].col_name, "c1");
        assert_eq!(rv0[0].defval, None);
        assert_eq!(rv0[3].col_name, "c4");
        assert_eq!(rv0[3].defval, None);

        // Revision 1: add c5, columns [c1, c2, c3, c4, c5]
        let rv1 = &DatafileFactory::from_file(PathBuf::from(IBD_RV_1))?
            .load_table_def()?
            .col_defs;
        assert_eq!(rv1[0].col_name, "c1");
        assert_eq!(rv1[0].defval, None);
        assert_eq!(rv1[3].col_name, "c4");
        assert_eq!(rv1[3].defval, None);
        assert_eq!(rv1[4].col_name, "c5");
        assert_eq!(rv1[4].version_added, 1);
        assert_eq!(rv1[4].defval, Some(Bytes::from("c5_def    ")));

        // Revision 2: drop c3, columns [c1, c2, c4, c5]
        let rv2 = &DatafileFactory::from_file(PathBuf::from(IBD_RV_3))?
            .load_table_def()?
            .col_defs;
        assert_eq!(rv2[0].col_name, "c1");
        assert_eq!(rv2[0].defval, None);
        assert_eq!(rv2[2].col_name, "c4");
        assert_eq!(rv2[2].defval, None);
        assert_eq!(rv2[3].col_name, "c5");
        assert_eq!(rv2[3].version_added, 1);
        assert_eq!(rv2[3].defval, Some(Bytes::from("c5_def    ")));
        assert!(rv2[7].col_name.ends_with("c3"));
        assert_eq!(rv2[7].version_added, 0);
        assert_eq!(rv2[7].version_dropped, 2);
        assert_eq!(rv2[7].defval, None);

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
        assert!(matches!(tuples[0][2].1, DataValue::RbPtr(_)));
        assert_eq!(tuples[0][3].1, DataValue::Str("Marketing".into()));

        // last row
        assert_eq!(tuples[8][0].1, DataValue::Str("d009".into()));
        assert!(matches!(tuples[8][1].1, DataValue::TrxId(_)));
        assert!(matches!(tuples[8][2].1, DataValue::RbPtr(_)));
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
        assert!(matches!(tuples[0][3].1, DataValue::RbPtr(_)));
        assert_eq!(tuples[0][4].1, DataValue::Date(util::dateval("1985-01-01")));
        assert_eq!(tuples[0][5].1, DataValue::Date(util::dateval("1991-10-01")));

        Ok(())
    }

    #[test]
    fn instant_col_unpack_00() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_IC_0))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 2);
        let tuples = rs.tuples;
        assert_eq!(tuples[0].len(), 6);

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "k1");
        assert_eq!(tuples[0][4].0, "c1");
        assert_eq!(tuples[0][5].0, "c2");

        // first row
        assert_eq!(tuples[0][3].1, DataValue::I32(1));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c1".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c2".into()));

        Ok(())
    }

    #[test]
    fn instant_col_unpack_01() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_IC_1))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 2);
        let tuples = rs.tuples;

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "k1");
        assert_eq!(tuples[0][4].0, "c1");
        assert_eq!(tuples[0][5].0, "c2");
        assert_eq!(tuples[0][6].0, "c3");

        // first row
        assert_eq!(tuples[0][3].1, DataValue::I32(1));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c1".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c2".into()));
        assert_eq!(tuples[0][6].1, DataValue::Str("c3_def".into()));

        assert_eq!(tuples[1][3].1, DataValue::I32(2));
        assert_eq!(tuples[1][4].1, DataValue::Str("r2c1".into()));
        assert_eq!(tuples[1][5].1, DataValue::Str("r2c2".into()));
        assert_eq!(tuples[1][6].1, DataValue::Str("c3_def".into()));

        Ok(())
    }

    #[test]
    fn instant_col_unpack_02() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_IC_2))?;
        let rs = fact.unpack_index_page(4, false)?;

        assert_eq!(rs.tuples.len(), 3);
        let tuples = rs.tuples;

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "k1");
        assert_eq!(tuples[0][4].0, "c1");
        assert_eq!(tuples[0][5].0, "c2");
        assert_eq!(tuples[0][6].0, "c3");

        // rows
        assert_eq!(tuples[0][3].1, DataValue::I32(1));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c1".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c2".into()));
        assert_eq!(tuples[0][6].1, DataValue::Str("c3_def".into()));

        assert_eq!(tuples[1][3].1, DataValue::I32(2));
        assert_eq!(tuples[1][4].1, DataValue::Str("r2c1".into()));
        assert_eq!(tuples[1][5].1, DataValue::Str("r2c2".into()));
        assert_eq!(tuples[1][6].1, DataValue::Str("c3_def".into()));

        assert_eq!(tuples[2][3].1, DataValue::I32(3));
        assert_eq!(tuples[2][4].1, DataValue::Str("r3c1".into()));
        assert_eq!(tuples[2][5].1, DataValue::Str("r3c2".into()));
        assert_eq!(tuples[2][6].1, DataValue::Str("r3c3".into()));

        Ok(())
    }

    #[test]
    fn row_version_unpack_00() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_RV_0))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 1);
        let tuples = rs.tuples;

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "c1");
        assert_eq!(tuples[0][4].0, "c2");
        assert_eq!(tuples[0][5].0, "c3");
        assert_eq!(tuples[0][6].0, "c4");

        // first row
        assert_eq!(tuples[0][3].1, DataValue::Str("r1c1      ".into()));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c2      ".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c3      ".into()));
        assert_eq!(tuples[0][6].1, DataValue::Str("r1c4      ".into()));

        Ok(())
    }

    #[test]
    fn row_version_unpack_01() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_RV_1))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 1);
        let tuples = rs.tuples;

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "c1");
        assert_eq!(tuples[0][4].0, "c2");
        assert_eq!(tuples[0][5].0, "c3");
        assert_eq!(tuples[0][6].0, "c4");
        assert_eq!(tuples[0][7].0, "c5");

        // first row
        assert_eq!(tuples[0][3].1, DataValue::Str("r1c1      ".into()));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c2      ".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c3      ".into()));
        assert_eq!(tuples[0][6].1, DataValue::Str("r1c4      ".into()));
        assert_eq!(tuples[0][7].1, DataValue::Str("c5_def    ".into()));

        Ok(())
    }

    #[test]
    fn row_version_unpack_02() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_RV_2))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 2);
        let tuples = rs.tuples;

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "c1");
        assert_eq!(tuples[0][4].0, "c2");
        assert_eq!(tuples[0][5].0, "c3");
        assert_eq!(tuples[0][6].0, "c4");
        assert_eq!(tuples[0][7].0, "c5");

        // row 0
        assert_eq!(tuples[0][3].1, DataValue::Str("r1c1      ".into()));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c2      ".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c3      ".into()));
        assert_eq!(tuples[0][6].1, DataValue::Str("r1c4      ".into()));
        assert_eq!(tuples[0][7].1, DataValue::Str("c5_def    ".into()));

        // row 1
        assert_eq!(tuples[1][3].1, DataValue::Str("r2c1      ".into()));
        assert_eq!(tuples[1][4].1, DataValue::Str("r2c2      ".into()));
        assert_eq!(tuples[1][5].1, DataValue::Str("r2c3      ".into()));
        assert_eq!(tuples[1][6].1, DataValue::Str("r2c4      ".into()));
        assert_eq!(tuples[1][7].1, DataValue::Str("r2c5      ".into()));

        Ok(())
    }

    #[test]
    fn row_version_unpack_03() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_RV_3))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 2);
        let tuples = rs.tuples;

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "c1");
        assert_eq!(tuples[0][4].0, "c2");
        assert_eq!(tuples[0][5].0, "c4");
        assert_eq!(tuples[0][6].0, "c5");

        // row 0
        assert_eq!(tuples[0][3].1, DataValue::Str("r1c1      ".into()));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c2      ".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c4      ".into()));
        assert_eq!(tuples[0][6].1, DataValue::Str("c5_def    ".into()));

        // row 1
        assert_eq!(tuples[1][3].1, DataValue::Str("r2c1      ".into()));
        assert_eq!(tuples[1][4].1, DataValue::Str("r2c2      ".into()));
        assert_eq!(tuples[1][5].1, DataValue::Str("r2c4      ".into()));
        assert_eq!(tuples[1][6].1, DataValue::Str("r2c5      ".into()));

        Ok(())
    }

    #[test]
    fn row_version_unpack_04() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_RV_4))?;
        let ans = fact.unpack_index_page(4, false);
        assert!(ans.is_ok());

        let rs = ans.unwrap();
        assert_eq!(rs.tuples.len(), 3);
        let tuples = rs.tuples;

        // check row name
        assert_eq!(tuples[0][0].0, "DB_ROW_ID");
        assert_eq!(tuples[0][1].0, "DB_TRX_ID");
        assert_eq!(tuples[0][2].0, "DB_ROLL_PTR");
        assert_eq!(tuples[0][3].0, "c1");
        assert_eq!(tuples[0][4].0, "c2");
        assert_eq!(tuples[0][5].0, "c4");
        assert_eq!(tuples[0][6].0, "c5");

        // row 0
        assert_eq!(tuples[0][3].1, DataValue::Str("r1c1      ".into()));
        assert_eq!(tuples[0][4].1, DataValue::Str("r1c2      ".into()));
        assert_eq!(tuples[0][5].1, DataValue::Str("r1c4      ".into()));
        assert_eq!(tuples[0][6].1, DataValue::Str("c5_def    ".into()));

        // row 1
        assert_eq!(tuples[1][3].1, DataValue::Str("r2c1      ".into()));
        assert_eq!(tuples[1][4].1, DataValue::Str("r2c2      ".into()));
        assert_eq!(tuples[1][5].1, DataValue::Str("r2c4      ".into()));
        assert_eq!(tuples[1][6].1, DataValue::Str("r2c5      ".into()));

        // row 2
        assert_eq!(tuples[2][3].1, DataValue::Str("r3c1      ".into()));
        assert_eq!(tuples[2][4].1, DataValue::Str("r3c2      ".into()));
        assert_eq!(tuples[2][5].1, DataValue::Str("r3c4      ".into()));
        assert_eq!(tuples[2][6].1, DataValue::Str("r3c5      ".into()));

        Ok(())
    }
}

#[cfg(test)]
mod factory_tests_run {

    use std::path::PathBuf;

    use anyhow::Error;

    use crate::{
        factory::DatafileFactory,
        ibd::page::{BasePage, IndexPageBody, PageNumber, PageTypes},
        util,
    };

    // const IBD_FILE: &str = "/opt/mysql/data/employees/employees.ibd";
    // const IBD_FILE: &str = "/opt/docker/mysql80027/rtc80027/tt.ibd";
    const IBD_FILE: &str = "/opt/mysql/data/rtc/t500w.ibd";

    // #[test]
    fn entry() -> Result<(), Error> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(IBD_FILE))?;
        for page_no in 0..fact.page_count() {
            let hdr = fact.read_fil_hdr(page_no)?;
            if hdr.page_type != PageTypes::INDEX {
                continue;
            }
            let idx: BasePage<IndexPageBody> = fact.read_page(page_no)?;
            let fs = idx.page_body.fseg_hdr_0;
            if !matches!(fs.page_no, PageNumber::None) {
                println!("page_no={}, {:#?}", page_no, fs);
            }
        }

        Ok(())
    }
}
