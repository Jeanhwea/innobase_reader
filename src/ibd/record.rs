use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use anyhow::Error;
use bytes::Bytes;
use colored::Colorize;
use derivative::Derivative;
use log::{debug, info};
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use crate::{
    ibd::{
        page::{PAGE_NONE, RECORD_HEADER_SIZE},
        record::RecordStatus::NODE_PTR,
    },
    meta::def::{HiddenTypes, TableDef},
    util,
    util::align8,
};

pub const REC_N_FIELDS_ONE_BYTE_MAX: u8 = 0x7f;
pub const REC_NODE_PTR_SIZE: usize = 4;

/// Record Status, rec.h:152
#[repr(u8)]
#[derive(Debug, Display, Default, Eq, PartialEq, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum RecordStatus {
    ORDINARY = 0,
    NODE_PTR = 1,
    INFIMUM = 2,
    SUPREMUM = 3,
    #[default]
    UNDEF,
}

/// Record Info Flag
#[derive(Debug, Display, Eq, PartialEq, Clone)]
pub enum RecInfoFlag {
    /// Mark current column is minimum record
    MIN_REC,

    /// Mark current column is deleted
    DELETED,

    /// Version flag, see INSTANT ADD and DROP Column blog series,
    /// [intro](https://blogs.oracle.com/mysql/post/mysql-80-instant-add-drop-columns),
    /// [design](https://blogs.oracle.com/mysql/post/mysql-80-instant-add-and-drop-columns-2)
    ///
    VERSION,

    /// Instant Column DDL flag,
    /// see [WL#11250: Support Instant Add Column](https://dev.mysql.com/worklog/task/?id=11250)
    INSTANT,
}

/// Record Header
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RecordHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// Original byte for info_bits
    #[derivative(Debug = "ignore")]
    info_byte: u8,

    /// (4 bits) info_bits, MIN_REC/DELETED/VERSION/INSTANT flags, see rec.h, 4 bits
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub info_bits: Vec<RecInfoFlag>,

    /// (4 bits) Number of owned records
    pub n_owned: u8,

    /// (13 bits) Heap Number
    pub heap_no: u16,

    /// (3 bits) Record Status, see rec.h
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub rec_status: RecordStatus,

    /// (2 bytes) Next record offset
    pub next_rec_offset: i16,
}

impl RecordHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let b0 = buf[addr];
        let b1 = util::u16_val(&buf, addr + 1);
        debug!("rec_hdr, b0=0x{:0x?}, b1=0x{:0x?}", b0, b1);

        let mut flags = Vec::new();
        if (b0 & Self::REC_INFO_MIN_REC_FLAG) > 0 {
            flags.push(RecInfoFlag::MIN_REC);
        }
        if (b0 & Self::REC_INFO_DELETED_FLAG) > 0 {
            flags.push(RecInfoFlag::DELETED);
        }
        if (b0 & Self::REC_INFO_VERSION_FLAG) > 0 {
            flags.push(RecInfoFlag::VERSION);
        }
        if (b0 & Self::REC_INFO_INSTANT_FLAG) > 0 {
            flags.push(RecInfoFlag::INSTANT);
        }

        let status = ((b1 & 0x0007) as u8).into();

        Self {
            info_byte: b0,
            info_bits: flags,
            n_owned: b0 & 0x0f,
            heap_no: (b1 & 0xfff8) >> 3,
            rec_status: status,
            next_rec_offset: util::i16_val(&buf, addr + 3),
            buf: buf.clone(),
            addr,
        }
    }

    pub fn next_addr(&self) -> usize {
        ((self.addr as i16) + self.next_rec_offset) as usize + RECORD_HEADER_SIZE
    }

    /// Info bit denoting the predefined minimum record: this bit is set if and
    /// only if the record is the first user record on a non-leaf B-tree page
    /// that is the leftmost page on its level (PAGE_LEVEL is nonzero and
    /// FIL_PAGE_PREV is FIL_NULL).
    const REC_INFO_MIN_REC_FLAG: u8 = 0x10;

    /// The deleted flag in info bits; when bit is set to 1, it means the record
    /// has been delete marked
    const REC_INFO_DELETED_FLAG: u8 = 0x20;

    /// Use this bit to indicate record has version
    const REC_INFO_VERSION_FLAG: u8 = 0x40;

    /// The instant ADD COLUMN flag. When it is set to 1, it means this record
    /// was inserted/updated after an instant ADD COLUMN.
    const REC_INFO_INSTANT_FLAG: u8 = 0x80;

    pub fn is_min_rec(&self) -> bool {
        (self.info_byte & Self::REC_INFO_MIN_REC_FLAG) > 0
    }

    pub fn is_deleted(&self) -> bool {
        (self.info_byte & Self::REC_INFO_DELETED_FLAG) > 0
    }

    pub fn is_version(&self) -> bool {
        (self.info_byte & Self::REC_INFO_VERSION_FLAG) > 0
    }

    pub fn is_instant(&self) -> bool {
        (self.info_byte & Self::REC_INFO_INSTANT_FLAG) > 0
    }
}

/// Field metadata
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FieldMeta {
    /// page offset
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// column opx
    pub opx: usize,

    /// is null value
    pub isnull: bool,

    /// row length
    pub length: usize,

    /// physical exists
    pub phy_exist: bool,

    /// logical exists
    pub log_exist: bool,
}

/// Field datum, data bytes
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FieldDatum {
    /// column opx
    pub opx: usize,

    /// page offset
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// row buffer
    pub rbuf: Option<Bytes>,
}

/// Row Info, var_area and nil_area
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RowInfo {
    /// page address, [nilfld, varfld], access in reverse order
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// instant add column flag
    pub instant_flag: bool,

    /// number of instant column
    pub n_instant_col: u8,

    /// row version
    pub row_version: u8,

    /// record status
    pub rec_status: RecordStatus,

    /// table definition
    #[derivative(Debug = "ignore")]
    pub table_def: Arc<TableDef>,

    /// which index in table_def
    pub index_pos: usize, // &tabdef.clone().idx_defs[index_pos]
}

impl RowInfo {
    pub fn new(rec_hdr: &RecordHeader, tabdef: Arc<TableDef>, index_pos: usize) -> Self {
        let buf = rec_hdr.buf.clone();

        // Handle the row version
        let row_ver = if rec_hdr.is_version() {
            buf[rec_hdr.addr - 1]
        } else {
            0
        };

        // Handle the row version
        let n_ins_col = if rec_hdr.is_instant() {
            buf[rec_hdr.addr - 1]
        } else {
            0
        };

        Self {
            table_def: tabdef.clone(),
            index_pos,
            instant_flag: rec_hdr.is_instant(),
            n_instant_col: n_ins_col,
            row_version: row_ver,
            rec_status: rec_hdr.rec_status.clone(),
            buf: buf.clone(),
            addr: rec_hdr.addr,
        }
    }

    // see function in mysql-server source code
    // static inline uint8_t rec_get_n_fields_length(ulint n_fields) {
    //   return (n_fields > REC_N_FIELDS_ONE_BYTE_MAX ? 2 : 1);
    // }
    pub fn field_byte(data_len: u32) -> usize {
        if data_len > REC_N_FIELDS_ONE_BYTE_MAX as u32 {
            2
        } else {
            1
        }
    }

    /// patch byte for VERSION and INSTANT flags is on
    fn get_patch_byte(&self) -> usize {
        if self.row_version > 0 || self.instant_flag {
            1
        } else {
            0
        }
    }

    pub fn resolve_metadata(&self) -> Result<Vec<FieldMeta>, Error> {
        if self.rec_status == NODE_PTR {
            self.resolve_node_ptr_metadata()
        } else {
            self.resolve_ordinary_metadata()
        }
    }

    /// resolve ordinary record
    pub fn resolve_ordinary_metadata(&self) -> Result<Vec<FieldMeta>, Error> {
        let row_ver = self.row_version as u32;
        let eles = &self.table_def.clone().idx_defs[self.index_pos].elements;
        let cols = &self.table_def.clone().col_defs;

        // for INSTANT flag
        let n_ins_col = if self.instant_flag {
            self.n_instant_col as usize
        } else if self.table_def.instant_col > 0 {
            let n_hidden = cols
                .iter()
                .filter(|col| col.hidden != HiddenTypes::HT_VISIBLE)
                .count();
            n_hidden + self.table_def.instant_col as usize
        } else {
            0
        };

        // for VERSION flag
        let has_phy_pos = cols.iter().any(|col| col.phy_pos >= 0);
        let eles_set = eles.iter().map(|e| e.column_opx).collect::<HashSet<_>>();

        // resolve physical layout
        let phy_layout = if has_phy_pos {
            cols.iter()
                .enumerate()
                .map(|(col_pos, col)| {
                    // case1: current row is dropped
                    let row_dropped = col.version_dropped > 0 && row_ver >= col.version_dropped;
                    // case2: no physical data, use the column default value
                    let use_default = col.version_added > 0 && row_ver < col.version_added;
                    // physical not exists if and only if in case1 and case2, otherwise it exist
                    let phy_exist = !(row_dropped || use_default);
                    // logical exist when index's element contains the column_opx
                    let log_exist = eles_set.contains(&col_pos);
                    (col.phy_pos as usize, (col_pos, phy_exist, log_exist))
                })
                .collect::<BTreeMap<usize, _>>()
        } else if n_ins_col > 0 {
            eles.iter()
                .enumerate()
                .map(|(phy_pos, ele)| {
                    let phy_exist = phy_pos < n_ins_col;
                    (phy_pos, (ele.column_opx, phy_exist, true))
                })
                .collect::<BTreeMap<usize, _>>()
        } else {
            eles.iter()
                .enumerate()
                .map(|(phy_pos, ele)| (phy_pos, (ele.column_opx, true, true)))
                .collect::<BTreeMap<usize, _>>()
        };

        info!(
            "row_version={}, has_phy_pos={}, phy_layout={:?}",
            row_ver,
            has_phy_pos.to_string().yellow(),
            &phy_layout,
        );

        // number of nullable fields
        let n_nilfld = phy_layout
            .values()
            .map(|v| if v.1 && cols[v.0].isnil { 1 } else { 0 })
            .sum();

        // reserve 1 byte for row_version if row_version > 0
        let niladdr = self.addr - self.get_patch_byte();
        let varaddr = niladdr - align8(n_nilfld);
        info!(
            "n_nilfld={}, hdraddr={}, niladdr={}, varaddr={}",
            n_nilfld.to_string().blue(),
            self.addr.to_string().yellow(),
            niladdr.to_string().yellow(),
            varaddr.to_string().yellow()
        );

        let mut row_meta_list = Vec::new();
        let mut nilfld_nth = 0;
        let mut varptr = varaddr;
        let mut fldaddr = self.addr + RECORD_HEADER_SIZE;
        for (phy_pos, (col_pos, phy_exist, log_exist)) in phy_layout {
            let col = &cols[col_pos];
            info!(
                "exist[{},{}] pos[phy={},opx={}] version[row={},added={},dropped={}] => {}",
                if phy_exist {
                    "PHY=Y".green()
                } else {
                    "PHY=N".red()
                },
                if log_exist {
                    "LOG=Y".green()
                } else {
                    "LOG=N".red()
                },
                phy_pos,
                col_pos,
                row_ver,
                col.version_added,
                col.version_dropped,
                col.col_name.magenta(),
            );

            let mut null = false;
            let mut vlen = 0;
            if phy_exist {
                if col.isnil {
                    null = self.is_null(niladdr, nilfld_nth);
                    nilfld_nth += 1;
                }
                if col.isvar {
                    if !null {
                        let (nbyte, len) = self.varfld_len(varptr, col.data_len);
                        info!("col={}, varptr={}, nbyte={}", &col.col_name, varptr, nbyte);
                        varptr -= nbyte;
                        vlen = len;
                    }
                } else {
                    vlen = col.data_len as usize;
                }
            }
            row_meta_list.push(FieldMeta {
                addr: fldaddr,
                opx: col_pos,
                isnull: null,
                length: vlen,
                phy_exist,
                log_exist,
            });
            fldaddr += vlen;
        }

        assert_eq!(nilfld_nth, n_nilfld, "所有可空的字段都应访问到");
        for (i, meta) in row_meta_list.iter().enumerate() {
            debug!("meta[{}]={:?}, {}", i, meta, &cols[meta.opx].col_name);
        }

        Ok(row_meta_list)
    }

    /// resolve node_ptr
    pub fn resolve_node_ptr_metadata(&self) -> Result<Vec<FieldMeta>, Error> {
        let eles = &self.table_def.clone().idx_defs[self.index_pos].elements;
        let cols = &self.table_def.clone().col_defs;

        // resolve physical layout
        let phy_layout = eles
            .iter()
            .enumerate()
            .map(|(phy_pos, ele)| (phy_pos, (ele.column_opx, true, true)))
            .collect::<BTreeMap<usize, _>>();

        let mut row_meta_list = Vec::new();
        let mut varptr = self.addr;
        let mut fldaddr = self.addr + RECORD_HEADER_SIZE;
        for (_, (col_pos, phy_exist, log_exist)) in phy_layout {
            let col = &cols[col_pos];
            if col.hidden == HiddenTypes::HT_HIDDEN_SE && col.col_name != "DB_ROW_ID" {
                break;
            }
            let mut vlen = 0;
            if phy_exist {
                if col.isvar {
                    let (nbyte, len) = self.varfld_len(varptr, col.data_len);
                    varptr -= nbyte;
                    vlen = len;
                } else {
                    vlen = col.data_len as usize;
                }
            }
            row_meta_list.push(FieldMeta {
                addr: fldaddr,
                opx: col_pos,
                isnull: false,
                length: vlen,
                phy_exist,
                log_exist,
            });
            fldaddr += vlen;
        }

        row_meta_list.push(FieldMeta {
            addr: fldaddr,
            opx: PAGE_NONE as usize,
            isnull: false,
            length: REC_NODE_PTR_SIZE,
            phy_exist: true,
            log_exist: true,
        });

        for (i, meta) in row_meta_list.iter().enumerate() {
            if meta.opx == PAGE_NONE as usize {
                continue;
            }
            debug!("meta[{}]={:?}, {}", i, meta, &cols[meta.opx].col_name);
        }

        Ok(row_meta_list)
    }

    fn is_null(&self, niladdr: usize, nilfld_nth: usize) -> bool {
        let null_mask = 1 << util::bitmap_shift(nilfld_nth);
        let null_byte = self.buf[niladdr - util::bitmap_index(nilfld_nth) - 1];
        (null_byte & null_mask) > 0
    }

    fn varfld_len(&self, varptr: usize, data_len: u32) -> (usize, usize) {
        let nbyte_guess = Self::field_byte(data_len);

        let mut nbyte = 1;
        let vlen = match nbyte_guess {
            1 => self.buf[varptr - 1] as usize,
            2 => {
                let b0 = self.buf[varptr - 1] as usize;
                if b0 > REC_N_FIELDS_ONE_BYTE_MAX.into() {
                    nbyte = 2;
                    let b1 = self.buf[varptr - 2] as usize;
                    b1 + ((b0 & (REC_N_FIELDS_ONE_BYTE_MAX as usize)) << 8)
                } else {
                    b0
                }
            }
            _ => panic!("ERR_PROCESS_VAR_FILED_BYTES"),
        };

        (nbyte, vlen)
    }
}

/// Row data
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RowData {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// row info
    pub row_info: Arc<RowInfo>,
    /// row metadata list
    pub meta_list: Vec<FieldMeta>,
    /// row data list
    pub data_list: Vec<FieldDatum>,
}

impl RowData {
    pub fn new(addr: usize, buf: Arc<Bytes>, row_info: Arc<RowInfo>) -> Self {
        let field_meta_list = row_info.resolve_metadata().unwrap();
        let cols = &row_info.table_def.clone().col_defs;
        let field_data_list = field_meta_list
            .iter()
            .filter(|m| m.log_exist)
            .map(|m| FieldDatum {
                opx: m.opx,
                addr: m.addr,
                rbuf: if !m.isnull {
                    if m.phy_exist {
                        Some(buf.slice(m.addr..m.addr + m.length))
                    } else {
                        cols[m.opx].defval.clone()
                    }
                } else {
                    None
                },
            })
            .collect();
        Self {
            buf: buf.clone(),
            addr,
            row_info: row_info.clone(),
            meta_list: field_meta_list,
            data_list: field_data_list,
        }
    }
}

/// Record
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Record {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// row information
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub row_info: Arc<RowInfo>,

    /// (5 bytes) record header
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub rec_hdr: RecordHeader,

    /// row data
    pub row_data: RowData,
}

impl Record {
    pub fn new(
        addr: usize,
        buf: Arc<Bytes>,
        hdr: RecordHeader,
        row_info: Arc<RowInfo>,
        row_data: RowData,
    ) -> Self {
        Self {
            rec_hdr: hdr,
            row_info: row_info.clone(),
            row_data,
            buf: buf.clone(),
            addr,
        }
    }

    pub fn calc_layout(&self) -> RecordLayout {
        let rec_addr = self.addr;
        let cols = &self.row_info.table_def.clone().col_defs;
        let rv_size = if self.row_info.row_version > 0 { 1 } else { 0 };
        let na_size = align8(
            self.row_data
                .meta_list
                .iter()
                .map(|m| {
                    if m.opx == PAGE_NONE as usize {
                        0
                    } else if m.phy_exist && cols[m.opx].isnil {
                        1
                    } else {
                        0
                    }
                })
                .sum(),
        );
        let va_size = self
            .row_data
            .meta_list
            .iter()
            .map(|m| {
                if m.opx == PAGE_NONE as usize {
                    return 0;
                }
                let col = &cols[m.opx];
                if m.phy_exist && col.isvar {
                    RowInfo::field_byte(col.data_len)
                } else {
                    0
                }
            })
            .sum();

        let pd_size = self
            .row_data
            .meta_list
            .iter()
            .map(|m| if m.phy_exist { m.length } else { 0 })
            .sum();

        RecordLayout {
            addr: rec_addr - RECORD_HEADER_SIZE - rv_size - na_size - va_size,
            rec_addr,
            var_area_size: va_size,
            nil_area_size: na_size,
            row_version_size: rv_size,
            rec_hdr_size: RECORD_HEADER_SIZE,
            phy_data_size: pd_size,
            total_size: va_size + na_size + rv_size + RECORD_HEADER_SIZE + pd_size,
        }
    }
}

/// Record Layout
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct RecordLayout {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub rec_addr: usize,

    pub var_area_size: usize,
    pub nil_area_size: usize,
    pub row_version_size: usize,
    pub rec_hdr_size: usize,
    pub phy_data_size: usize,
    pub total_size: usize,
}
