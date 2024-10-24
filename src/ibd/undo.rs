use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;
use log::info;
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use super::page::{
    FlstNode, PageNumber, SpaceId, UndoPageHeader, UndoPageTypes, FIL_HEADER_SIZE, UNIV_PAGE_SIZE,
};
use crate::{ibd::dict, util};

/// XID data size
pub const XIDDATASIZE: usize = 128;

/// undo log, see trx0undo.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoLog {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (186 bytes) undo log header
    pub undo_log_hdr: UndoLogHeader,

    /// undo record headers
    // #[derivative(Debug(format_with = "util::fmt_oneline_vec"))]
    pub undo_rec_list: Vec<UndoRecord>,
}

impl UndoLog {
    pub fn new(addr: usize, buf: Arc<Bytes>, page_hdr: &UndoPageHeader) -> Self {
        let log_hdr = UndoLogHeader::new(addr, buf.clone());

        let mut rec_addr = log_hdr.log_start as usize;
        let mut rec_list = vec![];
        loop {
            if rec_addr == 0 || rec_addr > UNIV_PAGE_SIZE {
                break;
            }

            let rec = UndoRecord::new(rec_addr, buf.clone(), page_hdr, None);
            rec_addr = rec.undo_rec_hdr.next_addr();
            let type_info = rec.undo_rec_hdr.type_info.clone();
            rec_list.push(rec);

            if matches!(type_info, UndoTypes::ZERO_VAL) {
                break;
            }
        }

        Self {
            undo_log_hdr: log_hdr,
            undo_rec_list: rec_list,
            buf: buf.clone(),
            addr,
        }
    }
}

/// undo log header, see trx0undo.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoLogHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (8 bytes) TRX_UNDO_TRX_ID, Transaction id
    pub trx_id: u64,

    /// (8 bytes) TRX_UNDO_TRX_NO, Transaction number of the transaction;
    /// defined only if the log is in a history list
    pub trx_no: u64,

    /// (2 bytes) TRX_UNDO_DEL_MARKS, Defined only in an update undo log: true
    /// if the transaction may have done delete markings of records, and thus
    /// purge is necessary
    pub del_marks: u16,

    /// (2 bytes) TRX_UNDO_LOG_START, Offset of the first undo log record of
    /// this log on the header page; purge may remove undo log record from the
    /// log start, and therefore this is not necessarily the same as this log
    /// header end offset
    pub log_start: u16,

    /// see undo_flags_bits
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub undo_flags: Vec<UndoFlags>,

    /// (1 byte) TRX_UNDO_FLAGS, Transaction UNDO flags in one byte. This is
    /// backward compatible as earlier we were storing either 1 or 0 for
    /// TRX_UNDO_XID_EXISTS
    #[derivative(Debug = "ignore")]
    pub undo_flags_bits: u8,

    /// (1 byte), TRX_UNDO_DICT_TRANS, true if the transaction is a table
    /// create, index create, or drop transaction: in recovery the transaction
    /// cannot be rolled back in the usual way: a 'rollback' rather means
    /// dropping the created or dropped table, if it still exists
    pub dict_trans: u8,

    /// (8 bytes) TRX_UNDO_TABLE_ID, Id of the table if the preceding field is
    /// true. Note: deprecated
    pub table_id: u64,

    /// (2 bytes) TRX_UNDO_NEXT_LOG, Offset of the next undo log header on this
    /// page, 0 if none
    pub next_log: u16,

    /// (2 bytes) TRX_UNDO_PREV_LOG, Offset of the previous undo log header on
    /// this page, 0 if none
    pub prev_log: u16,

    /// (12 bytes) TRX_UNDO_HISTORY_NODE, If the log is put to the history list,
    /// the file list node is here
    pub history_node: FlstNode,

    /// (140 bytes) XA
    pub xa_trx_info: Option<XaTrxInfo>,
}

impl UndoLogHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let b0 = util::u8_val(&buf, addr + 20);

        let mut flags = vec![];
        if (b0 & Self::TRX_UNDO_FLAG_XID) > 0 {
            flags.push(UndoFlags::XID);
        }
        if (b0 & Self::TRX_UNDO_FLAG_GTID) > 0 {
            flags.push(UndoFlags::GTID);
        }
        if (b0 & Self::TRX_UNDO_FLAG_XA_PREPARE_GTID) > 0 {
            flags.push(UndoFlags::XA_PREPARE_GTID);
        }

        let xid_flag = (b0 & Self::TRX_UNDO_FLAG_XID) > 0;
        let xa = if xid_flag {
            Some(XaTrxInfo::new(addr + 46, buf.clone()))
        } else {
            None
        };

        Self {
            trx_id: util::u64_val(&buf, addr),
            trx_no: util::u64_val(&buf, addr + 8),
            del_marks: util::u16_val(&buf, addr + 16),
            log_start: util::u16_val(&buf, addr + 18),
            undo_flags: flags,
            undo_flags_bits: b0,
            dict_trans: util::u8_val(&buf, addr + 21),
            table_id: util::u64_val(&buf, addr + 22),
            next_log: util::u16_val(&buf, addr + 30),
            prev_log: util::u16_val(&buf, addr + 32),
            history_node: FlstNode::new(addr + 34, buf.clone()),
            xa_trx_info: xa,
            buf: buf.clone(),
            addr,
        }
    }

    /// true if undo log header includes X/Open XA transaction identification XID
    const TRX_UNDO_FLAG_XID: u8 = 0x01;

    /// true if undo log header includes GTID information from replication
    const TRX_UNDO_FLAG_GTID: u8 = 0x02;

    /// true if undo log header includes GTID information for XA PREPARE
    const TRX_UNDO_FLAG_XA_PREPARE_GTID: u8 = 0x04;

    pub fn is_xid(&self) -> bool {
        (self.undo_flags_bits & Self::TRX_UNDO_FLAG_XID) > 0
    }

    pub fn is_gtid(&self) -> bool {
        (self.undo_flags_bits & Self::TRX_UNDO_FLAG_GTID) > 0
    }

    pub fn is_xa_prepare_gtid(&self) -> bool {
        (self.undo_flags_bits & Self::TRX_UNDO_FLAG_XA_PREPARE_GTID) > 0
    }
}

/// states of an undo log segment
#[repr(u8)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum UndoFlags {
    /// TRX_UNDO_FLAG_XID, undo log header includes X/Open XA transaction
    /// identification XID,
    XID,

    /// TRX_UNDO_FLAG_GTID, undo log header includes GTID information from
    /// replication
    GTID,

    /// TRX_UNDO_FLAG_XA_PREPARE_GTID, undo log header includes GTID information
    /// for XA PREPARE
    XA_PREPARE_GTID,

    #[default]
    UNDEF,
}

/// X/Open XA transaction identification, see trx0undo.h
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct XaTrxInfo {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) TRX_UNDO_XA_FORMAT, xid_t::formatID
    pub xa_format: u32,

    /// (4 bytes) TRX_UNDO_XA_TRID_LEN xid_t::gtrid_length
    pub xa_trid_len: u32,

    /// (4 bytes) TRX_UNDO_XA_BQUAL_LEN xid_t::bqual_length
    pub xa_bqual_len: u32,

    /// (128 bytes) XA Data, distributed trx identifier. not \0-terminated.
    pub xa_data: Bytes,
}

impl XaTrxInfo {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            xa_format: util::u32_val(&buf, addr),
            xa_trid_len: util::u32_val(&buf, addr + 4),
            xa_bqual_len: util::u32_val(&buf, addr + 8),
            xa_data: buf.slice(addr + 12..addr + 12 + XIDDATASIZE),
            buf: buf.clone(),
            addr,
        }
    }
}

/// parsed rollback pointer
#[derive(Clone, Derivative, Eq, PartialEq)]
#[derivative(Debug)]
pub struct RollPtr {
    #[derivative(Debug(format_with = "util::fmt_hex56"))]
    /// (7 bytes) original rollback pointer bytes value
    pub value: u64,

    /// (1 bit) insert flag
    #[derivative(Debug(format_with = "util::fmt_bool"))]
    pub insert: bool,

    /// (7 bits) rollback segment id
    #[derivative(Debug(format_with = "util::fmt_enum_2"))]
    pub rseg_id: SpaceId,

    /// (4 bytes) page number
    #[derivative(Debug(format_with = "util::fmt_enum_3"))]
    pub page_no: PageNumber,

    /// (2 bytes) page offset
    pub boffset: u16,
}

impl RollPtr {
    pub fn new(value: u64) -> Self {
        let seg_id = ((value >> 48) & 0x7f) as u32;
        Self {
            value,
            insert: ((value >> 55) & 0x1) > 0,
            rseg_id: SpaceId::UndoSpace(seg_id),
            page_no: (((value >> 16) & 0xffffffff) as u32).into(),
            boffset: (value & 0xffff) as u16,
        }
    }
}

/// undo record
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecord {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// undo record header
    pub undo_rec_hdr: UndoRecordHeader,

    /// undo record payload
    pub undo_rec_data: UndoRecordPayloads,
}

impl UndoRecord {
    pub fn new(
        addr: usize,
        buf: Arc<Bytes>,
        page_hdr: &UndoPageHeader,
        n_uniq: Option<usize>,
    ) -> Self {
        let hdr = UndoRecordHeader::new(addr, buf.clone());
        info!("hdr = {:?}", &hdr);

        let payload = match hdr.type_info {
            UndoTypes::ZERO_VAL => UndoRecordPayloads::Nothing,
            _ => match page_hdr.page_type {
                UndoPageTypes::TRX_UNDO_INSERT => {
                    UndoRecordPayloads::Insert(UndoRecForInsert::new(addr + 3, buf.clone(), n_uniq))
                }
                UndoPageTypes::TRX_UNDO_UPDATE => {
                    UndoRecordPayloads::Update(UndoRecForUpdate::new(addr + 3, buf.clone(), n_uniq))
                }
                UndoPageTypes::UNDEF => UndoRecordPayloads::Nothing,
            },
        };

        Self {
            undo_rec_hdr: hdr,
            undo_rec_data: payload,
            buf: buf.clone(),
            addr,
        }
    }

    pub fn read(addr: usize, buf: Arc<Bytes>, boffset: usize, n_uniq: usize) -> Self {
        let page_hdr = UndoPageHeader::new(addr + FIL_HEADER_SIZE, buf.clone());
        Self::new(addr + boffset, buf.clone(), &page_hdr, Some(n_uniq))
    }
}

/// undo record header
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecordHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (2 bytes) previous record offset
    pub prev_rec_offset: u16,

    /// (2 bytes) next record offset
    pub next_rec_offset: u16,

    /// (1 byte) type, extern flag, compilation info
    #[derivative(Debug = "ignore")]
    pub type_cmpl: u8,

    /// type info, see info_bytes
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub type_info: UndoTypes,

    /// compilation info, see info_bytes
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub cmpl_info: Vec<CmplInfos>,

    /// update external flags
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub extra_flags: Vec<UndoExtraFlags>,
}

impl UndoRecordHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let type_cmpl = util::u8_val(&buf, addr + 2);

        let cmpl_info_bits = (type_cmpl >> 4) & 0x03;
        let mut cmpl_info = vec![];
        if (cmpl_info_bits & Self::UPD_NODE_NO_ORD_CHANGE) > 0 {
            cmpl_info.push(CmplInfos::NO_ORD_CHANGE);
        }
        if (cmpl_info_bits & Self::UPD_NODE_NO_SIZE_CHANGE) > 0 {
            cmpl_info.push(CmplInfos::NO_SIZE_CHANGE);
        }

        let mut extra_flags = vec![];
        if (type_cmpl & Self::TRX_UNDO_CMPL_INFO_MULT) > 0 {
            extra_flags.push(UndoExtraFlags::CMPL_INFO_MULT);
        }
        if (type_cmpl & Self::TRX_UNDO_MODIFY_BLOB) > 0 {
            extra_flags.push(UndoExtraFlags::MODIFY_BLOB);
        }
        if (type_cmpl & Self::TRX_UNDO_UPD_EXTERN) > 0 {
            extra_flags.push(UndoExtraFlags::UPD_EXTERN);
        }

        let type_info: UndoTypes = (type_cmpl & 0x0f).into();

        Self {
            prev_rec_offset: util::u16_val(&buf, addr - 2),
            next_rec_offset: util::u16_val(&buf, addr),
            type_cmpl,
            type_info,
            cmpl_info,
            extra_flags,
            buf: buf.clone(),
            addr,
        }
    }

    pub fn prev_addr(&self) -> usize {
        self.prev_rec_offset as usize
    }

    pub fn next_addr(&self) -> usize {
        self.next_rec_offset as usize
    }

    // Compilation info flags: these must fit within 2 bits; see trx0rec.h

    /// no secondary index record will be changed in the update and no ordering
    /// field of the clustered index
    const UPD_NODE_NO_ORD_CHANGE: u8 = 1;

    /// no record field size will be changed in the update
    const UPD_NODE_NO_SIZE_CHANGE: u8 = 2;

    /// compilation info is multiplied by this and ORed to the type above
    const TRX_UNDO_CMPL_INFO_MULT: u8 = 16;

    /// If this bit is set in type_cmpl, then the undo log record has support
    /// for partial update of BLOBs. Also to make the undo log format
    /// extensible, introducing a new flag next to the type_cmpl flag.
    const TRX_UNDO_MODIFY_BLOB: u8 = 64;

    /// This bit can be ORed to type_cmpl to denote that we updated external
    /// storage fields: used by purge to free the external storage
    const TRX_UNDO_UPD_EXTERN: u8 = 128;
}

/// states of an undo log segment
#[repr(u8)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum UndoTypes {
    /// the default value: 0
    #[default]
    ZERO_VAL = 0,

    /// Operation type flags used in trx_undo_report_row_operation
    INSERT_OP = 1,

    /// Operation type flags used in trx_undo_report_row_operation
    MODIFY_OP = 2,

    /// fresh insert into clustered index
    INSERT_REC = 11,

    /// update of a non-delete-marked record
    UPD_EXIST_REC = 12,

    /// update of a delete marked record to a not delete marked record; also the
    /// fields of the record can change
    UPD_DEL_REC = 13,

    /// delete marking of a record; fields do not change
    DEL_MARK_REC = 14,
}

/// States of an undo log segment
#[repr(u8)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum CmplInfos {
    #[default]
    UNDEF = 0,

    /// Compilation info flags: these must fit within 2 bits; see trx0rec.h no
    /// secondary index record will be changed in the update and no ordering
    /// field of the clustered index
    NO_ORD_CHANGE = 1,

    /// no record field size will be changed in the update
    NO_SIZE_CHANGE = 2,
}

/// extra flags: modify BLOB, update external, ...
#[repr(u8)]
#[derive(Debug, Display, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString)]
pub enum UndoExtraFlags {
    /// compilation info is multiplied by this and ORed to the type above
    CMPL_INFO_MULT,

    /// If this bit is set in type_cmpl, then the undo log record has support
    /// for partial update of BLOBs. Also to make the undo log format
    /// extensible, introducing a new flag next to the type_cmpl flag.
    MODIFY_BLOB,

    /// This bit can be ORed to type_cmpl to denote that we updated external
    /// storage fields: used by purge to free the external storage
    UPD_EXTERN,
}

/// undo record payload
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum UndoRecordPayloads {
    Insert(UndoRecForInsert),
    Update(UndoRecForUpdate),
    Nothing,
}

/// see trx_undo_page_report_insert(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecForInsert {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (1..11 bytes) undo number, in much compressed form
    pub undo_no: u64,

    /// (1..11 bytes) table id, in much compressed form
    pub table_id: u64,

    /// key fields
    pub key_fields: Vec<UndoRecKeyField>,
}

impl UndoRecForInsert {
    pub fn new(addr: usize, buf: Arc<Bytes>, n_uniq: Option<usize>) -> Self {
        let mut ptr = addr;

        let undo_no = util::u64_much_compressed(ptr, buf.clone());
        ptr += undo_no.0;

        let table_id = util::u64_much_compressed(ptr, buf.clone());
        ptr += table_id.0;

        // key fields
        let mut key_fields = vec![];
        let n_unique_key = n_uniq.unwrap_or(dict::get_n_unique_key(table_id.1));
        for i in 0..n_unique_key {
            let key = UndoRecKeyField::new(ptr, buf.clone(), i);
            ptr += key.total_bytes;
            key_fields.push(key);
        }

        Self {
            undo_no: undo_no.1,
            table_id: table_id.1,
            key_fields,
            buf: buf.clone(),
            addr,
        }
    }
}

/// see trx_undo_page_report_modify(...)
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecForUpdate {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// introducing a change in undo log format
    pub new1byte: u8,

    /// (1..11 bytes) undo number, in much compressed form
    pub undo_no: u64,

    /// (1..11 bytes) table id, in much compressed form
    pub table_id: u64,

    /// (1 byte) info bits
    pub info_bits: u8,

    /// (1..11 bytes) transaction id, in compressed form
    pub trx_id: u64,

    /// (1..11 bytes) rollback pointer, in compressed form
    pub roll_ptr: RollPtr,

    /// key fields
    pub key_fields: Vec<UndoRecKeyField>,

    /// (1-5 bytes) updated field count
    pub n_fields: u32,

    /// updated fields
    pub upd_fields: Vec<UndoRecUpdatedField>,
}

impl UndoRecForUpdate {
    pub fn new(addr: usize, buf: Arc<Bytes>, n_uniq: Option<usize>) -> Self {
        let mut ptr = addr;

        // info!("peek={:?}", buf.slice(ptr..ptr + 20).to_vec());
        let new1byte = util::u8_val(&buf, ptr);
        ptr += 1;

        let undo_no = util::u64_much_compressed(ptr, buf.clone());
        ptr += undo_no.0;

        let table_id = util::u64_much_compressed(ptr, buf.clone());
        ptr += table_id.0;

        let info_bits = util::u8_val(&buf, ptr);
        ptr += 1;

        let trx_id = util::u64_compressed(ptr, buf.clone());
        ptr += trx_id.0;

        let roll_ptr = util::u64_compressed(ptr, buf.clone());
        ptr += roll_ptr.0;

        // key fields
        let mut key_fields = vec![];
        let n_unique_key = n_uniq.unwrap_or(dict::get_n_unique_key(table_id.1));
        for i in 0..n_unique_key {
            let key = UndoRecKeyField::new(ptr, buf.clone(), i);
            ptr += key.total_bytes;
            key_fields.push(key);
        }

        let n_updated = util::u32_compressed(ptr, buf.clone());
        ptr += n_updated.0;

        // updated fields
        let mut upd_fields = vec![];
        for i in 0..(n_updated.1 as usize) {
            let fld = UndoRecUpdatedField::new(ptr, buf.clone(), i);
            ptr += fld.total_bytes;
            upd_fields.push(fld);
        }

        Self {
            new1byte,
            undo_no: undo_no.1,
            table_id: table_id.1,
            info_bits,
            trx_id: trx_id.1,
            roll_ptr: RollPtr::new(roll_ptr.1),
            key_fields,
            n_fields: n_updated.1,
            upd_fields,
            buf: buf.clone(),
            addr,
        }
    }
}

/// undo record key fields
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecKeyField {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// sequence number
    pub sequence: usize,

    /// (1-5 bytes) key length
    pub key_len: usize,

    /// (key_len bytes) key data, see length for total size
    pub key_data: Bytes,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl UndoRecKeyField {
    pub fn new(addr: usize, buf: Arc<Bytes>, seq: usize) -> Self {
        let mut ptr = addr;

        let length = util::u32_compressed(ptr, buf.clone());
        ptr += length.0;

        let data = buf.slice(ptr..ptr + (length.1 as usize));
        ptr += length.1 as usize;

        Self {
            sequence: seq,
            key_len: length.1 as usize,
            key_data: data,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

/// undo record updated fields
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecUpdatedField {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// sequence number
    pub sequence: usize,

    /// (1-5 bytes) field number
    pub field_no: u32,

    /// (1-5 bytes) field length
    pub field_len: usize,

    /// (field_len bytes) field data, see length for total size
    pub field_data: Bytes,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl UndoRecUpdatedField {
    pub fn new(addr: usize, buf: Arc<Bytes>, seq: usize) -> Self {
        let mut ptr = addr;

        let field_no = util::u32_compressed(ptr, buf.clone());
        ptr += field_no.0;

        let length = util::u32_compressed(ptr, buf.clone());
        ptr += length.0;

        let data = buf.slice(ptr..ptr + (length.1 as usize));
        ptr += length.1 as usize;

        Self {
            sequence: seq,
            field_no: field_no.1,
            field_len: length.1 as usize,
            field_data: data,
            total_bytes: ptr - addr,
            buf: buf.clone(),
            addr,
        }
    }
}

#[cfg(test)]
mod undo_tests {

    use std::path::PathBuf;

    use anyhow::Result;

    use super::*;
    use crate::{factory::DatafileFactory, util};

    const REDO_1: &str = "data/redo_block_01";
    const UNDO_1: &str = "data/undo_log_01";

    #[test]
    fn test_read_undo_record() -> Result<()> {
        util::init_unit_test();

        let mut fact = DatafileFactory::from_file(PathBuf::from(UNDO_1))?;
        let page = 188;
        let buf = fact.page_buffer(page)?;

        let boffset = 418;
        let ans = UndoRecord::read(0, buf, boffset, 1);
        dbg!(&ans);

        Ok(())
    }
}
