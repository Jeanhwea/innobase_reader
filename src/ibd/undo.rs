use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use super::page::FlstNode;
use crate::util::{self, mach_u32_read_much_compressed, mach_u64_read_much_compressed};

/// States of an undo log segment
#[repr(u8)]
#[derive(Debug, Display, Default, Eq, PartialEq, Ord, PartialOrd, Clone)]
#[derive(Deserialize_repr, Serialize_repr, EnumString, FromPrimitive)]
pub enum UndoFlags {
    ///  undo log header includes X/Open XA transaction identification XID
    XID,

    /// undo log header includes GTID information from replication
    GTID,

    ///  undo log header includes GTID information for XA PREPARE
    XA_PREPARE_GTID,

    #[default]
    UNDEF,
}

/// Undo Log Header, see trx0undo.h
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

        let mut flags = Vec::new();
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

/// X/Open XA Transaction Identification, see trx0undo.h
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

    /// (128 bytes) XA Data
    pub xa_data: Bytes,
}

impl XaTrxInfo {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            xa_format: util::u32_val(&buf, addr),
            xa_trid_len: util::u32_val(&buf, addr + 4),
            xa_bqual_len: util::u32_val(&buf, addr + 8),
            xa_data: buf.slice(addr + 12..addr + 12 + 128),
            buf: buf.clone(),
            addr,
        }
    }
}

/// Undo Record
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

    /// undo record data
    pub undo_rec_data: Option<UndoRecordData>,
}

impl UndoRecord {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let hdr = UndoRecordHeader::new(addr, buf.clone());

        let data = if !matches!(hdr.type_info, UndoTypes::ZERO_VAL) {
            Some(UndoRecordData::new(addr + hdr.total_bytes, buf.clone()))
        } else {
            None
        };

        Self {
            undo_rec_hdr: hdr,
            undo_rec_data: data,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Undo Record Header
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
    pub info_bits: u8,

    /// type info, see info_bytes
    #[derivative(Debug(format_with = "util::fmt_enum"))]
    pub type_info: UndoTypes,

    /// compilation info, see info_bytes
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub cmpl_info: Vec<CmplInfos>,

    /// update external flags
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub extra_flags: Vec<UndoExtraFlags>,

    /// (1..11 bytes) compressed form, see mach_u64_write_much_compressed(...), Undo number
    pub undo_no: u64,

    /// (1..11 bytes) compressed form, see mach_u64_write_much_compressed(...), Table ID
    pub table_id: u64,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl UndoRecordHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        let b1 = util::u8_val(&buf, addr + 2);

        let cmpl_info_bits = (b1 >> 4) & 0x03;
        let mut cmpl_info = Vec::new();
        if (cmpl_info_bits & Self::UPD_NODE_NO_ORD_CHANGE) > 0 {
            cmpl_info.push(CmplInfos::NO_ORD_CHANGE);
        }
        if (cmpl_info_bits & Self::UPD_NODE_NO_SIZE_CHANGE) > 0 {
            cmpl_info.push(CmplInfos::NO_SIZE_CHANGE);
        }

        let mut extra_flags = Vec::new();
        if (b1 & Self::TRX_UNDO_CMPL_INFO_MULT) > 0 {
            extra_flags.push(UndoExtraFlags::CMPL_INFO_MULT);
        }
        if (b1 & Self::TRX_UNDO_MODIFY_BLOB) > 0 {
            extra_flags.push(UndoExtraFlags::MODIFY_BLOB);
        }
        if (b1 & Self::TRX_UNDO_UPD_EXTERN) > 0 {
            extra_flags.push(UndoExtraFlags::UPD_EXTERN);
        }

        let type_info: UndoTypes = (b1 & 0x0f).into();

        let mut total = 3usize;

        let mut undo_no = 0u64;
        let mut table_id = 0u64;
        if !matches!(type_info, UndoTypes::ZERO_VAL) {
            // trx_undo_page_report_modify(...) for UndoRecord buffer structure
            let pack01 = mach_u64_read_much_compressed(addr + 3, buf.clone());
            total += pack01.0;
            undo_no = pack01.1;

            let pack02 = mach_u64_read_much_compressed(addr + 3 + pack01.0, buf.clone());
            total += pack02.0;
            table_id = pack02.1;
        }

        Self {
            prev_rec_offset: util::u16_val(&buf, addr - 2),
            next_rec_offset: util::u16_val(&buf, addr),
            type_info,
            cmpl_info,
            extra_flags,
            undo_no,
            table_id,
            info_bits: b1,
            total_bytes: total,
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

    /// Compilation info flags: these must fit within 2 bits; see trx0rec.h no
    /// secondary index record will be changed in the update and no ordering
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

/// States of an undo log segment
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

/// Extra flags: modify BLOB, Update external, ...
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

/// Undo Record Data
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecordData {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// key fields
    pub key_fields: Vec<UndoRecKeyField>,

    /// (1-5 bytes) updated field count
    pub upd_count: u32,

    /// updated fields
    pub upd_fields: Vec<UndoRecUpdatedField>,
}

impl UndoRecordData {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        // key fields
        let mut key_fields = Vec::new();
        let mut ptr = addr;
        for i in 0..1 {
            let key = UndoRecKeyField::new(ptr, buf.clone(), i);
            ptr += key.total_bytes;
            key_fields.push(key);
        }

        // update count
        let upd = mach_u32_read_much_compressed(addr, buf.clone());
        ptr += upd.0;

        // updated fields
        let mut upd_fields = Vec::new();
        for i in 0..(upd.1 as usize) {
            let fld = UndoRecUpdatedField::new(ptr, buf.clone(), i);
            ptr += fld.total_bytes;
            upd_fields.push(fld);
        }

        Self {
            key_fields,
            upd_count: upd.1,
            upd_fields,
            buf: buf.clone(),
            addr,
        }
    }
}

/// Undo Record Key Fields
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecKeyField {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// sequence
    pub seq: usize,

    /// (1-5 bytes) key length
    pub length: u32,

    /// (??? bytes) key content, see length for total size
    pub content: Bytes,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl UndoRecKeyField {
    pub fn new(addr: usize, buf: Arc<Bytes>, seq: usize) -> Self {
        let len = mach_u32_read_much_compressed(addr, buf.clone());
        let beg = addr + len.0;
        let end = beg + (len.1 as usize);
        Self {
            seq,
            length: len.1,
            content: buf.clone().slice(beg..end),
            total_bytes: len.0 + (len.1 as usize),
            buf: buf.clone(),
            addr,
        }
    }
}

/// Undo Record Updated Fields
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct UndoRecUpdatedField {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// sequence
    pub seq: usize,

    /// (1-5 bytes) field number
    pub field_num: u32,

    /// (1-5 bytes) key length
    pub length: u32,

    /// (??? bytes) key content, see length for total size
    pub content: Bytes,

    /// total bytes
    #[derivative(Debug = "ignore")]
    pub total_bytes: usize,
}

impl UndoRecUpdatedField {
    pub fn new(addr: usize, buf: Arc<Bytes>, seq: usize) -> Self {
        let fno = mach_u32_read_much_compressed(addr, buf.clone());
        let len = mach_u32_read_much_compressed(addr + fno.0, buf.clone());
        let beg = addr + fno.0 + len.0;
        let end = beg + (len.1 as usize);
        Self {
            seq,
            field_num: fno.1,
            length: len.1,
            content: buf.clone().slice(beg..end),
            total_bytes: fno.0 + len.0 + (len.1 as usize),
            buf: buf.clone(),
            addr,
        }
    }
}
