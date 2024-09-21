use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;
use num_enum::FromPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::{Display, EnumString};

use super::page::FlstNode;
use crate::util;

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
    pub cmpl_info: Vec<CmplInfos>,

    /// update external flags
    pub extra_flags: Vec<UndoExtraFlags>,
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

        Self {
            prev_rec_offset: util::u16_val(&buf, addr - 2),
            next_rec_offset: util::u16_val(&buf, addr),
            type_info: (b1 & 0x0f).into(),
            cmpl_info,
            extra_flags,
            info_bits: b1,
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
    #[default]
    ZERO = 0,

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
