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