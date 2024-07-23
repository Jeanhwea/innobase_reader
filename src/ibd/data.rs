use bytes::Bytes;

use enum_display::EnumDisplay;

pub const PAGE_ADDR_INF: usize = 99;
pub const PAGE_ADDR_SUP: usize = 112;

#[repr(u8)]
#[allow(non_camel_case_types)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, EnumDisplay, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecordStatus {
    REC_STATUS_ORDINARY = 0,
    REC_STATUS_NODE_PTR = 1,
    REC_STATUS_INFIMUM = 2,
    REC_STATUS_SUPREMUM = 3,
    MARKED(u8),
}

impl From<u8> for RecordStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => RecordStatus::REC_STATUS_ORDINARY,
            1 => RecordStatus::REC_STATUS_NODE_PTR,
            2 => RecordStatus::REC_STATUS_INFIMUM,
            3 => RecordStatus::REC_STATUS_SUPREMUM,
            _ => RecordStatus::MARKED(value),
        }
    }
}

#[derive(Debug)]
pub struct RecordHeader {
    pub info_bits: u8,            // 4 bits, MIN_REC/DELETED/VERSION/INSTANT, see rec.h
    pub n_owned: u8,              // 4 bits
    pub heap_no: u16,             // 13 bits
    pub rec_status: RecordStatus, // 3 bits, see rec.h
    pub next_rec_offset: u16,     // next record offset
}

impl RecordHeader {
    pub fn new(buffer: Bytes) -> Self {
        let b1 = u16::from_be_bytes(buffer.as_ref()[1..3].try_into().unwrap());
        let status = (b1 & 0x0007) as u8;
        Self {
            info_bits: (buffer[0] & 0xf0) >> 4,
            n_owned: (buffer[0] & 0x0f),
            heap_no: (b1 & 0xfff8) >> 3,
            rec_status: status.into(),
            next_rec_offset: u16::from_be_bytes(buffer.as_ref()[3..5].try_into().unwrap()),
        }
    }

    // Info bit denoting the predefined minimum record: this bit is set if and
    // only if the record is the first user record on a non-leaf B-tree page
    // that is the leftmost page on its level (PAGE_LEVEL is nonzero and
    // FIL_PAGE_PREV is FIL_NULL).
    const REC_INFO_MIN_REC_FLAG: u8 = 1;
    // The deleted flag in info bits; when bit is set to 1, it means the record
    // has been delete marked
    const REC_INFO_DELETED_FLAG: u8 = 2;
    // Use this bit to indicate record has version
    const REC_INFO_VERSION_FLAG: u8 = 4;
    // The instant ADD COLUMN flag. When it is set to 1, it means this record
    // was inserted/updated after an instant ADD COLUMN.
    const REC_INFO_INSTANT_FLAG: u8 = 8;

    pub fn is_min_rec(&self) -> bool {
        (self.info_bits & Self::REC_INFO_MIN_REC_FLAG) > 0
    }

    pub fn is_deleted(&self) -> bool {
        (self.info_bits & Self::REC_INFO_DELETED_FLAG) > 0
    }
}

#[derive(Debug)]
pub struct RecordInfo {
    // nullable list
    // vary field list
}

#[derive(Debug)]
pub struct Row {
    pub row_id: u64,   // 6 bytes
    pub trx_id: u64,   // 6 bytes
    pub roll_ptr: u64, // 7 bytes
}

#[derive(Debug)]
pub struct Record {
    pub rec_pre: Option<RecordInfo>, // record prefix information
    pub rec_hdr: RecordHeader,       // record header
    pub row: Option<Row>,            // row data
}

impl Record {
    pub fn new(hdr: RecordHeader) -> Self {
        Self {
            rec_hdr: hdr,
            rec_pre: None,
            row: None,
        }
    }
}
