use bytes::Bytes;

use enum_display::EnumDisplay;

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
pub struct Record {
    rec_hdr: RecordHeader, // record header
    row_data: Bytes,       // row data
}

impl Record {
    pub fn new(buffer: Bytes) -> Self {
        Self {
            rec_hdr: RecordHeader::new(buffer.slice(..5)),
            row_data: buffer.slice(5..),
        }
    }
}

#[derive(Debug)]
pub struct RecordHeader {
    info_bits: u8,            // 4 bits, MIN_REC/DELETED/VERSION/INSTANT, see rec.h
    n_owned: u8,              // 4 bits
    heap_no: u16,             // 13 bits
    rec_status: RecordStatus, // 3 bits, see rec.h
    next_rec_offset: u16,     // next record offset
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
