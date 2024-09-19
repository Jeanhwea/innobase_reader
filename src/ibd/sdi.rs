use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;

use crate::{ibd::record::RecordHeader, util};

// sdi
pub const SDI_DATA_HEADER_SIZE: usize = 33;

/// SDI Record
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiRecord {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// record header
    #[derivative(Debug(format_with = "util::fmt_oneline"))]
    pub rec_hdr: RecordHeader,

    /// SDI Data Header
    pub sdi_hdr: SdiDataHeader,

    /// SDI Data String, uncompressed string
    pub sdi_str: String,
}

impl SdiRecord {
    pub fn new(addr: usize, buf: Arc<Bytes>, rec_hdr: RecordHeader, hdr: SdiDataHeader) -> Self {
        let beg = addr + SDI_DATA_HEADER_SIZE;
        let comped_data = buf.slice(beg..beg + (hdr.comp_len as usize));
        let uncomped_data = util::zlib_uncomp(comped_data).unwrap();
        assert_eq!(uncomped_data.len(), hdr.uncomp_len as usize);
        Self {
            rec_hdr,
            sdi_hdr: hdr,
            sdi_str: uncomped_data,
            buf: buf.clone(),
            addr,
        }
    }
}

/// SDI Data Header
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct SdiDataHeader {
    /// page address
    #[derivative(Debug(format_with = "util::fmt_addr"))]
    pub addr: usize,

    /// page data buffer
    #[derivative(Debug = "ignore")]
    pub buf: Arc<Bytes>,

    /// (4 bytes) Length of TYPE field in record of SDI Index
    pub data_type: u32,
    /// (8 bytes) Data ID
    pub data_id: u64,
    /// (6 bytes) Transaction ID
    pub trx_id: u64,
    /// (7 bytes) Rollback pointer
    pub roll_ptr: u64,
    /// (4 bytes) Length of UNCOMPRESSED_LEN field in record of SDI Index
    pub uncomp_len: u32,
    /// (4 bytes) Length of COMPRESSED_LEN field in record of SDI Index
    pub comp_len: u32,
}

impl SdiDataHeader {
    pub fn new(addr: usize, buf: Arc<Bytes>) -> Self {
        Self {
            data_type: util::u32_val(&buf, addr),
            data_id: util::u64_val(&buf, addr + 4),
            trx_id: util::u48_val(&buf, addr + 12),
            roll_ptr: util::u56_val(&buf, addr + 18),
            uncomp_len: util::u32_val(&buf, addr + 25),
            comp_len: util::u32_val(&buf, addr + 29),
            buf: buf.clone(),
            addr,
        }
    }
}
