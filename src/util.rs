use anyhow::Result;
use bytes::Bytes;
use chrono::Local;
use flate2::read::ZlibDecoder;
use std::fmt::{Display, LowerHex};
use std::io::{Read, Write};
use std::sync::Once;

static INIT_LOGGER_ONCE: Once = Once::new();

pub fn init() {
    INIT_LOGGER_ONCE.call_once(|| {
        dotenv::dotenv().ok();

        env_logger::builder()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "[{} {:<5} {}] {}",
                    Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.module_path().unwrap(),
                    record.args()
                )
            })
            .init();
    })
}

pub fn fmt_hex32<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:08x}({})", d, d)
}

pub fn fmt_hex64<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:016x}({})", d, d)
}

pub fn fmt_addr<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:x} @{}", d, d)
}

pub fn zlib_uncomp(input: Bytes) -> Result<String> {
    let input_buffer = input.to_vec();
    let mut decoder = ZlibDecoder::new(&*input_buffer);
    let mut output = String::new();
    decoder.read_to_string(&mut output)?;
    Ok(output)
}

pub fn align8(num: usize) -> usize {
    (num >> 3) + if (num & 0x7) > 0 { 1 } else { 0 }
}

pub fn numoff(num: usize) -> usize {
    num & 0x7
}

pub fn numidx(num: usize) -> usize {
    (num & (!0x7)) >> 3
}

pub fn u16_val(buf: &[u8], addr: usize) -> u16 {
    u16::from_be_bytes(buf[addr..addr + 2].try_into().expect("ERR_READ_VALUE_u16"))
}

pub fn u32_val(buf: &[u8], addr: usize) -> u32 {
    u32::from_be_bytes(buf[addr..addr + 4].try_into().expect("ERR_READ_VALUE_u32"))
}

pub fn u64_val(buf: &[u8], addr: usize) -> u64 {
    u64::from_be_bytes(buf[addr..addr + 8].try_into().expect("ERR_READ_VALUE_u64"))
}

pub fn from_bytes6(b: Bytes) -> u64 {
    assert_eq!(b.len(), 6);
    let arr = [b[0], b[1], b[2], b[3], b[4], b[5], 0u8, 0u8];
    u64::from_be_bytes(arr)
}

pub fn from_bytes7(b: Bytes) -> u64 {
    assert_eq!(b.len(), 7);
    let arr = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], 0u8];
    u64::from_be_bytes(arr)
}

#[cfg(test)]
mod util_tests {

    use std::env::set_var;

    use log::info;

    use super::*;

    fn setup() {
        set_var("RUST_LOG", "info");
        init();
    }

    #[test]
    fn test_conv_u32() {
        setup();
        let buf = Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]);
        info!("buf={:?}", buf);
        assert_eq!(u32_val(&buf[0..7], 2), 0x03040506);
    }

    #[test]
    fn test_align_count() {
        setup();
        assert_eq!(align8(0), 0);
        assert_eq!(align8(1), 1);
        assert_eq!(align8(8), 1);
        assert_eq!(align8(9), 2);
        assert_eq!(align8(254), 32);
        assert_eq!(align8(255), 32);
    }

    #[test]
    fn test_calc_number_offset() {
        setup();
        assert_eq!(numoff(0), 0);
        assert_eq!(numoff(1), 1);
        assert_eq!(numoff(7), 7);
        assert_eq!(numoff(8), 0);
    }

    #[test]
    fn test_calc_number_index() {
        setup();
        assert_eq!(numidx(0), 0);
        assert_eq!(numidx(1), 0);
        assert_eq!(numidx(7), 0);
        assert_eq!(numidx(8), 1);
        assert_eq!(numidx(15), 1);
        assert_eq!(numidx(16), 2);
    }
}
