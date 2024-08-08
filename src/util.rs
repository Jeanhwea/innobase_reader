use std::{
    collections::HashMap,
    env::set_var,
    fmt::{Binary, Debug, Display, LowerHex},
    io::{Read, Write},
    sync::Once,
};

use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime};
use colored::Colorize;
use flate2::read::ZlibDecoder;
use log::{debug, trace};

static INIT_LOGGER_ONCE: Once = Once::new();

pub fn init() {
    INIT_LOGGER_ONCE.call_once(|| {
        dotenv::dotenv().ok();

        env_logger::builder()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "[{} {:<5} {}:{}] {}",
                    Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.file().unwrap(),
                    record.line().unwrap(),
                    record.args()
                )
            })
            .init();
    })
}

pub fn init_unit_test() {
    INIT_LOGGER_ONCE.call_once(|| {
        set_var("RUST_LOG", "info");
        env_logger::builder().format_timestamp(None).init();
    });
}

pub fn fmt_bin32<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + Binary + LowerHex,
{
    write!(f, "0b{:032b}(0x{:08x})", d, d)
}

pub fn fmt_hex32<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:08x}({})", d, d.to_string().blue())
}

pub fn fmt_hex64<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:016x}({})", d, d.to_string().blue())
}

pub fn fmt_hex48<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:012x}", d)
}

pub fn fmt_hex56<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:014x}", d)
}

pub fn fmt_addr<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + LowerHex,
{
    write!(f, "0x{:04x}@({})", d, d.to_string().yellow())
}

pub fn fmt_page_no(d: &u32, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    if *d == 0xffffffff {
        write!(f, "NONE")
    } else {
        write!(f, "{}", d)
    }
}

pub fn fmt_enum<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display,
{
    write!(f, "{}", d.to_string().magenta())
}

pub fn fmt_oneline<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Debug,
{
    write!(f, "{:?}", d)
}

pub fn fmt_oneline_vec<T>(d: &Vec<T>, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Debug,
{
    if d.is_empty() {
        write!(f, "[]")
    } else {
        let _ = writeln!(f, "[");
        for e in d {
            let _ = writeln!(f, "    {:?},", e);
        }
        write!(f, "]")
    }
}

pub fn fmt_bytes_bin(d: &Bytes, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    let vs = d.to_vec();
    if vs.is_empty() {
        write!(f, "[]")
    } else {
        let _ = write!(f, "[");
        let n = d.len();
        for (i, e) in d.iter().enumerate() {
            let _ = write!(f, "0b{:08b}", e);
            if i < n - 1 {
                let _ = write!(f, ", ");
            }
        }
        write!(f, "]")
    }
}

pub fn fmt_bytes_hex(d: &Bytes, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    let vs = d.to_vec();
    if vs.is_empty() {
        write!(f, "[]")
    } else {
        let _ = write!(f, "[");
        let n = d.len();
        for (i, e) in d.iter().enumerate() {
            let _ = write!(f, "0x{:02x}", e);
            if i < n - 1 {
                let _ = write!(f, ", ");
            }
        }
        write!(f, "]")
    }
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

pub fn numpos(num: usize) -> usize {
    (num & (!0x7)) >> 3
}

pub fn i16_val(buf: &[u8], addr: usize) -> i16 {
    i16::from_be_bytes(buf[addr..addr + 2].try_into().expect("ERR_READ_VALUE_i16"))
}

pub fn i32_val(buf: &[u8], addr: usize) -> i32 {
    i32::from_be_bytes(buf[addr..addr + 4].try_into().expect("ERR_READ_VALUE_i32"))
}

pub fn u16_val(buf: &[u8], addr: usize) -> u16 {
    u16::from_be_bytes(buf[addr..addr + 2].try_into().expect("ERR_READ_VALUE_u16"))
}

pub fn u32_val(buf: &[u8], addr: usize) -> u32 {
    u32::from_be_bytes(buf[addr..addr + 4].try_into().expect("ERR_READ_VALUE_u32"))
}

pub fn u48_val(buf: &[u8], addr: usize) -> u64 {
    let arr = [
        buf[addr],
        buf[addr + 1],
        buf[addr + 2],
        buf[addr + 3],
        buf[addr + 4],
        buf[addr + 5],
        0u8,
        0u8,
    ];
    u64::from_be_bytes(arr)
}

pub fn u56_val(buf: &[u8], addr: usize) -> u64 {
    let arr = [
        buf[addr],
        buf[addr + 1],
        buf[addr + 2],
        buf[addr + 3],
        buf[addr + 4],
        buf[addr + 5],
        buf[addr + 6],
        0u8,
    ];
    u64::from_be_bytes(arr)
}

pub fn u64_val(buf: &[u8], addr: usize) -> u64 {
    u64::from_be_bytes(buf[addr..addr + 8].try_into().expect("ERR_READ_VALUE_u64"))
}

// https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
pub fn unpack_i32_val(buf: &[u8]) -> i32 {
    let signed = (buf[0] & 0x80) > 0;
    if signed {
        let b = [buf[0] & 0x7f, buf[1], buf[2], buf[3]];
        i32::from_be_bytes(b)
    } else {
        let b = [buf[0] | 0x80, buf[1], buf[2], buf[3]];
        i32::from_be_bytes(b)
    }
}

pub fn unpack_i64_val(buf: &[u8]) -> i64 {
    let signed = (buf[0] & 0x80) > 0;
    if signed {
        let b = [buf[0] & 0x7f, buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]];
        i64::from_be_bytes(b)
    } else {
        let b = [buf[0] | 0x80, buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]];
        i64::from_be_bytes(b)
    }
}

pub fn unpack_enum_val(buf: &[u8]) -> u16 {
    match buf.len() {
        1 => u16::from_be_bytes([0, buf[0]]),
        2 => u16::from_be_bytes([buf[0], buf[1]]),
        _ => 0,
    }
}

// signed(1), year(14), month(4), day(5)
pub fn unpack_newdate_val(b: &Bytes) -> Option<NaiveDate> {
    let arr = [0, b[0], b[1], b[2]];
    let val = u32::from_be_bytes(arr);
    let day = val & 0x1f;
    let month = (val >> 5) & 0xf;
    let year = (val >> (4 + 5)) & 0x3fff;
    let _signed = ((val >> (4 + 5 + 14)) & 0x1) > 0;
    trace!("arr={:?}, val=0x{:0x?}", arr, val,);
    NaiveDate::from_ymd_opt(year as i32, month, day)
}

// u32 => unix timestamp
pub fn unpack_timestamp2_val(b: &Bytes) -> DateTime<Local> {
    let arr = [b[0], b[1], b[2], b[3]];
    let val = u32::from_be_bytes(arr);
    DateTime::from_timestamp(val.into(), 0).unwrap().into()
}

// signed(1), year_month(17), day(5), hour(5), minute(6), second(6)
pub fn unpack_datetime2_val(b: &Bytes) -> Option<NaiveDateTime> {
    let arr = [0, 0, 0, b[0], b[1], b[2], b[3], b[4]];
    let val = u64::from_be_bytes(arr);
    let sec = val & 0x3f;
    let min = (val >> 6) & 0x3f;
    let hour = (val >> (6 + 6)) & 0x1f;
    let day = (val >> (5 + 6 + 6)) & 0x1f;
    let year_month = (val >> (5 + 5 + 6 + 6)) & 0x1ffff;
    let year = year_month / 13;
    let month = year_month % 13;
    let _signed = ((val >> (17 + 5 + 5 + 6 + 6)) & 0x1) > 0;
    debug!("arr={:?}, val=0x{:0x?}", arr, val);
    match NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32) {
        Some(d) => d.and_hms_opt(hour as u32, min as u32, sec as u32),
        None => None,
    }
}

pub fn unpack_u48_val(b: &Bytes) -> u64 {
    assert_eq!(b.len(), 6);
    let arr = [0, 0, b[0], b[1], b[2], b[3], b[4], b[5]];
    u64::from_be_bytes(arr)
}

pub fn unpack_u56_val(b: &Bytes) -> u64 {
    assert_eq!(b.len(), 7);
    let arr = [0, b[0], b[1], b[2], b[3], b[4], b[5], b[6]];
    u64::from_be_bytes(arr)
}

pub fn dateval(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").expect(&format!("ERR_DATE_STR: {}", s))
}

pub fn conv_strdata_to_map(str: &str) -> HashMap<String, String> {
    let mut ret = HashMap::new();
    if str.is_empty() {
        return ret;
    }

    for entry in str.split(';') {
        if let Some(i) = entry.find('=') {
            ret.insert(entry[0..i].to_string(), entry[i + 1..].to_string());
        }
    }

    ret
}

#[cfg(test)]
mod util_tests {

    use log::info;

    use super::*;

    #[test]
    fn it_works() {
        init_unit_test();
        let n = 5;
        let ans: Vec<_> = (0..n).map(|x| x + 1).collect();
        info!("ans={:?}", ans);
    }

    #[test]
    fn test_conv_string_to_map() {
        init_unit_test();
        let str01 = String::from("id=156;root=5;space_id=3;table_id=1065;trx_id=1298;");
        let map01 = conv_strdata_to_map(&str01);
        info!("map01={:?}", map01);
        let id_val: u64 = map01["id"].clone().parse().unwrap();
        assert_eq!(156, id_val);
    }

    #[test]
    fn test_conv_u32() {
        init_unit_test();
        let buf = Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]);
        info!("buf={:?}", buf);
        assert_eq!(u32_val(&buf[0..7], 2), 0x03040506);
    }

    #[test]
    fn test_conv_datetime() {
        init_unit_test();
        let buf = Bytes::from_static(&[0, 0, 0, 0, 0]);
        info!("buf={:?}", buf);
        let ans = unpack_datetime2_val(&buf);
        info!("ans={:?}", ans);
        assert_eq!(ans, None);
    }

    #[test]
    fn test_align_count() {
        init_unit_test();
        assert_eq!(align8(0), 0);
        assert_eq!(align8(1), 1);
        assert_eq!(align8(8), 1);
        assert_eq!(align8(9), 2);
        assert_eq!(align8(254), 32);
        assert_eq!(align8(255), 32);
    }

    #[test]
    fn test_calc_number_offset() {
        init_unit_test();
        assert_eq!(numoff(0), 0);
        assert_eq!(numoff(1), 1);
        assert_eq!(numoff(7), 7);
        assert_eq!(numoff(8), 0);
    }

    #[test]
    fn test_calc_number_index() {
        init_unit_test();
        assert_eq!(numpos(0), 0);
        assert_eq!(numpos(1), 0);
        assert_eq!(numpos(7), 0);
        assert_eq!(numpos(8), 1);
        assert_eq!(numpos(15), 1);
        assert_eq!(numpos(16), 2);
    }
}
