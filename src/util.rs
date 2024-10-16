use std::{
    cmp::min,
    collections::HashMap,
    env::set_var,
    fmt::{Binary, Debug, Display, LowerHex},
    io::{Read, Write},
    sync::{Arc, Once},
};

use anyhow::Result;
use bytes::Bytes;
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime};
use colored::{ColoredString, Colorize};
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
        env_logger::builder()
            .is_test(true)
            .format_timestamp(None)
            .init();
    });
}

pub fn fmt_bin8<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display + Binary + LowerHex,
{
    write!(f, "0b{:08b}(0x{:02x})", d, d)
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

pub fn fmt_bool(d: &bool, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    write!(f, "{}", if *d { "T".green() } else { "F".red() })
}

pub fn fmt_enum<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display,
{
    write!(f, "{}", d.to_string().magenta())
}

pub fn fmt_enum_2<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display,
{
    write!(f, "{}", d.to_string().cyan())
}

pub fn fmt_enum_3<T>(d: &T, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error>
where
    T: Display,
{
    write!(f, "{}", d.to_string().green())
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

pub fn fmt_str(d: &String, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    write!(f, "{}", format!("{:?}", d).yellow())
}

pub fn fmt_bytes_vec(d: &Bytes, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    write!(f, "{}", format!("{:?}", d.to_vec()).blue())
}

pub fn fmt_bytes_bin(d: &Bytes, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
    let vs = d.to_vec();
    if vs.is_empty() {
        write!(f, "[]")
    } else {
        let _ = writeln!(f, "[");
        let n = d.len();
        for (i, e) in d.iter().enumerate() {
            let _ = write!(f, "    0b{:08b}", e);
            if i < n - 1 {
                let _ = writeln!(f, ", ");
            } else {
                let _ = writeln!(f);
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

pub fn colored_page_number(page_no: usize) -> ColoredString {
    format!("#{}", page_no).cyan()
}

pub fn colored_extent_number(xdes_no: usize) -> ColoredString {
    format!("${}", xdes_no).yellow()
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

pub fn bitmap_shift(num: usize) -> usize {
    num & 0x7
}

pub fn bitmap_index(num: usize) -> usize {
    (num & (!0x7)) >> 3
}

pub fn i16_val(buf: &[u8], addr: usize) -> i16 {
    i16::from_be_bytes(buf[addr..addr + 2].try_into().expect("ERR_READ_VALUE_i16"))
}

pub fn i32_val(buf: &[u8], addr: usize) -> i32 {
    i32::from_be_bytes(buf[addr..addr + 4].try_into().expect("ERR_READ_VALUE_i32"))
}

pub fn u8_val(buf: &[u8], addr: usize) -> u8 {
    u8::from_be_bytes(buf[addr..addr + 1].try_into().expect("ERR_READ_VALUE_u8"))
}

pub fn u16_val(buf: &[u8], addr: usize) -> u16 {
    u16::from_be_bytes(buf[addr..addr + 2].try_into().expect("ERR_READ_VALUE_u16"))
}

pub fn u32_val(buf: &[u8], addr: usize) -> u32 {
    u32::from_be_bytes(buf[addr..addr + 4].try_into().expect("ERR_READ_VALUE_u32"))
}

pub fn u48_val(buf: &[u8], addr: usize) -> u64 {
    let arr = [
        0u8,
        0u8,
        buf[addr],
        buf[addr + 1],
        buf[addr + 2],
        buf[addr + 3],
        buf[addr + 4],
        buf[addr + 5],
    ];
    u64::from_be_bytes(arr)
}

pub fn u56_val(buf: &[u8], addr: usize) -> u64 {
    let arr = [
        0u8,
        buf[addr],
        buf[addr + 1],
        buf[addr + 2],
        buf[addr + 3],
        buf[addr + 4],
        buf[addr + 5],
        buf[addr + 6],
    ];
    u64::from_be_bytes(arr)
}

pub fn u64_val(buf: &[u8], addr: usize) -> u64 {
    u64::from_be_bytes(buf[addr..addr + 8].try_into().expect("ERR_READ_VALUE_u64"))
}

pub fn str_val(buf: &[u8], addr: usize, len: usize) -> String {
    assert!(addr + len <= buf.len());
    let bytes = (addr..addr + len)
        .take_while(|i| buf[*i] != 0)
        .map(|i| buf[i])
        .collect();
    String::from_utf8(bytes).unwrap_or("???".to_string())
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
        let b = [
            buf[0] & 0x7f,
            buf[1],
            buf[2],
            buf[3],
            buf[4],
            buf[5],
            buf[6],
            buf[7],
        ];
        i64::from_be_bytes(b)
    } else {
        let b = [
            buf[0] | 0x80,
            buf[1],
            buf[2],
            buf[3],
            buf[4],
            buf[5],
            buf[6],
            buf[7],
        ];
        i64::from_be_bytes(b)
    }
}

/// enumeration value
pub fn unpack_enum_val(buf: &[u8]) -> u16 {
    match buf.len() {
        1 => u16::from_be_bytes([0, buf[0]]),
        2 => u16::from_be_bytes([buf[0], buf[1]]),
        _ => 0,
    }
}

/// signed(1), year(14), month(4), day(5)
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

/// u32 => unix timestamp
pub fn unpack_timestamp2_val(b: &Bytes) -> DateTime<Local> {
    let arr = [b[0], b[1], b[2], b[3]];
    let val = u32::from_be_bytes(arr);
    DateTime::from_timestamp(val.into(), 0).unwrap().into()
}

/// signed(1), year_month(17), day(5), hour(5), minute(6), second(6)
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
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap_or_else(|_| panic!("日期字符串格式错误: {}", s))
}

pub fn conv_strdata_to_map(s: &str) -> HashMap<String, String> {
    let mut ret = HashMap::new();
    if s.is_empty() {
        return ret;
    }

    for entry in s.split(';') {
        if let Some(i) = entry.find('=') {
            ret.insert(entry[0..i].to_string(), entry[i + 1..].to_string());
        }
    }

    ret
}

pub fn conv_strdata_to_bytes(s: &str) -> Option<Bytes> {
    if s.is_empty() {
        return None;
    }
    let mut arr = Vec::with_capacity(s.len() / 2);
    for i in 0..s.len() / 2 {
        let beg = 2 * i;
        let s1 = &s[beg..beg + 2];
        arr.push(u8::from_str_radix(s1, 16).unwrap_or(0));
    }
    Some(Bytes::from(arr))
}

pub fn mach_read_from_1(addr: usize, buf: Arc<Bytes>) -> u32 {
    let arr = [0u8, 0u8, 0u8, buf[addr]];
    u32::from_be_bytes(arr)
}

pub fn mach_read_from_2(addr: usize, buf: Arc<Bytes>) -> u32 {
    let arr = [0u8, 0u8, buf[addr], buf[addr + 1]];
    u32::from_be_bytes(arr)
}

pub fn mach_read_from_3(addr: usize, buf: Arc<Bytes>) -> u32 {
    let arr = [0u8, buf[addr], buf[addr + 1], buf[addr + 2]];
    u32::from_be_bytes(arr)
}

pub fn mach_read_from_4(addr: usize, buf: Arc<Bytes>) -> u32 {
    let arr = [buf[addr], buf[addr + 1], buf[addr + 2], buf[addr + 3]];
    u32::from_be_bytes(arr)
}

/// Read a ulint in a compressed form.
pub fn mach_read_compressed(addr: usize, buf: Arc<Bytes>) -> u32 {
    let mut val = u8_val(&buf, addr) as u32;
    if val < 0x80 {
        /* 0nnnnnnn (7 bits) */
    } else if val < 0xC0 {
        /* 10nnnnnn nnnnnnnn (14 bits) */
        val = mach_read_from_2(addr, buf.clone()) & 0x3FFF;
        assert!(val > 0x7F);
    } else if val < 0xE0 {
        /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
        val = mach_read_from_3(addr, buf.clone()) & 0x1FFFFF;
        assert!(val > 0x3FFF);
    } else if val < 0xF0 {
        /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
        val = mach_read_from_4(addr, buf.clone()) & 0xFFFFFFF;
        assert!(val > 0x1FFFFF);
    } else if val < 0xF8 {
        /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
        assert_eq!(val, 0xF0);
        val = mach_read_from_4(addr + 1, buf.clone());
        /* this can treat not-extended format also. */
        assert!(val > 0xFFFFFFF);
    } else if val < 0xFC {
        /* 111110nn nnnnnnnn (10 bits) (extended) */
        val = (mach_read_from_2(addr, buf.clone()) & 0x3FF) | 0xFFFFFC00;
    } else if val < 0xFE {
        /* 1111110n nnnnnnnn nnnnnnnn (17 bits) (extended) */
        val = (mach_read_from_3(addr, buf.clone()) & 0x1FFFF) | 0xFFFE0000;
        assert!(val < 0xFFFFFC00);
    } else {
        /* 11111110 nnnnnnnn nnnnnnnn nnnnnnnn (24 bits) (extended) */
        assert_eq!(val, 0xFE);
        val = mach_read_from_3(addr + 1, buf.clone()) | 0xFF000000;
        assert!(val < 0xFFFE0000);
    }

    trace!(
        "val = {:?}, buf = {:?}",
        val,
        buf.slice(addr..min(addr + 5, buf.len())).to_vec()
    );

    val
}

/// Return the size of an ulint when written in the compressed form.
pub fn mach_get_compressed_size(n: u32) -> usize {
    if n < 0x80 {
        /* 0nnnnnnn (7 bits) */
        1
    } else if n < 0x4000 {
        /* 10nnnnnn nnnnnnnn (14 bits) */
        return 2;
    } else if n < 0x200000 {
        /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
        return 3;
    } else if n < 0x10000000 {
        /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
        return 4;
    } else if n >= 0xFFFFFC00 {
        /* 111110nn nnnnnnnn (10 bits) (extended) */
        return 2;
    } else if n >= 0xFFFE0000 {
        /* 1111110n nnnnnnnn nnnnnnnn (17 bits) (extended) */
        return 3;
    } else if n >= 0xFF000000 {
        /* 11111110 nnnnnnnn nnnnnnnn nnnnnnnn (24 bits) (extended) */
        return 4;
    } else {
        /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
        return 5;
    }
}

/// Reads a 64-bit integer in much compressed form.
pub fn u64_much_compressed(addr: usize, buf: Arc<Bytes>) -> (usize, u64) {
    let b0 = u8_val(&buf, addr);
    if b0 != 0xFF {
        let low32 = mach_read_compressed(addr, buf.clone());
        let size = mach_get_compressed_size(low32);
        return (size, low32 as u64);
    }

    let high = mach_read_compressed(addr + 1, buf.clone());
    let high_size = mach_get_compressed_size(high);
    let low = mach_read_compressed(addr + 1 + high_size, buf.clone());
    let low_size = mach_get_compressed_size(low);
    let val = ((high as u64) << 32) | (low as u64);

    (1 + high_size + low_size, val)
}

/// Reads a 64-bit integer in compressed form.
pub fn u64_compressed(addr: usize, buf: Arc<Bytes>) -> (usize, u64) {
    let high = mach_read_compressed(addr, buf.clone());
    let high_size = mach_get_compressed_size(high);
    let low = u32_val(&buf, addr + high_size);
    let val = ((high as u64) << 32) | (low as u64);

    (high_size + 4, val)
}

/// Reads a 32-bit integer in much compressed form.
pub fn u32_compressed(addr: usize, buf: Arc<Bytes>) -> (usize, u32) {
    let value = mach_read_compressed(addr, buf.clone());
    let size = mach_get_compressed_size(value);
    (size, value)
}

#[cfg(test)]
mod util_tests {

    use std::string::String;

    use log::info;

    use super::*;

    fn newbuf(data: &[u8]) -> Arc<Bytes> {
        Arc::new(Bytes::copy_from_slice(data))
    }

    #[test]
    fn mach_read_from_bytes_array() {
        init_unit_test();
        let buf = newbuf(&[1, 2, 3, 4]);
        let ans01 = mach_read_from_1(0, buf.clone());
        assert_eq!(ans01, 1);
        let ans02 = mach_read_from_2(0, buf.clone());
        assert_eq!(ans02, 0x0102);
        let ans03 = mach_read_from_3(0, buf.clone());
        assert_eq!(ans03, 0x010203);
        let ans04 = mach_read_from_4(0, buf.clone());
        assert_eq!(ans04, 0x01020304);
    }

    #[test]
    fn test_mach_read_compressed_u32() {
        init_unit_test();
        assert_eq!(mach_read_compressed(0, newbuf(&[1, 2, 3, 4])), 1);
        // 0xaa => 0b10101010
        assert_eq!(mach_read_compressed(0, newbuf(&[0xaa, 3, 0, 0, 0])), 0x2a03);
        // 1144
        assert_eq!(mach_read_compressed(0, newbuf(&[132, 120, 0, 0])), 1144);
        // 88
        assert_eq!(mach_read_compressed(0, newbuf(&[88])), 88);
    }

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
    fn test_conv_strdata_to_bytes() {
        init_unit_test();
        let s = "63355f64656620202020";
        let ans = conv_strdata_to_bytes(s);
        assert!(ans.is_some());
        info!("ans={:?}", ans);
        let ret = String::from_utf8(ans.unwrap().to_vec()).unwrap();
        assert_eq!("c5_def    ".to_string(), ret);

        let s2 = "";
        let ans2 = conv_strdata_to_bytes(s2);
        assert!(ans2.is_none());
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
        let buf = Bytes::from_static(&[0x99, 0xb4, 0x11, 0x77, 0x96]);
        info!("buf={:?}", buf);
        let ans = unpack_datetime2_val(&buf);
        info!("ans={:?}", ans);
        assert!(ans.is_some());
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
        assert_eq!(bitmap_shift(0), 0);
        assert_eq!(bitmap_shift(1), 1);
        assert_eq!(bitmap_shift(7), 7);
        assert_eq!(bitmap_shift(8), 0);
    }

    #[test]
    fn test_calc_number_index() {
        init_unit_test();
        assert_eq!(bitmap_index(0), 0);
        assert_eq!(bitmap_index(1), 0);
        assert_eq!(bitmap_index(7), 0);
        assert_eq!(bitmap_index(8), 1);
        assert_eq!(bitmap_index(15), 1);
        assert_eq!(bitmap_index(16), 2);
    }
}
