use std::io::{Read, Write};

use anyhow::Result;
use bytes::Bytes;
use chrono::Local;
use flate2::read::ZlibDecoder;

use std::sync::Once;

static INIT_LOGGER_ONCE: Once = Once::new();

pub fn init() {
    INIT_LOGGER_ONCE.call_once(|| {
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

#[cfg(test)]
mod util_tests {

    use std::env::set_var;

    use super::*;

    fn setup() {
        set_var("RUST_LOG", "info");
        init();
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
