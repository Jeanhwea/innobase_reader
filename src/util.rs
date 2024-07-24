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

pub fn align(num: usize) -> usize {
    (num as f64 / 8.0).ceil() as usize
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
        assert_eq!(align(0), 0);
        assert_eq!(align(1), 1);
        assert_eq!(align(8), 1);
        assert_eq!(align(9), 2);
        assert_eq!(align(254), 32);
        assert_eq!(align(255), 32);
    }
}
