use std::io::{Read, Write};

use anyhow::Result;
use bytes::Bytes;
use chrono::Local;
use flate2::read::ZlibDecoder;

pub fn initlog() {
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
}

pub fn uncomp(din: Bytes) -> Result<String> {
    let buf = din.to_vec();
    let mut decoder = ZlibDecoder::new(&*buf);
    let mut dout = String::new();
    decoder.read_to_string(&mut dout)?;
    Ok(dout)
}
