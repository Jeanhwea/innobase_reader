@echo off

rustup run nightly cargo fmt
cargo test
cargo build --release --target=x86_64-pc-windows-gnu
