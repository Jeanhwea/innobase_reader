@echo off

rustup run nightly cargo fmt

cargo test

rem cargo build --release
cargo install --path .
