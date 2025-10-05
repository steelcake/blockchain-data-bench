use std::{env, fs, path::Path};

const LIB_NAME: &str = "olivers";
const ZIG_OUT_DIR: &str = "./olive-rs/zig-out/lib";

fn main() {
    println!("cargo:rustc-link-search=native={}", ZIG_OUT_DIR);
    println!("cargo:rustc-link-lib=static={}", LIB_NAME);
}
