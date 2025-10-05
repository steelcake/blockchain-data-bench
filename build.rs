const LIB_NAME: &str = "olivers";
const ZIG_OUT_DIR: &str = "./olive-rs/zig-out/lib";

fn main() {
    println!("cargo:rustc-link-search=native={}", ZIG_OUT_DIR);
    println!("cargo:rustc-link-lib=static={}", LIB_NAME);
    println!("cargo:rerun-if-changed={}", ZIG_OUT_DIR);
}
