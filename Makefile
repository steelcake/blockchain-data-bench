build_zig:
	cd olive-rs && zig build install -Doptimize=ReleaseSafe
run: build_zig
	RUSTFLAGS="-C target-cpu=native" cargo run --release
