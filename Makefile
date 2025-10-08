build_zig:
	cd olive-rs && zig build install -Doptimize=ReleaseSafe
build_zig_debug:
	cd olive-rs && zig build install 
run: build_zig
	RUSTFLAGS="-C target-cpu=native" cargo run --release
run_debug: build_zig_debug
	RUSTFLAGS="-C target-cpu=native" cargo run 
