env "CC_wasm32-wasi=${WASI_CC}" \
CFLAGS="-DSQLITE_OS_OTHER --sysroot=$WASI_SDK_PATH/share/wasi-sysroot" \
AR="$WASI_SDK_PATH/bin/ar" \
CARGO_TARGET_WASM32_WASI_LINKER=$WASI_SDK_PATH/bin/lld \
RUST_LOG=lunatic=debug cargo run --target=wasm32-wasi
