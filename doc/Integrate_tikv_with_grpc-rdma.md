# Integrate tikv with grpc-rdma
## Build grpc-rs with grpc-rdma code
###  Clone and build grpc-rs repo
1. `git clone https://github.com/tikv/grpc-rs.git`
2. Change to the repo's directory and build:
```shell
cd tikv/grpc-rs
cargo xtask submodule
cargo build
```
### Backup the abseil-cpp
```shell
cd grpc-sys/grpc/third_party
tar -zcf abseil-cpp.bak.tar.gz abseil-cpp/
```
### Remove submodule cache
```bash
cd tikv/grpc-rs
git rm --cached grpc-sys/grpc
```
### Replace the grpc-sys 
```bash
cd  grpc-sys/
mv grpc grpc-bak
cp -rf TiDBOverRoCE/grpc ./grpc
```
### Modify grpc-rs dependencies
#### Add librdmacm、libibverbs in dependencies
```bash
cd grpc-rs/grpc-sys/
vim build.rs
```
Add the following code in build.rs line 233:
```rust
println!("cargo:rustc-link-search=all=/usr/lib/x86_64-linux-gnu");
println!("cargo:rustc-link-lib=dylib=rdmacm");
println!("cargo:rustc-link-lib=dylib=ibverbs");
```
**Notice: In most cases, librdmacm/libverbs are installed in /usr/lib/x86_64-linux-gnu on x64 platforms, or your own compiled path if the libs are built from source**

#### modify xtask
`vim grpc-rs/xtask/src/main.rs`
Find the implementation of submodule() and comment the codes in submodule() to disable git submodule update :

```rust
fn submodule() {
//
//	origin code
//
}
```
### Copy abseil-cpp_bak to grpc-sys/grpc/third_party:
```bash
cp -rf grpc-sys/grpc-bak/third_party/abseil-cpp/ grpc-sys/grpc/third_party
```
Or extract the abseil-cpp.bak.tar.gz to grpc-sys/grpc/third_party

### Build grpc-rs again
```bash
cd grpc-rs/
cargo xtask submodule
cargo build
```

## Build Tikv
### Clone the Tikv repo
`git clone https://github.com/tikv/tikv.git`

### Replace tikv/grpc-rs with local grpc-rs
Modify the dependency on grpc-rs in tikv, open the tikv/Cargo.toml and add following code in [patch.crates-io] :
```toml
[patch.crates-io]
...
grpcio = { path = "..(local path)/grpc-rs", version = "0.9", default-features = false, features = ["openssl-vendored"]}
grpcio-health = { path = "..(local path)/grpc-rs/health", version = "0.9", default-features = false}
```
### Build 
```bash
cd tikv/
make build
```