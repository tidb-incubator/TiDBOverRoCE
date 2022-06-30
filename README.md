# TiDB-over-RoCE Project

This project proposes running TiDB over RoCE protocol, which means commucations between TiDB sever, pd and TiKV can benefit from RDMA technology, and higher performance and lower CPU workload are expected.

## Architecture

![tidb-over-roce-arch](./images/tidboverroce-architecture.png)

### Design for go-rdma

TODO
### Design for grpc-rdma

TODO

## Performance


### gRPC?

The benchmark test based on [gRPC performance test](https://github.com/grpc/grpc/tree/master/tools/run_tests/performance)
[gRPC-rdma benchmark test results](./images/gRPC_RDMA_benchmark_result.png)

### YCSB Workloads

YCSB workloads test based on [go-ycsb](https://github.com/pingcap/go-ycsb)

![./ycsb-result.png](./images/ycsb-result.png)

## Steps to use

### TiDB\PD (or any other Go projects like go-mysql-client and ycsb-go)

1. Download all directories in this resposity

2. Set GOROOT to the go-rdma path, build golang binary using `./make.bash` in `go-rdma/src/`

3. Compile rdma-core library using `./build.sh` in `rdma-core` path, and make sure the library path in top of [go-rdma/src/rdma/fd_unix.go](./go-rdma/src/rdma/fd_unix.go) is consistent with your rdma-core path

4. Compile TiDB and PD, then the executable file (tidb-server\pd-server) you got will communicate using RDMA 

### TiKV (or any other projects using grpc/grpc-rs)

[How to integrate TiKV with grpc-rdma](./doc/Integrate_tikv_with_grpc-rdma.md)

## Tips

TODO
