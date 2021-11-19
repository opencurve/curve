# CHANGELOG of v1.2

Previous change logs can be found at [CHANGELOG-1.1](https://github.com/opencurve/curve/blob/master/CHANGELOG-1.1.md)

## new features

- [When add a new pool, default new logic pool is DENY, need tool to enable it.](https://github.com/opencurve/curve/pull/221)
- [When io error occurs, whether nebd-server discards the corresponding rpc request depends on configuration setting(discard default).](https://github.com/opencurve/curve/pull/212)
- [When client read a unallocated space, it does not allocate the segment to improve the space utilization.](https://github.com/opencurve/curve/pull/172)
- [Add data stripe feature.](https://github.com/opencurve/curve/pull/211)


## optimization

- Optimize ansible script, optimize build script, optimize log printing.
- Translate some document and code comment from chinese to english.
- [Curve_ops_tool statistics chunkserver capacity by pool.](https://github.com/opencurve/curve/pull/199)
- Add a script for k8s to attach curve volume.
- curve_ops_tool improve:
    - [Check copyset consistency between chunkserver and copyset.](https://github.com/opencurve/curve/pull/184)
    - [Support to take a snapshot of copyset.](https://github.com/opencurve/curve/pull/188)
    - [Support to list the volume on copyset where majority copy is failed.](https://github.com/opencurve/curve/pull/233)
- [Clean up the unit test temporary folder.](https://github.com/opencurve/curve/pull/206/)


Hardware: 6 nodes, each with:
 - 20x SATA SSD Intel® SSD DC S3500 Series 800G
 - 2x Intel(R) Xeon(R) CPU E5-2660 v4 @ 2.00GHz
 - 2x Intel Corporation 82599ES 10-Gigabit SFI/SFP+ Network Connection, bond mode is 802.3ad with layer2+3 hash policy
 - 251G RAM

Performance test is based on curve-nbd, the size of each block device is 200GB, all configurations are default, and each Chunkserver is deployed on one SSD.

1 NBD block device:

| item | iops/bandwidth | avg-latency | 99th-latency  | striped volume<br>iops/bandwidth | striped volume<br>avg-latency | striped volume<br>99th-latency |
| :----: | :----: | :----: | :----: | :----: |:----: |:----: |
| 4K randwrite, 128 depth | 97,700 iops | 1.3 ms | 2.0 ms | 97,100 iops | 1.3 ms | 3.0 ms |
| 4K randread, 128 depth | 119,000 iops | 1.07 ms | 1.8 ms | 98,000 iops | 1.3 ms | 2.2 ms |
| 512K write, 128 depth | 208 MB/s | 307 ms | 347 ms | 462 MB/s | 138 ms | 228 ms |
| 512K read, 128 depth | 311 MB/s | 206 ms | 264 ms | 843 MB/s | 75 ms | 102 ms |


10 NBD block device:

| item | iops/bandwidth | avg-latency | 99th-latency | striped volume<br>iops/bandwidth | striped volume<br>avg-latency | striped volume<br>99th-latency |
| :----: | :----: | :----: | :----: | :----: |:----: |:----: |
| 4K randwrite, 128 depth | 231,000 iops | 5.6 ms | 50 ms | 227,000 iops | 5.9 ms | 53 ms |
| 4K randread, 128 depth | 350,000 iops | 3.7 ms | 8.2 ms | 345,000 iops | 3.8 ms | 8.2 ms |
| 512K write, 128 depth | 805 MB/s | 415 ms | 600 ms | 1,077 MB/s | 400 ms | 593 ms |
| 512K read, 128 depth | 2,402 MB/s | 267 ms | 275 ms | 3,313 MB/s | 201 ms | 245 ms |


## bug fix

- [Fix clone delete bug.](https://github.com/opencurve/curve/pull/176)
- [Nbd unmap need wait thread exit.](https://github.com/opencurve/curve/pull/228)
- [Mds need check file attach status.](https://github.com/opencurve/curve/pull/153)
- [Fixed when disk fails, copyset reports an error, but sometimes the chunkserver does not exit.](https://github.com/opencurve/curve/pull/152)
- [Fixed the direct_fd leak problem when wal write disk.](https://github.com/opencurve/curve/pull/113)
- [Fixed the atomicity problem of GetFile when not using the file pool.](https://github.com/opencurve/curve/pull/195)
- [Fixed in cluster_basic_test mds start error on InitEtcdClient() which produced coredump](https://github.com/opencurve/curve/pull/205)


<hr/>

# CHANGELOG of v1.2.1-rc0

## New Feature

- [curve-nbd: support simulate 512-byte logical/physical block device](https://github.com/opencurve/curve/pull/444)
- [curve-nbd: prevent mounted device to unmap](https://github.com/opencurve/curve/pull/453)
- [curve-nbd: add auto mount options](https://github.com/opencurve/curve/pull/473)
- [nebd: support multi-instances for different user](https://github.com/opencurve/curve/pull/444)
- [client: retry allocate segment until success](https://github.com/opencurve/curve/pull/452)
- [mds: schedule pending online chunkserver](https://github.com/opencurve/curve/pull/462)

## Bug Fix

- [ansible: backup and recover curvetab](https://github.com/opencurve/curve/pull/484)
- [debian package: backup and recover curvetab](https://github.com/opencurve/curve/pull/491)
- [scripts: fix potential overflow when cnovert ip to value](https://github.com/opencurve/curve/pull/474)
- [client: fix segment in SourceReader](https://github.com/opencurve/curve/pull/474)
- [client: explicit stop LeaseExecutor in FileInstance::UnInitialize](https://github.com/opencurve/curve/pull/474)
- [curve-nbd: fix concurrent nbd map](https://github.com/opencurve/curve/pull/447)
- [snapshotclone: fix cancel task lost](https://github.com/opencurve/curve/pull/463)
- [tools: remove copyset data after remove peer](https://github.com/opencurve/curve/pull/458)
- [mds: modify stale heartbeat update error with warning](https://github.com/opencurve/curve/pull/480)
- [chunkserver: fix config change epoch update error](https://github.com/opencurve/curve/pull/487)


<hr/>

# CHANGELOG of v1.2.2

## Optimization

- [Change auto map service to systemd standard](https://github.com/opencurve/curve/pull/699)
