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

## bug fix

- [Fix clone delete bug.](https://github.com/opencurve/curve/pull/176)
- [Nbd unmap need wait thread exit.](https://github.com/opencurve/curve/pull/228)
- [Mds need check file attach status.](https://github.com/opencurve/curve/pull/153)
- [Fixed when disk fails, copyset reports an error, but sometimes the chunkserver does not exit.](https://github.com/opencurve/curve/pull/152)
- [Fixed the direct_fd leak problem when wal write disk.](https://github.com/opencurve/curve/pull/113)
- [Fixed the atomicity problem of GetFile when not using the file pool.](https://github.com/opencurve/curve/pull/195)
- [Fixed in cluster_basic_test mds start error on InitEtcdClient() which produced coredump](https://github.com/opencurve/curve/pull/205)
