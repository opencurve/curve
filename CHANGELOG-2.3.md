# CHANGELOG of v2.3

Previous change logs can be found at [CHANGELOG-2.2](https://github.com/opencurve/curve/blob/master/CHANGELOG-2.2.md)

## Notable Changes

- [CurveFS : adapt to curveadm to support curvebs as data backend](https://github.com/opencurve/curve/pull/1349)

- [CurveFS monitor: promethus](https://github.com/opencurve/curve/pull/1237)

- [CurveFS client: perf optimize](https://github.com/opencurve/curve/pull/1194)

- [CurveFS metaserver: fixed rocksdb storage memory leak caused by unreleasing iterator and transaction.](https://github.com/opencurve/curve/pull/1388)

## new features

- [CurveFS : adapt to curveadm to support curvebs as data backend](https://github.com/opencurve/curve/pull/1349)

- [CurveFS monitor: promethus](https://github.com/opencurve/curve/pull/1237)

- [CurveFS client: add s3.useVirtualAddressing config, default value: false](https://github.com/opencurve/curve/pull/1253)

## optimization

- [CurveFS metaserver: speed up getting inode by padding inode's s3chunk](https://github.com/opencurve/curve/pull/1344)

- [CurveFS client: perf optimize](https://github.com/opencurve/curve/pull/1194)


## bug fix

- [CurveFS mds: fix an uninitialized that spawn infinite threads](https://github.com/opencurve/curve/pull/1270)

- [CurveFS client: fix missing update extent cache](https://github.com/opencurve/curve/pull/1279)

- [CurveFS: fix a compatible issue which caused by modify FsFileType ](https://github.com/opencurve/curve/pull/1359)

- [CurveFS client: fix bug cachedisk never trim](https://github.com/opencurve/curve/pull/1378)

- [CurveFS client: fix bug of io hang](https://github.com/opencurve/curve/pull/1377)

- [CurveFS metaserver: fixed rocksdb storage memory leak caused by unreleasing iterator and transaction.](https://github.com/opencurve/curve/pull/1388)

- [CurveFS client: Adjust the number of retries to the maximum to avoid an error when the number of retries is reached](https://github.com/opencurve/curve/pull/1410)

- [CurveFS : fix clear copyset creating flag when create copyset success](https://github.com/opencurve/curve/pull/1417)

- [CurveFS mds: fix miss set txid when create partition and fix inodeId type](https://github.com/opencurve/curve/pull/1479)

- [CurveFS client: fix the data iteration error when rpc retry.](https://github.com/opencurve/curve/pull/1474)

- [CurveFS metaserver: skip find dentry when loading from snapshot](https://github.com/opencurve/curve/pull/1460)

- [CurveFS : fix compile error which was caused by LatencyUpdater success](https://github.com/opencurve/curve/pull/1508)

- [CurveFS mds: fix create partition error at parallel case](https://github.com/opencurve/curve/pull/1511)

- [CurveFS metaserver: fix the data iteration error when rpc retry.metaserver: recover s3ChunkInfoRemove field for GetOrModify](https://github.com/opencurve/curve/pull/1404)

- [CurveFS metaserver: fixed s3chunkinfo was padding into inode when it wasnt needed](https://github.com/opencurve/curve/pull/1510)

- [Curve common: fix timer bug](https://github.com/opencurve/curve/pull/1492)

- [CurveFS client: fix statfs problem](https://github.com/opencurve/curve/pull/1620)