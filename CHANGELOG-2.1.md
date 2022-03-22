# CHANGELOG of v2.1

## new features

- [CurveFS: a curve filesystem can be mounted by multi fuse clients.](https://github.com/opencurve/curve/pull/1101)

- [CurveFS: support summary info in dir xattr.](https://github.com/opencurve/curve/pull/1150)

- [CurveFS: support Multiple s3.](https://github.com/opencurve/curve/pull/1132)

## optimization

- [CurveFS client: adapter lru list for disk cache.](https://github.com/opencurve/curve/pull/1088)

- [CurveFS: meta balance.](https://github.com/opencurve/curve/pull/1105)

- [CurveFS metaserver: let s3 compact task store a shared copyset node.](https://github.com/opencurve/curve/pull/1165)

- [CurveFS: add metric for mds topology to update metaserver metric.](https://github.com/opencurve/curve/pull/1177)

## bug fix

- [CurveFS client: fix bug of getleader always fails causes stack overflow.](https://github.com/opencurve/curve/pull/1070)

- [CurveFS tool: fix copyset health check error.](https://github.com/opencurve/curve/pull/1024)

- [CurveFS client: fix client release read data cache core dump.](https://github.com/opencurve/curve/pull/1090)

- [CurveBS nbd: fix misspell in log.](https://github.com/opencurve/curve/pull/1073)

- [CurveFS mds: update copyset condidate.](https://github.com/opencurve/curve/pull/1085)

- [CurveFS client: fix bug of rpc overtime time not backoff.](https://github.com/opencurve/curve/pull/1139)

- [CurveFS client: fix bug when upload failed.](https://github.com/opencurve/curve/pull/1130)

- [CurveFS client: fix the problem that chunkCacheManager has been releaed.](https://github.com/opencurve/curve/pull/1179)

- [CurveFS: fix partition num in topology metric.](https://github.com/opencurve/curve/pull/1187)

- [CurveFS client: fix core dump of DataCache::Flush & fix rpc timeout not backoff.](https://github.com/opencurve/curve/pull/1193)
