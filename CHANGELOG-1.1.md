Previous change logs can be found at [CHANGELOG-1.0](https://github.com/opencurve/curve/blob/master/CHANGELOG-1.0.md)

<hr>

## [v1.1.0-beta](https://github.com/opencurve/curve/releases/tag/v1.1.0-beta)(2020-9-24)

**This version is mainly for performance optimization.**

### Performance Optimization
 - [nebd part2 memory zero copy](https://github.com/opencurve/curve/pull/78)
 - [chunkserver memory zero copy](https://github.com/opencurve/curve/pull/81)
 - [client performance optimization](https://github.com/opencurve/curve/pull/88)
 - [datastore read and write thread separation](https://github.com/opencurve/curve/pull/75)
 - [raft wal provide overwrite and direct/non-direct mode](https://github.com/opencurve/curve/pull/92)

### Fix
 - [clean client error log](https://github.com/opencurve/curve/pull/94)