# CHANGELOG of v1.3

Previous change logs can be found at [CHANGELOG-1.2](https://github.com/opencurve/curve/blob/master/CHANGELOG-1.2.md)

## new features

- [Support discard to garbage collection.](https://github.com/opencurve/curve/pull/189)
- [QoS of client.](https://github.com/opencurve/curve/pull/268)
- [QoS of SnapshotCloneServer.](https://github.com/opencurve/curve/pull/303)
- [Recover file in recycleBin.](https://github.com/opencurve/curve/pull/259)
- [Fill chunks in chunkfile pool with 0 in the background.](https://github.com/opencurve/curve/pull/322)
- [Silent data CRC verification in the background.](https://github.com/opencurve/curve/pull/377)
- [Periodic automatic cleaning of recyclebin.](https://github.com/opencurve/curve/pull/310)


## optimization

- Optimize optimize build script, optimize log printing.
- Translate some document and code comment from Chinese to English.
- [Mark a online chunkserver to pendding status to migrate it's data.](https://github.com/opencurve/curve/pull/252)
- [Clean up the remaining copyset after remove peer.](https://github.com/opencurve/curve/pull/373)
- [Nbd auto map at reboot.](https://github.com/opencurve/curve/pull/347)
- ansible script improve:
    - [Improve bool type var and some issues.](https://github.com/opencurve/curve/pull/331)
    - [Improve daemon restart.](https://github.com/opencurve/curve/pull/315)
    - [Incorporate some scripts in debain package and ansible template.](https://github.com/opencurve/curve/pull/380)


## bug fix

- [Fix recyclebin space statistics error bug.](https://github.com/opencurve/curve/pull/294)
- [Fix a metric bug when not use walpool.](https://github.com/opencurve/curve/pull/291)
- [Fix a nbd map concurrently bug.](https://github.com/opencurve/curve/pull/302)
- [Fix discard and flatten concurrency issues.](https://github.com/opencurve/curve/pull/312)
- [Fix max chunk id calculation error when use chunkfile poo as wal pool.](https://github.com/opencurve/curve/pull/341)
- [Fix client retry when allocate segment failed as cluster space is pool.](https://github.com/opencurve/curve/pull/338)
- [Add protection for unmap operation when io is not stopped.](https://github.com/opencurve/curve/pull/348)
- [Fix a segment fault when client read from clone source.](https://github.com/opencurve/curve/pull/358)
