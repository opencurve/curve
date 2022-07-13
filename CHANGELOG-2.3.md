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

## Data Performance
Hardware: 3 nodes (3*mds, 9*metaserver), each with:
 - Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz
 - 256G RAM
 - disk cache: INTEL SSDSC2BB80 800G(iops is about 30000+,bw is about 300MB)

  - performance is as follows:

s3 backend is minio and the cto is disable([what is cto](https://github.com/opencurve/curve/blob/master/docs/cn/CurveFS%E6%94%AF%E6%8C%81%E5%A4%9A%E6%8C%82%E8%BD%BD.pdf)). and as you know, the performance of read may associated with read cache hit.

| minio + diskcache(free)| iops/bandwidth | avg-latency(ms) | clat 99.00th (ms) | clat 99.99th (ms) |
| :----: | :----: | :----: | :----: | :----: |
| (numjobs 1) (50G filesize) 4k randwrite | 3539 | 0.281 | 1.5 | 16 |
| (numjobs 1) (50G filesize) 4k randread | 2785 | 0.357 | 0.9 | 5.8|
| (numjobs 1) (50G filesize) 512k write | 290 MB/s | 1 | 600 ms | 248|
| (numjobs 1) (50G filesize) 512k read | 216 MB/s | 4.3 | 275 ms | 7.6 |


| minio + diskcache(near full)| iops/bandwidth | avg-latency(ms) | clat 99.00th (ms) | clat 99.99th (ms) |
| :----: | :----: | :----: | :----: | :----: |
| (numjobs 1) (50G filesize) 4k randwrite | 2988 | 0.3 | 1.2 | 18 |
| (numjobs 1) (50G filesize) 4k randread | 1559 | 0.6 | 1.9 | 346|
| (numjobs 1) (50G filesize) 512k write | 266 MB/s | 0.9| 600 ms | 396|
| (numjobs 1) (50G filesize) 512k read | 82 MB/s | 86 | 275 ms | 901|


| minio + diskcache(full)| iops/bandwidth | avg-latency(ms) | clat 99.00th (ms) | clat 99.99th (ms) |
| :----: | :----: | :----: | :----: | :----: |
| (numjobs 1) (20G filesize * 5) 4k randwrite | 2860 | 1.7| 14 | 41 |
| (numjobs 1) (20G filesize * 5) 4k randread | 76 | 65 | 278 | 725|
| (numjobs 1) (20G filesize * 5) 512k write | 240 MB/s | 10| 278 | 513|
| (numjobs 1) (20G filesize * 5) 512k read | 192 MB/s | 12 | 40 | 1955 |

## Metadata Performance

Cluster Topology: 3 nodes (3mds, 6metaserver), each metaserver is deployed on a separate SATA SSD

Configuration: use default configuration

Test tool: mdtest v3.4.0

Test cases:
- case 1: the directory structure is relatively flat and total 130,000 dirs and files.
mdtest -z 2 -b 3 -I 10000 -d /mountpoint

- case 2: the directory structure is relatively deep and total 204,700 dirs and files.
mdtest -z 10 -b 2 -I 100 -d /mountpoint


| Case | Dir creation | Dir stat | Dir rename | Dir removal | File creation | File stat | File read | File removal | Tree creation | Tree removal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| case 1 | 1320 | 5853 | 149 | 670 | 1103 | 5858 | 1669 | 1419 | 851 | 64 |
| case 2 | 1283 | 5205 | 147 | 924 | 1081 | 5316 | 1634 | 1260 | 1302 | 887 |

You can set configuration item ‘fuseClient.enableMultiMountPointRename’ to false in client.conf if you don't need concurrent renames on multiple mountpoints on the same filesystem. It will improve the performance of metadata.

fuseClient.enableMultiMountPointRename=false

| Case | Dir creation | Dir stat | Dir rename | Dir removal | File creation | File stat | File read | File removal | Tree creation | Tree removal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| case 1 | 1537 | 7885 | 530 | 611 | 1256 | 7998 | 1861 | 1614 | 1050 | 72 |
| case 2 | 1471 | 6328 | 509 | 1055 | 1237 | 6874 | 1818 | 1454 | 1489 | 1034 |

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
