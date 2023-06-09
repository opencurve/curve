# CHANGELOG of v2.6

Previous change logs can be found at [CHANGELOG-2.5](https://github.com/opencurve/curve/blob/master/CHANGELOG-2.5.md)

## Features

* [curvefs/client: add qos for curve-fuse, we can limits the bandwidth](https://github.com/opencurve/curve/pull/2424), thanks to [@UniverseParticle](https://github.com/UniverseParticle).
* [curvefs/client:add memcache to warmup](https://github.com/opencurve/curve/pull/2270), thanks to [@Cyber-SiKu](https://github.com/Cyber-SiKu).

## Improve

* [curvefs/client: improve metadata performance](https://github.com/opencurve/curve/pull/2387), thanks to [@Wine93](https://github.com/Wine93).
* [curvefs/client: change the data organization format](https://github.com/opencurve/curve/pull/2420), thanks to [@201341](https://github.com/201341).
* [curvefs/client: optimize DiskCacheManager::UmountDiskCache()](https://github.com/opencurve/curve/pull/2252), thanks to [@Ziy1-Tan](https://github.com/Ziy1-Tan).
* [curvefs/client: add memcached startup check](https://github.com/opencurve/curve/pull/2296), thanks to [@Ziy1-Tan](https://github.com/Ziy1-Tan).

## Bugfix

* [curvefs/client: enableSumInDir in multi-mount situation](https://github.com/opencurve/curve/pull/2491), thanks to [@bit-dance](https://github.com/bit-dance).
* [curvefs/client: fuse client exit when cache size less than 8MB](https://github.com/opencurve/curve/pull/2492), thanks to [@Ziy1-Tan](https://github.com/Ziy1-Tan).
* [curvefs/client: remove unused setting fuseMaxSize](https://github.com/opencurve/curve/pull/2497), thanks to [@Ziy1-Tan](https://github.com/Ziy1-Tan).
* [curvefs/client: enable async read for FileCacheManager::ReadKVRequest](https://github.com/opencurve/curve/pull/2421), thanks to [@Ziy1-Tan](https://github.com/Ziy1-Tan).


# Performance

## Hardware
3 nodes (3*mds, 9*metaserver), each with:
 - Intel(R) Xeon(R) CPU E5-2680 v4 @ 2.40GHz
 - 256G RAM
 - disk cache: INTEL SSDSC2BB80 800G (IOPS is about 30000+, bandwidth is about 300 MiB)

## Configure

```yaml
fs.cto: true
fs.lookupCache.negativeTimeoutSec: 1
fs.lookupCache.minUses: 3
fuseClient.supportKVcache: true
client.loglevel: 0
```

## fio

```bash
[global]
rw=randread
direct=1
size=50G
iodepth=128
ioengine=libaio
bsrange=4k-4k
ramp_time=10
runtime=300
group_reporting

[disk01]
filename=/path/to/mountpoint/1.txt
```

| fio | IOPS/bandwidth | avg-latency(ms) | clat 99.00th (ms) | clat 99.99th (ms) |
| :----: | :----: | :----: | :----: | :----: |
| numjobs=1 / size=50GB / 4k randwrite | 4243      | 0.23 | 0.176 | 2   |
| numjobs=1 / size=50GB / 4k randwrite | 908       | 1.0  | 3.5   | 104 |
| numjobs=1 / size=50GB / 512k write   | 412 MiB/s | 2.4  | 19    | 566 |
| numjobs=1 / size=50GB / 512k read    | 333 MiB/s | 2.9  | 20    | 115 |

## mdtest

```bash
for i in 1 4 8; do mpirun --allow-run-as-root -np $i mdtest -z 2 -b 3 -I 10000 -d /path/to/mountpoint; done
```

| Case | Dir creation | Dir stat | Dir removal | File creation | File stat | File read | File removal | Tree creation | Tree removal |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| client*1 | 341 | 395991 | 291 | 334 | 383844 | 3694 | 309 | 322 | 851 |
| client*4 | 385 | 123266 | 288 | 361 | 1515592 | 15056 | 310 | 363 | 16 |
| client*8 | 415 | 22138 | 314 | 400 | 2811416 | 20976 | 347 | 355 | 8 |