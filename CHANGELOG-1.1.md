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


Hardware: 6 nodes, each with:
 - 20x SATA SSD Intel® SSD DC S3500 Series 800G
 - 2x Intel(R) Xeon(R) CPU E5-2660 v4 @ 2.00GHz
 - 2x Intel Corporation 82599ES 10-Gigabit SFI/SFP+ Network Connection, bond mode is 802.3ad with layer2+3 hash policy
 - 251G RAM

Performance test is based on curve-nbd, the size of each block device is 200GB, all configurations are default, and each Chunkserver is deployed on one SSD.

1 NBD block device:

| item | iops/bandwidth | avg-latency | 99th-latency  | release-1.0<br>iops/bandwidth | release-1.0<br>avg-latency | release-1.0<br>99th-latency | iops/bandwidth improvement percentage |
| :----: | :----: | :----: | :----: | :----: |:----: |:----: | :----: |
| 4K randwrite, 128 depth | 109,000 iops | 1,100 us | 2,040 us | 62,900 iops | 2,000 us | 3,000 us | 73% |
| 4K randread, 128 depth | 128,000 iops | 1,000 us | 1,467 us | 76,600 iops | 1,600 us | 2,000us | 67% |
| 512K write, 128 depth | 204 MB/s | 314 ms | 393 ms | 147 MB/s | 435 ms | 609 ms| 38% |
| 512K read, 128 depth | 995 MB/s | 64 ms | 92 ms | 757 MB/s | 84 ms | 284 ms| 31% |


10 NBD block device:

| item | iops/bandwidth | avg-latency | 99th-latency | release-1.0<br>iops/bandwidth | release-1.0<br>avg-latency | release-1.0<br>99th-latency | iops/bandwidth improvement percentage |
| :----: | :----: | :----: | :----: | :----: |:----: |:----: | :----: |
| 4K randwrite, 128 depth | 262,000 iops | 4.9 ms | 32 ms | 176,000 iops | 7.2 ms | 16 ms | 48% |
| 4K randread, 128 depth | 497,000 iops | 2.69 ms | 6 ms | 255,000 iops | 5.2 ms | 22 ms | 94% |
| 512K write, 128 depth | 1,122 MB/s | 569 ms | 1,101 ms | 899 MB/s | 710 ms | 1,502 ms | 24% |
| 512K read, 128 depth | 3,241 MB/s | 200 ms | 361 ms | 1,657 MB/s | 386 ms | 735 ms | 95% |



### Fix
 - [clean client error log](https://github.com/opencurve/curve/pull/94)