# CHANGELOG of v2.5

Previous change logs can be found at [CHANGELOG-2.4](https://github.com/opencurve/curve/blob/master/CHANGELOG-2.4.md)



## New Features

- Memcache support
    - [curvefs/client: support memcached cluster](https://github.com/opencurve/curve/pull/2096) @ilixiaocui
    - [curvefs/mds: support memcache cluster](https://github.com/opencurve/curve/pull/2108) @Cyber-SiKu
    - [curvefs/client: add global cache client like memcached](https://github.com/opencurve/curve/pull/2102) @fansehep
    - [curvefs/client: fix client core dump error](https://github.com/opencurve/curve/pull/2157) @ilixiaocui
    - [add ut](https://github.com/opencurve/curve/pull/2164) @Cyber-SiKu

- curvefs new tools 
    - [add delete impl](https://github.com/opencurve/curve/pull/2088) @shentupenghui
    - [feat: [tools-v2] bs list dir](https://github.com/opencurve/curve/pull/2082) @Sindweller
    - [fix missing docs links](https://github.com/opencurve/curve/pull/2082) @zyb521
    - [curve/toos-v2: add list client #2037](https://github.com/opencurve/curve/pull/2076) @tsonglew
    - [support create volume directory by curve_ops_tool](https://github.com/opencurve/curve/pull/2078) @aspirer

- [install: added playground.](https://github.com/opencurve/curve/pull/2053) @Wine93

## Optimization

- [update the braft to vesion v1.1.2](https://github.com/opencurve/curve/pull/2091) @tangwz

- [curvefs/client: local cache policy optimization](https://github.com/opencurve/curve/pull/2064) @Tangruilin


- [sync by threadpool](https://github.com/opencurve/curve/pull/1912) @fansehep

- Merge block storage and file storage compilation scripts
    - [Merge block storage and file storage compilation scripts
](https://github.com/opencurve/curve/pull/2089) @linshiyx
    - [Add help_msg in Makefile](https://github.com/opencurve/curve/pull/2133) @linshiyx
    - [fix image.sh](https://github.com/opencurve/curve/pull/2124) @SeanHai

- [fix bs list zone typo](https://github.com/opencurve/curve/pull/2066) @tsonglew 

- [spell optimize](https://github.com/opencurve/curve/pull/2059) @shentupenghui


## Bug Fix

- aws s3 sdk revert
    - [revert new s3 sdk due to critical bug](https://github.com/opencurve/curve/pull/2149) @h0hmj
    - [curvefs/client: change the default value of s3](https://github.com/opencurve/curve/pull/2158) @wuhongsong
