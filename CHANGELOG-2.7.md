# CHANGELOG of v2.7

Previous change logs can be found at [CHANGELOG-2.6](https://github.com/opencurve/curve/blob/master/CHANGELOG-2.6.md)

## Features

* [curvefs/client: add quota for S3 client](https://github.com/opencurve/curve/pull/2836), thanks to [@h0hmj][h0hmj].
* [curvefs/client: now we support hadoop sdk](https://github.com/opencurve/curve/pull/2807), thanks to [@Wine93][Wine93].
* [curvefs/client: make user agent configurable](https://github.com/opencurve/curve/pull/2501), thanks to [@Ziy1-Tan][Ziy1-Tan].
* [curvefs/mds: add ability to change capacity in fsinfo](https://github.com/opencurve/curve/pull/2821), thanks to [@h0hmj][h0hmj].
* [curvefs/metaserver: support asynchronous snapshot](https://github.com/opencurve/curve/pull/2691), thanks to [@NaturalSelect][NaturalSelect].
* [curvefs/{client,metaserver}: support braft lease read](https://github.com/opencurve/curve/pull/2599), thanks to [@Xinlong-Chen][Xinlong-Chen].
* [curvefs/*: support space deallocate for curvebs volume as backend](https://github.com/opencurve/curve/pull/2313), thanks to [@ilixiaocui][ilixiaocui].
* [curvefs/monitor: add memcache](https://github.com/opencurve/curve/pull/2834), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/monitor: add cluster usage](https://github.com/opencurve/curve/pull/2842), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/monitor: support plugin](https://github.com/opencurve/curve/pull/2830), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/docker: support run curvefs on Openeuler 22.03-lts-sp2 platform](https://github.com/opencurve/curve/pull/2789), thanks to [@peter5232][peter5232].
* [curvebs/chunkserver: support format chunkfile pool in asynchronous](https://github.com/opencurve/curve/pull/2775), thanks to [@Vigor-jpg][Vigor-jpg].
* [curvebs/{client,chunkserver}: support braft lease read](https://github.com/opencurve/curve/pull/2543), thanks to [@Xinlong-Chen][Xinlong-Chen].
* [curvebs/*: support poolset](https://github.com/opencurve/curve/pull/1988), thanks to [@jolly-sy][jolly-sy].
* [curvebs/*: support 512 aligned IO](https://github.com/opencurve/curve/pull/2538), thanks to [@wu-hanqing][wu-hanqing].
* [curvebs/tool: support list or query scan status](https://github.com/opencurve/curve/pull/2481), thanks to [@lianzhanbiao][lianzhanbiao].
* [curvebs/tool: support get copyset status](https://github.com/opencurve/curve/pull/2520), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvebs/tool: support list may-broken-vol](https://github.com/opencurve/curve/pull/2518), thanks to [@baytan0720][baytan0720].
* [curvebs/tool: support update leader-schedule[-all]](https://github.com/opencurve/curve/pull/2551), thanks to [@montaguelhz][montaguelhz].
* [curvebs/tool: support get cluster status](https://github.com/opencurve/curve/pull/2530), thanks to [@caoxianfei1][caoxianfei1].
* [curvebs/tool: support update volume flatten](https://github.com/opencurve/curve/pull/2757), thanks to [@baytan0720][baytan0720].
* [curvebs/tool: support check chunkserver](https://github.com/opencurve/curve/pull/2632), thanks to [@lng2020][lng2020].
* [curvebs/tool: support create snapshot for all copyset](https://github.com/opencurve/curve/pull/2618), thanks to [@montaguelhz][montaguelhz].
* [curvebs/tool: add snapshot utils](https://github.com/opencurve/curve/pull/2758), thanks to [@baytan0720][baytan0720].

## Improve

* [curvefs/client: optimizing the read amplification problem, especially for memcache](https://github.com/opencurve/curve/pull/2792), thanks to [@wuhongsong][wuhongsong].
* [curvefs/client: split latency metric into data and attr](https://github.com/opencurve/curve/pull/2824), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/client: add kvcache metric](https://github.com/opencurve/curve/pull/2806), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/client: add discache metric](https://github.com/opencurve/curve/pull/2798), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/client: support list or stop warmup progress](https://github.com/opencurve/curve/pull/2652), thanks to [@ken90242][ken90242].
* [curvefs/client: support warmup symbol link](https://github.com/opencurve/curve/pull/2723), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/client: merge two rpc into one rpc for makenode](https://github.com/opencurve/curve/pull/2680), thanks to [@201341][201341].
* [curvefs/client: delete apply index](https://github.com/opencurve/curve/pull/2631), thanks to [@Xinlong-Chen][Xinlong-Chen].
* [curvefs/client: make enableSumInDir to false in default](https://github.com/opencurve/curve/pull/2767), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/client: add s3,diskcache,kvcache io metric](https://github.com/opencurve/curve/pull/2735), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/mds: support modify some configs on fly](https://github.com/opencurve/curve/pull/2813), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/mds: change hostname to optional for register server](https://github.com/opencurve/curve/pull/2574), thanks to [@201341][201341].
* [curvefs/mds: split large test cases into focused smaller ones](https://github.com/opencurve/curve/pull/2717), thanks to [@ken90242][ken90242].
* [curvefs/metaserver: remove some unnecessary error logs](https://github.com/opencurve/curve/pull/2831), thanks to [@SeanHai][SeanHai].
* [curvefs/metaserver: add delete inode metric](https://github.com/opencurve/curve/pull/2822), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/metaserver: separate raft read and write apply queue](https://github.com/opencurve/curve/pull/2498), thanks to [@Xinlong-Chen][Xinlong-Chen].
* [curvefs/{mds,metaserver}: add idle_timeout_sec option](https://github.com/opencurve/curve/pull/2479), thanks to [@201341][201341].
* [curvefs/tool: enable go modules](https://github.com/opencurve/curve/pull/2804), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/tool: support update copyset avail flag](https://github.com/opencurve/curve/pull/2455), thanks to [@baytan0720][baytan0720].
* [curvebs/client: add slow request metric](https://github.com/opencurve/curve/pull/2746), thanks to [@wu-hanqing][wu-hanqing].
* [curvebs/client: delete apply index](https://github.com/opencurve/curve/pull/2630), thanks to [@Xinlong-Chen][Xinlong-Chen].
* [curvebs/chunkserver: add some common functions](https://github.com/opencurve/curve/pull/2826), thanks to [@Xinlong-Chen][Xinlong-Chen].
* [curvebs/chunkserver: enable odsync by default](https://github.com/opencurve/curve/pull/2614), thanks to [@wu-hanqing][wu-hanqing].
* [curvebs/chunkserver: unify apply queue](https://github.com/opencurve/curve/pull/2641), thanks to [@Xinlong-Chen][Xinlong-Chen].
* [curvebs/tool: refactor for remove duplicate codes](https://github.com/opencurve/curve/pull/2534), thanks to [@baytan0720][baytan0720].
* [curvebs/docker: upgrade base image to debian11](https://github.com/opencurve/curve/pull/2629), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [build: install clang-format-14 in dev docker](https://github.com/opencurve/curve/pull/2828), thanks to [@wu-hanqing][wu-hanqing].
* [build: add braft format patch](https://github.com/opencurve/curve/pull/2661), thanks to [@wu-hanqing][wu-hanqing].
* [build: support build specified library](https://github.com/opencurve/curve/pull/2829), thanks to [@quas-modo][quas-modo].
* [build: replace spdlog repo mirror address](https://github.com/opencurve/curve/pull/2786), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [build: replace fmt repo mirror address](https://github.com/opencurve/curve/pull/2796), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [build: deprecate mk-deb.sh mk-tar.sh and use make targets instead](https://github.com/opencurve/curve/pull/2644), thanks to [@charlie0129][charlie0129].
* [build: speed up bazel build](https://github.com/opencurve/curve/pull/2679), thanks to [@h0hmj][h0hmj].
* [build: support build on GCC 11+](https://github.com/opencurve/curve/pull/2642), thanks to [@NaturalSelect][NaturalSelect].
* [tool: support shell completion](https://github.com/opencurve/curve/pull/2662), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [tool: support upgrade curve tool online](https://github.com/opencurve/curve/pull/2674), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [tool: refactor all *map to map](https://github.com/opencurve/curve/pull/2541), thanks to [@XR-stb][XR-stb].
* [test: upgrade googletest to v1.12.1](https://github.com/opencurve/curve/pull/2558), thanks to [@wu-hanqing][wu-hanqing].
* [ci: add clang-format action](https://github.com/opencurve/curve/pull/2739), thanks to [@peter5232][peter5232].
* [ci: add cppcheck action, sanitizers](https://github.com/opencurve/curve/pull/2784), thanks to [@czm23333][czm23333].

## Bugfix
* [curvefs/client: fixed diskcache lru bug](https://github.com/opencurve/curve/pull/2815), thanks to [@wuhongsong][wuhongsong].
* [curvefs/client: fixed some minor bugs.](https://github.com/opencurve/curve/pull/2522), thanks to [@Wine93][Wine93].
* [curvefs/client: fixed check filetype with wrong method](https://github.com/opencurve/curve/pull/2729), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/client: fixed ut fail](https://github.com/opencurve/curve/pull/2743), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/client: fixed wrong metric name](https://github.com/opencurve/curve/pull/2694), thanks to [@SeanHai][SeanHai].
* [curvefs/client: fixed warmup issue which will leads data inconsitent](https://github.com/opencurve/curve/pull/2562), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvefs/mds: fixed partition range cross](https://github.com/opencurve/curve/pull/2727), thanks to [@SeanHai][SeanHai].
* [curvefs/tool: fixed list copyset information error](https://github.com/opencurve/curve/pull/2523), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [curvebs/tool: get copyset status from braft metric on chunkserver](https://github.com/opencurve/curve/pull/2731), thanks to [@caoxianfei1][caoxianfei1].
* [build: fixed buildfs.sh run failed on debian11](https://github.com/opencurve/curve/pull/2511), thanks to [@201341][201341].
* [test: fixed filepool test invalid memory access](https://github.com/opencurve/curve/pull/2575), thanks to [@wu-hanqing][wu-hanqing].
* [ci: let ci works better](https://github.com/opencurve/curve/pull/2812), thanks to [@wu-hanqing][wu-hanqing].

## Doc
* [doc: updated maintainers for curve project](https://github.com/opencurve/curve/pull/2695), thanks to [@aspirer][aspirer].
* [doc: fix typo](https://github.com/opencurve/curve/pull/2799), thanks to [@NopeDl][NopeDl].
* [doc: updated tool's readme](https://github.com/opencurve/curve/pull/2506), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [doc: replace harbor with opencurve](https://github.com/opencurve/curve/pull/2549), thanks to [@Cyber-SiKu][Cyber-SiKu].
* [doc: fixed some wrong usage exmaple for tool](https://github.com/opencurve/curve/pull/2675), thanks to [@montaguelhz][montaguelhz].

[Cyber-SiKu]: https://github.com/Cyber-SiKu
[h0hmj]: https://github.com/h0hmj
[wu-hanqing]: https://github.com/wu-hanqing
[Wine93]: https://github.com/Wine93
[Vigor-jpg]: https://github.com/Vigor-jpg
[SeanHai]: https://github.com/SeanHai
[quas-modo]: https://github.com/quas-modo
[wuhongsong]: https://github.com/wuhongsong
[peter5232]: https://github.com/peter5232
[aspirer]: https://github.com/aspirer
[Xinlong-Chen]: https://github.com/Xinlong-Chen
[NopeDl]: https://github.com/NopeDl
[charlie0129]: https://github.com/charlie0129
[201341]: https://github.com/201341
[baytan0720]: https://github.com/baytan0720
[caoxianfei1]: https://github.com/caoxianfei1
[czm23333]: https://github.com/czm23333
[dependabot]: https://github.com/dependabot
[ilixiaocui]: https://github.com/ilixiaocui
[jolly-sy]: https://github.com/jolly-sy
[ken90242]: https://github.com/ken90242
[lianzhanbiao]: https://github.com/lianzhanbiao
[lng2020]: https://github.com/lng2020
[montaguelhz]: https://github.com/montaguelhz
[NaturalSelect]: https://github.com/NaturalSelect
[setcy]: https://github.com/setcy
[XR-stb]: https://github.com/XR-stb
[Ziy1-Tan]: https://github.com/Ziy1-Tan

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
