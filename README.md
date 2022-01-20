[中文版](README_cn.md)


<img src="docs/images/curve-logo1.png"/>

# CURVE

[![Jenkins Coverage](https://img.shields.io/jenkins/coverage/cobertura?jobUrl=http%3A%2F%2F59.111.91.248%3A8080%2Fjob%2Fcurve_untest_job%2F)](http://59.111.91.248:8080/job/curve_untest_job/HTML_20Report/)
[![Robot failover](https://img.shields.io/jenkins/build?jobUrl=http%3A%2F%2F59.111.91.248%3A8080%2Fjob%2Fcurve_failover_testjob%2F&label=failover)](http://59.111.91.248:8080/job/curve_failover_testjob/)
[![Robot interface](https://img.shields.io/jenkins/tests?jobUrl=http%3A%2F%2F59.111.91.248%3A8080%2Fjob%2Fcurve_robot_job%2F)](http://59.111.91.248:8080/job/curve_robot_job/)
[![Curve_choas](https://img.shields.io/jenkins/build?jobUrl=http%3A%2F%2F59.111.91.248%3A8080%2Fjob%2Fcurve_choas_test%2F&label=choas)](http://59.111.91.248:8080/job/curve_choas_test/)
[![BUILD Status](https://img.shields.io/jenkins/build?jobUrl=http%3A%2F%2F59.111.91.248%3A8080%2Fjob%2Fopencurve_multijob%2F)](http://59.111.91.248:8080/job/opencurve_multijob/lastBuild)
[![Docs](https://img.shields.io/badge/docs-latest-green.svg)](https://github.com/opencurve/curve/tree/master/docs)
[![Releases](https://img.shields.io/github/v/release/opencurve/curve?include_prereleases)](https://github.com/opencurve/curve/releases)
[![LICENSE](https://img.shields.io/badge/licence-Apache--2.0%2FGPL-blue)](https://github.com/opencurve/curve/blob/master/LICENSE)

Curve is a distributed storage system designed and developed by NetEase, featured with high performance, easy operation and cloud native. Curve is compose of CurveBS(Curve Block Storage) and CurveFS(Curve FileSystem). CurveBS supports snapshot, clone, and recover, also supports virtual machines with qemu and physical machine with nbd. CurveFS supports POSIX based on Fuse.

## Curve Block Service vs Ceph Block Device

Curve: v1.2.0

Ceph: L/N
### Performance
Curve random read and write performance far exceeds Ceph in the block storage scenario.

Environment：3 replicas on a 6-node cluster, each node has 20xSATA SSD, 2xE5-2660 v4 and 256GB memory.

Single Vol：
<image src="docs/images/1-nbd-en.png">

Multi Vols：
<image src="docs/images/10-nbd-en.png">

### Stability
The stability of the common abnormal Curve is better than that of Ceph in the block storage scenario.
| Fault Case | One Disk Failure | Slow Disk Detect | One Server Failure | Server Suspend Animation |
| :----: | :----: | :----: | :----: | :----: |
| Ceph | jitter 7s | Continuous io jitter | jitter 7s | unrecoverable |
| Curve | jitter 4s | no effect | jitter 4s | jitter 4s |
### Ops
Curve ops is more friendly than Curve in the block storage scenario.
| Ops scenarios | Upgrade clients | Balance |
| :----: | :----: | :----: |
| Ceph | do not support live upgrade | via plug-in with IO influence |
| Curve | support live upgrade with second jitter | auto with no influence on IO |

## Design Documentation

- Wanna have a glance at Curve? Click here for [Intro to Curve](https://www.opencurve.io/)!
- Want more details about CurveBS? Our documentation for every component:
  - [NEBD](docs/en/nebd_en.md)
  - [MDS](docs/en/mds_en.md)
  - [Chunkserver](docs/en/chunkserver_design_en.md)
  - [Snapshotcloneserver](docs/en/snapshotcloneserver_en.md)
  - [CURVE quality control](docs/en/quality_en.md)
  - [CURVE monitoring](docs/en/monitor_en.md)
  - [Client](docs/en/client_en.md)
  - [Client Python API](docs/en/curve-client-python-api_en.md)
- Application based on CurveBS
  - [Work with k8s](docs/en/k8s_csi_interface_en.md)
- Want more details about CurveFS? Our documentation for every component:
  - [Architecture design](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/CurveFS%E6%96%B9%E6%A1%88%E8%AE%BE%E8%AE%A1%EF%BC%88%E6%80%BB%E4%BD%93%E8%AE%BE%E8%AE%A1%EF%BC%8C%E5%8F%AA%E5%AE%9E%E7%8E%B0%E4%BA%86%E9%83%A8%E5%88%86%EF%BC%89.pdf)
  - [Client design](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/CurveFS%20Client%20%E6%A6%82%E8%A6%81%E8%AE%BE%E8%AE%A1.pdf)
  - [Metadata management](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%96%87%E4%BB%B6%E7%B3%BB%E7%BB%9F%E5%85%83%E6%95%B0%E6%8D%AE%E7%AE%A1%E7%90%86.pdf)
  - [Data caching](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%94%AF%E6%8C%81S3%20%E6%95%B0%E6%8D%AE%E7%BC%93%E5%AD%98%E6%96%B9%E6%A1%88.pdf)
  - [Space allocation](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%96%87%E4%BB%B6%E7%B3%BB%E7%BB%9F%E7%A9%BA%E9%97%B4%E5%88%86%E9%85%8D%E6%96%B9%E6%A1%88.pdf)
  - [more details](https://github.com/opencurve/curve-meetup-slides/tree/main/CurveFS)

## Quick Start of CurveBS

Want to try on it? Take it easy! We'll help you step by step, but make sure you've read this [Tips](docs/en/deploy_en.md#Tips) before you start.

### Deploy an all-in-one environment (to try how CURVE works)

[Deploy on single machine](docs/en/deploy_en.md#deploy-on-single-machine)

### Deploy multi-machine cluster (try it in production environment)

[Deploy on multiple machines](docs/en/deploy_en.md#deploy-on-multiple-machines)

### curve_ops_tool introduction

[curve_ops_tool introduction](docs/en/curve_ops_tool_en.md)

## Quick Start of CurveFS
In order to improve the convenience of Curve operation and maintenance, we have designed and developed the [CurveAdm](https://github.com/opencurve/curveadm) project, which is mainly used to deploy and manage Curve clusters. Currently, it supports the deployment of CurveFS ​​(CurveBS support is under development).

Detail for CurveFS deploy: [CurveFS ​​deployment](https://github.com/opencurve/curveadm#deploy-cluster)

## For Developers

### Deploy build and development environment

[development environment deployment](docs/en/build_and_run_en.md)

### Compile test cases and run
[test cases compiling and running](docs/en/build_and_run_en.md#test-case-compilation-and-execution)

### FIO curve block storage engine
Fio curve engine is added, you can clone https://github.com/skypexu/fio/tree/nebd_engine and compile the fio tool with our engine(depend on nebd lib), fio command line example: `./fio --thread --rw=randwrite --bs=4k --ioengine=nebd --nebd=cbd:pool//pfstest_test_ --iodepth=10 --runtime=120 --numjobs=10 --time_based --group_reporting --name=curve-fio-test`

### Coding style guides
CURVE is coded following [Google C++ Style Guide strictly](https://google.github.io/styleguide/cppguide.html). Please follow this guideline if you're trying to contribute your codes.

### Code coverage requirement
1. Unit tests: Incremental line coverage ≥ 80%, incremental branch coverage ≥ 70%
2. Integration tests: Measure together with unit tests, and should fulfill the same requirement
3. Exception tests: Not required yet

### Other processes

After finishing the development of your code, you should submit a pull request to master branch of CURVE and fill out a pull request template. The pull request will trigger the CI automatically, and the code will only be merged after passing the CI and being reviewed.

For more detail, please refer to [CONTRIBUTING](https://github.com/opencurve/curve/blob/master/CONTRIBUTING.md).

## Release Cycle
- CURVE release cycle：Half a year for major version, 1~2 months for minor version

- Versioning format: We use a sequence of three digits and a suffix (x.y.z{-suffix}), x is the major version, y is the minor version, and z is for bugfix. The suffix is for distinguishing beta (-beta), RC (-rc) and GA version (without any suffix). Major version x will increase 1 every half year, and y will increase every 1~2 months. After a version is released, number z will increase if there's any bugfix.

## Branch

All the developments will be done under master branch. If there's any new version to establish, a new branch release-x.y will be pulled from the master, and the new version will be released from this branch.

## Feedback & Contact

- [Github Issues](https://github.com/openCURVE/CURVE/issues)：You are sincerely welcomed to issue any bugs you came across or any suggestions through Github issues. If you have any question you can refer to our FAQ or join our user group for more details.
- [FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)：Frequently asked question in our user group, and we'll keep working on it.
- User group：We use Wechat group currently.
- [Double Week Meetings](https://github.com/opencurve/curve-meetup-slides/tree/main/2022): We have an online community meeting every two weeks which talk about what Curve is doing and planning to do. The time and links of the meeting are public in the user group and [Double Week Meetings](https://github.com/opencurve/curve-meetup-slides/tree/main/2022).

<img src="docs/images/curve-wechat.jpeg" style="zoom: 75%;" />
