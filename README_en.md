[中文版](README.md)

[![BUILD Status](https://img.shields.io/jenkins/build?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fcurve_multijob%2F)](http://59.111.93.165:8080/job/curve_multijob/lastBuild)
[![Jenkins Coverage](https://img.shields.io/jenkins/coverage/cobertura?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fcurve_multijob%2F)](http://59.111.93.165:8080/job/curve_multijob/HTML_20Report/)

CURVE is a distributed storage system designed and developed independently by NetEase, featured with high performance, high availability, high reliability and well expansibility, and it can serve as the basis for storage systems designed for different scenario (e.g. block storage, object storage and cloud database). 

So far, we have implemented a high performance block storage system, which supports snapshot, clone and recovery, and can be attached on QEMU virtual machine or physical machine (by curve-nbd). CURVE has been served as an elastic block storage service inside NetEase for a certain time, during which high performance and reliability have shown.

## Design Documentation

- Wanna have a glance at CURVE? Click here for [Intro to CURVE](https://opencurve.github.io/)!
- Want more details? Our documentation for every component:
  - [NEBD](docs/en/nebd_en.md)
  - [MDS](docs/en/mds_en.md)
  - [Chunkserver](docs/cn/chunkserver_design.md)
  - [Snapshotcloneserver](docs/cn/snapshotcloneserver.md)
  - [CURVE quality control](docs/cn/quality.md)
  - [CURVE monitoring](docs/cn/monitor.md)
  - [Client](docs/cn/curve-client.md)
  - [Client Python API](docs/cn/curve-client-python-api.md)
- Application based on CURVE
  - [Work with k8s](docs/cn/k8s_csi_interface.md)

## Quick Start

Want to try on it? Take it easy! We'll help you step by step, but make sure you've read this [Special Statement](docs/cn/deploy.md#%E7%89%B9%E5%88%AB%E8%AF%B4%E6%98%8E) before you start.

### Deploy an all-in-one environment (to try how CURVE works)

[Deploy on single machine](docs/cn/deploy.md#%E5%8D%95%E6%9C%BA%E9%83%A8%E7%BD%B2)

### Deploy multi-machine cluster (try it in production environment)

[Deploy on multiple machines](docs/cn/deploy.md#%E5%A4%9A%E6%9C%BA%E9%83%A8%E7%BD%B2)

Feeling good? Maybe you would like to contribute your codes and make CURVE better!

## For Developers

### Deploy build and development environment

[development environment deployment](docs/cn/build_and_run.md)

### Compile test cases and run
[test cases compiling and running](docs/cn/build_and_run.md#%E6%B5%8B%E8%AF%95%E7%94%A8%E4%BE%8B%E7%BC%96%E8%AF%91%E5%8F%8A%E6%89%A7%E8%A1%8C)

### Coding style guides
CURVE is coded following [Google C++ Style Guide strictly](https://google.github.io/styleguide/cppguide.html). Please follow this guideline if you're trying to contribute your codes.

### Code coverage requirement
1. Unit tests：Incremental line coverage ≥ 80%, incremental branch coverage ≥ 70%
2. Integration tests：Measure together with unit tests, and should fulfill the same requirement
3. Exception tests：Not required

### Other processes
TODO

## Release Cycle
- CURVE release cycle：Half a year for major version, 1~2 months for minor version
- Versioning format: We use a sequence of three digits (x.y.z), x is the major version, y is for distinguishing developing , RC and GA version (corresponding to 0, 1 and 2), and z is for marking BugFixes or minor version. Major version x will increase 1 every half year, and z will increase every 1~2 months. 

## Feedback & Contact

- [Github Issues](https://github.com/openCURVE/CURVE/issues)：You are sincerely welcomed to issue any bugs you came across or any suggestions through Github issues. If you have any question you can refer to our FAQ or join our user group for more details.
- [FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)：Frequently asked question in our user group, and we'll keep working on it.
- User group：We use Wechat group currently.

<img src="https://raw.githubusercontent.com/opencurve/opencurve.github.io/master/image/curve-wechat.jpeg" style="zoom: 75%;" />