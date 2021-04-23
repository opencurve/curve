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

CURVE is a distributed storage system designed and developed independently by NetEase, featured with high performance, high availability, high reliability and well expansibility, and it can serve as the basis for storage systems designed for different scenario (e.g. block storage, object storage and cloud database). 

So far, we have implemented a high performance block storage system, which supports snapshot, clone and recovery, and it can be attached to QEMU virtual machine or physical machine (by curve-nbd). CURVE has been served as an elastic block storage service inside NetEase for a certain time, during which high performance and reliability have shown.

## Design Documentation

- Wanna have a glance at CURVE? Click here for [Intro to CURVE](https://www.opencurve.io/)!
- Want more details? Our documentation for every component:
  - [NEBD](docs/en/nebd_en.md)
  - [MDS](docs/en/mds_en.md)
  - [Chunkserver](docs/en/chunkserver_design_en.md)
  - [Snapshotcloneserver](docs/en/snapshotcloneserver_en.md)
  - [CURVE quality control](docs/en/quality_en.md)
  - [CURVE monitoring](docs/en/monitor_en.md)
  - [Client](docs/en/client_en.md)
  - [Client Python API](docs/en/curve-client-python-api_en.md)
- Application based on CURVE
  - [Work with k8s](docs/en/k8s_csi_interface_en.md)

## Quick Start

Want to try on it? Take it easy! We'll help you step by step, but make sure you've read this [Special Statement](docs/en/deploy_en.md#special-statement) before you start.

### Deploy an all-in-one environment (to try how CURVE works)

[Deploy on single machine](docs/en/deploy_en.md#deploy-on-single-machine)

### Deploy multi-machine cluster (try it in production environment)

[Deploy on multiple machines](docs/en/deploy_en.md#deploy-on-multiple-machines)

### curve_ops_tool introduction

[curve_ops_tool introduction](docs/en/curve_ops_tool_en.md)

## For Developers

### Deploy build and development environment

[development environment deployment](docs/en/build_and_run_en.md)

### Compile test cases and run
[test cases compiling and running](docs/en/build_and_run_en.md#test-case-compilation-and-execution)

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

<img src="docs/images/curve-wechat.jpeg" style="zoom: 75%;" />
