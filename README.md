[![BUILD Status](https://img.shields.io/jenkins/build?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fcurve_multijob%2F)](http://59.111.93.165:8080/job/curve_multijob/lastBuild)
[![Jenkins Coverage](https://img.shields.io/jenkins/coverage/cobertura?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fcurve_multijob%2F)](http://59.111.93.165:8080/job/curve_multijob/HTML_20Report/)

CURVE是网易自主设计研发的高性能、高可用、高可靠分布式存储系统，具有非常良好的扩展性。基于该存储底座可以打造适用于不同应用场景的存储系统，如块存储、对象存储、云原生数据库等。当前我们基于CURVE已经实现了高性能块存储系统，支持快照克隆和恢复 ,支持QEMU虚拟机和物理机NBD设备两种挂载方式, 在网易内部作为高性能云盘使用。

## 文档

- 通过 [简介](https://opencurve.github.io/) 可以了解 curve 架构
- 通过 [编译与运行](docs/cn/build_and_run.md) 可以了解如何编译和执行单元测试
- CURVE系列文档
  - [NEBD](docs/cn/nebd.md)

## 快速开始

- 通过 [集群部署](docs/cn/deploy.md) 快速搭建 curve 集群

## 反馈和参与

- bug、疑惑、修改建议都欢迎提在[Github Issues](https://github.com/opencurve/curve/issues)中
- [FAQ](https://github.com/opencurve/curve/wiki/CURVE-FAQ)
