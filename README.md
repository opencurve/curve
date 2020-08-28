# CURVE

[![Jenkins Coverage](https://img.shields.io/jenkins/coverage/cobertura?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fcurve_untest_job%2F)](http://59.111.93.165:8080/job/curve_untest_job/HTML_20Report/)
[![Robot failover](https://img.shields.io/jenkins/build?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fcurve_failover_testjob%2F&label=failover)](http://59.111.93.165:8080/job/curve_failover_testjob/)
[![Robot interface](https://img.shields.io/jenkins/tests?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fcurve_robot_job%2F)](http://59.111.93.165:8080/job/curve_robot_job/)
[![BUILD Status](https://img.shields.io/jenkins/build?jobUrl=http%3A%2F%2F59.111.93.165%3A8080%2Fjob%2Fopencurve_multijob%2F)](http://59.111.93.165:8080/job/opencurve_multijob/lastBuild)
[![Docs](https://img.shields.io/badge/docs-latest-green.svg)](https://github.com/opencurve/curve/tree/master/docs)
[![Releases](https://img.shields.io/github/v/release/opencurve/curve?include_prereleases)](https://github.com/opencurve/curve/releases)
[![LICENSE](https://img.shields.io/badge/licence-Apache--2.0%2FGPL-blue)](https://github.com/opencurve/curve/blob/master/LICENSE)


CURVE是网易自主设计研发的高性能、高可用、高可靠分布式存储系统，具有非常良好的扩展性。基于该存储底座可以打造适用于不同应用场景的存储系统，如块存储、对象存储、云原生数据库等。当前我们基于CURVE已经实现了高性能块存储系统，支持快照克隆和恢复 ,支持QEMU虚拟机和物理机NBD设备两种挂载方式, 在网易内部作为高性能云盘使用。

## 设计文档

- 通过 [CURVE概述](https://opencurve.github.io/) 可以了解 CURVE 架构
- CURVE相关文档
  - [NEBD](docs/cn/nebd.md)
  - [MDS](docs/cn/mds.md)
  - [Chunkserver](docs/cn/chunkserver_design.md)
  - [Snapshotcloneserver](docs/cn/snapshotcloneserver.md)
  - [CURVE质量体系介绍](docs/cn/quality.md)
  - [CURVE监控体系介绍](docs/cn/monitor.md)
  - [Client](docs/cn/curve-client.md)
  - [Client Python API](docs/cn/curve-client-python-api.md)
- CURVE上层应用
  - [对接k8s文档](docs/cn/k8s_csi_interface.md)

## 快速开始

在您开始动手部署前请先仔细阅读特别说明部分：[特别说明](docs/cn/deploy.md#%E7%89%B9%E5%88%AB%E8%AF%B4%E6%98%8E)

### 部署All-in-one体验环境

[单机部署](docs/cn/deploy.md#%E5%8D%95%E6%9C%BA%E9%83%A8%E7%BD%B2)

### 部署多机集群

[多机部署](docs/cn/deploy.md#%E5%A4%9A%E6%9C%BA%E9%83%A8%E7%BD%B2)

### 查询工具说明

[查询工具说明](docs/cn/curve_ops_tool.md)

## 参与开发


### 部署编译开发环境

[编译开发环境搭建](docs/cn/build_and_run.md)

### 测试用例编译及运行
[测试用例编译及运行](docs/cn/build_and_run.md#%E6%B5%8B%E8%AF%95%E7%94%A8%E4%BE%8B%E7%BC%96%E8%AF%91%E5%8F%8A%E6%89%A7%E8%A1%8C)

### 编码规范
CURVE编码规范严格按照[Google C++开源项目编码指南](https://zh-google-styleguide.readthedocs.io/en/latest/google-cpp-styleguide/contents/)来进行代码编写，请您也遵循这一指南来提交您的代码。

### 测试覆盖率要求
1. 单元测试：增量行覆盖80%以上，增量分支覆盖70%以上
2. 集成测试：与单元测试合并统计，满足上述覆盖率要求即可
3. 异常测试：暂不做要求

### 其他开发流程说明
代码开发完成之后，提[pr](https://github.com/opencurve/curve/compare)到curve的master分支。提交pr时，请填写pr模板。pr提交之后会自动触发CI，CI通过并且经过review之后，代码才可合入。
具体规则请见[CONTRIBUTING](https://github.com/opencurve/curve/blob/master/CONTRIBUTING.md).

## 版本发布周期
- CURVE版本发布周期：大版本半年，小版本1~2个月
- 版本号规则：采用3段式版本号，x.y.z{-后缀}，x是大版本，y是小版本，z是bugfix，后缀用来区beta版本(-beta)、rc版本(-rc)、和稳定版本(没有后缀)。每半年的大版本是指x增加1，每1~2个月的小版本是y增加1。正式版本发布之后，如果有bugfix是z增加1。

## 分支规则
所有的开发都在master分支开发，如果需要发布版本，从master拉取新的分支**release-x.y**。版本发布从release-x.y分支发布。

## 反馈及交流

- [Github Issues](https://github.com/openCURVE/CURVE/issues)：欢迎提交BUG、建议，使用中如遇到问题可参考FAQ或加入我们的User group进行咨询
- [FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)：主要根据User group中常见问题整理，还在逐步完善中
- User group：当前为微信群，由于群人数过多，需要先添加以下个人微信，再邀请进群。

<img src="https://raw.githubusercontent.com/opencurve/opencurve.github.io/master/image/curve-wechat.jpeg" style="zoom: 75%;" />






