

<div align=center> <img src="docs/images/curve-logo1-nobg.png" width = 45%>

<div align=center> <image src="docs/images/cncf-icon-color.png" width = 8%>

**A could-native distributed storage system**

#### [English](README.md) | 简体中文
### 📄 [文档](https://github.com/opencurve/curve/tree/master/docs) || 🌐 [官网](https://www.opencurve.io/Curve/HOME) || 🏠 [论坛](https://ask.opencurve.io/t/topic/7)
<div align=left>

<div class="column" align="middle">
  </a>
  <a href="https://github.com/opencurve/curve/blob/master/LICENSE">
    <img src=https://img.shields.io/aur/license/android-studio?style=plastic alt="license"/>
  </a>
  <a href="https://github.com/opencurve/curve/releases">
   <img src="https://img.shields.io/github/v/release/opencurve/curve?include_prereleases" alt="release"/>
  </a>
  <a href="https://bestpractices.coreinfrastructure.org/projects/6136">
    <img src="https://bestpractices.coreinfrastructure.org/projects/6136/badge">
  </a>
  <a href="https://github.com/opencurve/curve/tree/master/docs">
    <img src="https://img.shields.io/badge/docs-latest-green.svg">
</div>

✨ 目录
========

  - [关于 Curve](#关于-curve)
  - [Curve 架构](#curve-架构)
  - [设计文档](#设计文档)
  - [CurveBS 快速体验](#curvebs-快速体验)
    - [部署All-in-one体验环境](#部署all-in-one体验环境)
    - [FIO curve块存储引擎](#fio-curve块存储引擎)
  - [CurveFS 快速体验](#curvefs-快速体验)
  - [测试环境配置](#测试环境配置)
  - [社区治理](#社区治理)
  - [贡献我们](#贡献我们)
  - [行为守则](#行为守则)
  - [LICENSE](#license)
  - [版本发布周期](#版本发布周期)
  - [分支规则](#分支规则)
  - [反馈及交流](#反馈及交流)


##  关于 Curve
**Curve** 是网易主导自研的现代化存储系统, 目前支持文件存储(CurveFS)和块存储(CurveBS)。现作为沙箱项目托管于[CNCF](https://www.cncf.io/)。

<details>
  <summary><b><font=5>高性能 | 更稳定 | 易运维 | 云原生</b></font></summary>

- 高性能 : CurveBS vs CephBS

  CurveBS: v1.2.0

  CephBS: L/N
  性能:
  块存储场景下，CurveBS随机读写性能远优于CephBS。
  测试环境：6台服务器*20块SATA SSD，E5-2660 v4，256G，3副本，使用nbd场景。

  单卷场景：
  <image src="docs/images/1-nbd.jpg">

  多卷场景：
  <image src="docs/images/10-nbd.jpg">

- 更稳定
  - 块存储场景下，常见异常CurveBS的稳定性优于CephBS。

    | 异常场景 | 单盘故障 | 慢盘 | 机器宕机 | 机器卡住 |
    | :----: | :----: | :----: | :----: | :----: |
    | CephBS | 抖动7s | 持续io抖动 | 抖动7s | 不可恢复 |
    | CurveBS | 抖动4s | 无影响 | 抖动4s | 抖动4s |

- 易运维
  - 我们开发了 [CurveAdm](https://github.com/opencurve/curveadm/wiki)来帮助运维人员。

    | 工具 |CephAdm | CurveAdm|
    | :--:  | :--: |:--: |
    | 一键安装  |   ✔️ |   ✔️ |
    | 一键部署  | ❌(步骤稍多) |  ✔️ |
    | playground |  ❌|   ✔️|
    | 多集群管理  |  ❌ |   ✔️ |
    | 一键扩容 | ❌(步骤稍多)|   ✔️|
    |一键升级 |   ✔️ |   ✔️|
    |一键停服 |  ❌ |   ✔️|
    |一键清理 |  ❌ |   ✔️ |
    |部署环境检测|  ❌ |   ✔️ |
    |操作审计| ❌ |   ✔️|
    |周边组件部署| ❌ |   ✔️|
    |一键日志上报| ❌ |   ✔️|
    |集群状态统计上报|  ❌|   ✔️|
    |错误码分类及解决方案|  ❌ |   ✔️|
  - 运维
    块存储场景下，CurveBS常见运维更友好。

    | 运维场景 | 客户端升级 | 均衡 |
    | :----: | :----: | :----: |
    | CephBS | 不支持热升级 | 外部插件调整，影响业务IO |
    | CurveBS | 支持热升级，秒级抖动 | 自动均衡，对业务IO无影响 |
- 云原生
  - 详见[我们对云原生的理解](https://github.com/opencurve/curve/wiki/Roadmap_CN)。

  </details>
<details>
  <summary><b><font=5>对接 OpenStack</b></font></summary>

- 详见 [Curve-cinder](https://github.com/opencurve/curve-cinder)。


</details>
<details>
  <summary><b><font=5>对接 Kubernates</b></font></summary>

- 使用 [Curve CSI Driver](https://github.com/opencurve/curve-csi) 插件在 Container Orchestrator (CO) 与 Curve 集群中实现了 Container Storage Interface(CSI)。
- 文档详见[CSI Curve Driver Doc](https://github.com/opencurve/curve-csi/blob/master/docs/README.md)。
</details>
<details>
  <summary><b><font=5>对接 PolarDB | PG </b></font></summary>

- 作为存算分离形态分布式数据库 [PolarDB | PG](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) 底层存储底座, 为上层数据库应用提供数据一致性保证, 极致弹性, 高性能HTAP。部署详见[PolarDB | PG 进阶部署(CurveBS)](https://apsaradb.github.io/PolarDB-for-PostgreSQL/zh/deploying/storage-curvebs.html)。


</details>
<details>
  <summary><b><font=5> 更多...</b></font></summary>

- Curve 亦可作为云存储中间件使用 S3 兼容的对象存储作为数据存储引擎，为公有云用户提供高性价比的共享文件存储。
</details>

## Curve 架构

<div align=center> <image src="docs/images/Curve-arch.png" width=60%>
<div align=left>

<details>
  <summary><b><font=4>Curve混合云支持</b></font></summary>

Curve支持部署在私有云和公有云环境，也可以以混合云方式使用，私有云环境下的部署架构如下：
<div align=center> <image src="docs/images/Curve-deploy-on-premises-idc.png" width=60%>
<div align=left>

其中CurveFS共享文件存储系统可以弹性伸缩到公有云存储，可以为用户提供更大的容量弹性、更低的成本、更好的性能体验。

</details>

<div align=left>

<details>
  <summary><b><font=4>Curve公有云支持</b></font></summary>

公有云环境下，用户可以部署CurveFS集群，用来替换云厂商提供的共享文件存储系统，并利用云盘进行加速，可极大的降低业务成本，其部署架构如下：
<div align=center>
 <image src="docs/images/Curve-deploy-on-public-cloud.png" width=55%>

</details>
<div align=left>


## 设计文档

- 通过 [Curve概述](https://opencurve.github.io/) 可以了解 Curve 架构。
- CurveBS相关文档
  - [NEBD](docs/cn/nebd.md)
  - [MDS](docs/cn/mds.md)
  - [Chunkserver](docs/cn/chunkserver_design.md)
  - [Snapshotcloneserver](docs/cn/snapshotcloneserver.md)
  - [Curve质量体系介绍](docs/cn/quality.md)
  - [Curve监控体系介绍](docs/cn/monitor.md)
  - [Client](docs/cn/curve-client.md)
  - [Client Python API](docs/cn/curve-client-python-api.md)
- CurveBS上层应用
  - [对接k8s文档](docs/cn/k8s_csi_interface.md)
- CurveFS相关文档
  - [架构设计](docs/cn/curvefs_architecture.md)
  - [Client概要设计](docs/cn/curvefs-client-design.md)
  - [元数据管理](docs/cn/curvefs-metaserver-overview.md)
  - [数据缓存方案](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%94%AF%E6%8C%81S3%20%E6%95%B0%E6%8D%AE%E7%BC%93%E5%AD%98%E6%96%B9%E6%A1%88.pdf)
  - [空间分配方案](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%96%87%E4%BB%B6%E7%B3%BB%E7%BB%9F%E7%A9%BA%E9%97%B4%E5%88%86%E9%85%8D%E6%96%B9%E6%A1%88.pdf)
  - [更多](https://github.com/opencurve/curve-meetup-slides/tree/main/CurveFS)
- CurveAdm相关文档
  - [Wiki](https://github.com/opencurve/curveadm/wiki)

##  CurveBS 快速体验
为了提升 Curve 的运维便利性，我们设计开发了 [CurveAdm](https://github.com/opencurve/curveadm) 项目，其主要用于部署和管理 Curve 集群，目前已支持部署CurveBS & CurveFS（扩容、版本升级等更多功能正在开发中），相关使用文档请参考 [CurveAdm用户手册](https://github.com/opencurve/curveadm/wiki)，并根据手册首先安装CurveAdm工具之后再进行Curve集群的部署。

###  部署All-in-one体验环境
请参考CurveAdm用户手册中[CurveBS集群部署步骤](https://github.com/opencurve/curveadm/wiki/curvebs-cluster-deployment)，单机体验环境请使用“集群拓扑文件-单机部署”模板。

curve 提供了命令行工具以查看集群状态和进行基本集群操作:[命令行工具说明](docs/cn/curve_ops_tool.md)
### FIO Curve块存储引擎
fio的Curve块存储引擎代码已经上传到 https://github.com/opencurve/fio ，请自行编译测试（依赖nebd库），fio命令行示例：
```bash
$ ./fio --thread --rw=randwrite --bs=4k --ioengine=nebd --nebd=cbd:pool//pfstest_test_ --iodepth=10 --runtime=120 --numjobs=10 --time_based --group_reporting --name=curve-fio-test
```

在性能测试过程中有任何问题，请查看[Curve块存储性能调优指南](docs/cn/Curve%E5%9D%97%E5%AD%98%E5%82%A8%E6%80%A7%E8%83%BD%E8%B0%83%E4%BC%98%E6%8C%87%E5%8D%97.md)

##  CurveFS 快速体验
请使用 [CurveAdm](https://github.com/opencurve/curveadm/wiki) 工具进行 CurveFS 的部署，具体流程见：[CurveFS部署流程](https://github.com/opencurve/curveadm/wiki/curvefs-cluster-deployment), 以及[CurveFS命令行工具说明](curvefs/src/tools#readme)。

## 测试环境配置

请参考 [测试环境配置](docs/cn/测试环境配置信息.md)
##  社区治理
请参考[社区治理](https://github.com/opencurve/community/blob/master/GOVERNANCE.md)。

##  贡献我们

参与 Curve 项目开发详见[Curve 开源社区指南](Community_Guidelines_cn.md)并且请遵循[贡献者准则](https://github.com/opencurve/curve/blob/master/CODE_OF_CONDUCT.md), 我们期待您的贡献!

## 最佳实践
- [CurveBS+NFS搭建NFS存储](docs/practical/curvebs_nfs.md)

##  行为守则
Curve 的行为守则遵循[CNCF Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md)。

## LICENSE
Curve 在 [Apache 2.0](LICENSE) 协议下进行分发。

##  版本发布周期
- CURVE版本发布周期：大版本半年，小版本1~2个月
- 版本号规则：采用3段式版本号，x.y.z{-后缀}，x是大版本，y是小版本，z是bugfix，后缀用来区beta版本(-beta)、rc版本(-rc)、和稳定版本(没有后缀)。每半年的大版本是指x增加1，每1~2个月的小版本是y增加1。正式版本发布之后，如果有bugfix是z增加1。

##  分支规则
所有的开发都在master分支开发，如果需要发布版本，从master拉取新的分支**release-x.y**。版本发布从release-x.y分支发布。

##  反馈及交流

- [Github Issues](https://github.com/openCURVE/CURVE/issues)：欢迎提交BUG、建议，使用中如遇到问题可参考FAQ或加入我们的User group进行咨询。
- [FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)：主要根据User group中常见问题整理，还在逐步完善中。
- User group：当前为微信群，由于群人数过多，需要先添加以下个人微信，再邀请进群。

<img src="docs/images/curve-wechat.jpeg" style="zoom: 75%;" />





