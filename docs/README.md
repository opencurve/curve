
<img src="images/curve-logo1.png"/>

## 关于Curve

Curve是一款高性能、易运维、云原生的开源分布式存储系统。可应用于主流的云原生基础设施平台：对接 OpenStack 平台为云主机提供高性能块存储服务；对接 Kubernetes 为其提供 RWO、RWX 等类型的持久化存储卷；对接 PolarFS 作为云原生数据库的高性能存储底座，完美支持云原生数据库的存算分离架构。Curve 亦可作为云存储中间件使用 S3 兼容的对象存储作为数据存储引擎，为公有云用户提供高性价比的共享文件存储。

Curve已加入云原生计算基金会（CNCF）作为沙箱项目托管。

Curve架构概览：

<image src="images/Curve-arch.png" width=70%>


## 快速上手

为了提升 Curve 的运维便利性，我们设计开发了 [CurveAdm](https://github.com/opencurve/curveadm) 项目，其主要用于部署和管理 Curve 集群，目前已支持部署CurveBS & CurveFS，相关使用文档请参考 [CurveAdm用户手册](https://github.com/opencurve/curveadm/wiki)，并根据手册首先安装CurveAdm工具之后再进行Curve集群的部署。


## 运维手册


### 运维工具手册
[运维工具手册](https://github.com/opencurve/curveadm/wiki)

### 命令行手册
- [Curve命令行工具手册](../tools-v2#README)
  
### 监控告警配置
- [CurveBS监控部署](https://github.com/opencurve/curve/tree/master/monitor)
- [CurveFS监控部署](https://github.com/opencurve/curve/tree/master/curvefs/monitor)

### 常见问题处理
[常见问题处理](https://github.com/opencurve/curveadm/wiki/maintain-curve)



## 生态对接

- Kubernetes：[CurveBS CSI](https://github.com/opencurve/curve-csi)、CurveFS CSI近期发布
- OpenStack：[OpenStack Curve Drivers](https://github.com/opencurve/curve-cinder)
- Libvirt/QEMU：[Curve patches for Libvirt/QEMU](https://github.com/opencurve/curve-qemu-block-driver)
- CurveBS卷通过NBD挂载到物理机：[curve-nbd](https://github.com/opencurve/curveadm/wiki/curvebs-client-deployment)
- CurveFS文件系统通过FUSE挂载到物理机：[curve-fuse](https://github.com/opencurve/curveadm/wiki/curvefs-client-deployment)
- PolarFS云原生数据库文件系统：[基于Curve后端的PolarFS版本](https://github.com/opencurve/PolarDB-FileSystem/blob/curvebs_sdk_devio/README-curve.md)
- iSCSI：[Curve iSCSI Target](https://github.com/opencurve/curve-tgt)、[部署Curve target](https://github.com/opencurve/curveadm/wiki/others#%E9%83%A8%E7%BD%B2-tgt)
- FIO性能测试工具：[基于CurveBS SDK的FIO工具](https://github.com/opencurve/fio)、CurveAdm即将集成fio for Curve


## 架构介绍

### 架构概览
Curve支持部署在私有云和公有云环境，也可以以混合云方式使用，私有云环境下的部署架构如下：
<image src="images/Curve-deploy-on-premises-idc.png" width=60%>

其中CurveFS共享文件存储系统可以弹性伸缩到公有云存储，可以为用户提供更大的容量弹性、更低的成本、更好的性能体验。


公有云环境下，用户可以部署CurveFS集群，用来替换云厂商提供的共享文件存储系统，并利用云盘进行加速，可极大的降低业务成本，其部署架构如下：

<image src="images/Curve-deploy-on-public-cloud.png" width=55%>

### 设计文档
- 通过 [Curve概述](https://opencurve.github.io/) 可以了解 Curve 架构
- CurveBS相关文档
  - [NEBD](cn/nebd.md)
  - [MDS](cn/mds.md)
  - [Chunkserver](cn/chunkserver_design.md)
  - [Snapshotcloneserver](cn/snapshotcloneserver.md)
  - [Curve质量体系介绍](cn/quality.md)
  - [Curve监控体系介绍](cn/monitor.md)
  - [Client](cn/curve-client.md)
  - [Client Python API](cn/curve-client-python-api.md)
- CurveFS相关文档
  - [架构设计](cn/curvefs_architecture.md)
  - [Client概要设计](cn/curvefs-client-design.md)
  - [元数据管理](cn/curvefs-metaserver-overview.md)
  - [数据缓存方案](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%94%AF%E6%8C%81S3%20%E6%95%B0%E6%8D%AE%E7%BC%93%E5%AD%98%E6%96%B9%E6%A1%88.pdf)
  - [空间分配方案](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%96%87%E4%BB%B6%E7%B3%BB%E7%BB%9F%E7%A9%BA%E9%97%B4%E5%88%86%E9%85%8D%E6%96%B9%E6%A1%88.pdf)
- 更多文档可查阅子目录


### 源码解读
[Curve源码及核心流程深度解读](https://github.com/opencurve/curve/wiki/Curve%E6%BA%90%E7%A0%81%E5%8F%8A%E6%A0%B8%E5%BF%83%E6%B5%81%E7%A8%8B%E6%B7%B1%E5%BA%A6%E8%A7%A3%E8%AF%BB)

## 版本相关
- CURVE版本发布周期：大版本半年，小版本1~2个月
- 版本号规则：采用3段式版本号，x.y.z{-后缀}，x是大版本，y是小版本，z是bugfix，后缀用来区beta版本(-beta)、rc版本(-rc)、和稳定版本(没有后缀)。每半年的大版本是指x增加1，每1~2个月的小版本是y增加1。正式版本发布之后，如果有bugfix是z增加1。

### 分支管理
所有的开发需要首先merge到master分支，如果需要发布版本，从master拉取新的分支**release-x.y**。版本发布从release-x.y分支发布。

## 功能清单

[功能清单](https://github.com/opencurve/curve/wiki/Feature-list)

## 性能指标
块存储场景下，Curve随机读写性能远优于Ceph。
测试环境：6台服务器*20块SATA SSD，E5-2660 v4，256G，3副本，使用nbd场景。

单卷场景：
<image src="images/1-nbd.jpg">

多卷场景：
<image src="images/10-nbd.jpg">



## 参与贡献

如何参与 Curve 项目开发详见[Curve 开源社区指南](../Community_Guidelines_cn.md)

### 部署编译开发环境

[编译开发环境搭建](cn/build_and_run.md)

### 测试用例编译及运行
[测试用例编译及运行](cn/build_and_run.md#%E6%B5%8B%E8%AF%95%E7%94%A8%E4%BE%8B%E7%BC%96%E8%AF%91%E5%8F%8A%E6%89%A7%E8%A1%8C)

### 最佳实践
- [CurveBS+NFS搭建共享存储](practical/curvebs_nfs.md)

## 联系社区
- [Github Issues](https://github.com/openCURVE/CURVE/issues)：欢迎提交BUG、建议，使用中如遇到问题可参考FAQ或加入我们的User group进行咨询
- [FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)：主要根据User group中常见问题整理，还在逐步完善中
- 论坛：[https://ask.opencurve.io/](https://ask.opencurve.io/)
- 微信公众号：OpenCurve
- slack：workspace cloud-native.slack.com，channel #project_curve
- User group：当前为微信群，由于群人数过多，需要先添加以下个人微信，再邀请进群。
<img src="images/curve-wechat.jpeg" style="zoom: 75%;" />
