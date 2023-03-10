

<div align=center> <img src="docs/images/curve-logo1-nobg.png" width = 45%>

<div align=center> <image src="docs/images/cncf-icon-color.png" width = 8%>

**A cloud-native distributed storage system**

#### English | [简体中文](README_cn.md)
### 📄 [Documents](https://github.com/opencurve/curve/tree/master/docs) || 🌐 [Official Website](https://www.opencurve.io/Curve/HOME) || 🏠 [Forum](https://ask.opencurve.io/t/topic/7)
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

✨ Contents
========

  - [About Curve](#about-curve)
  - [Curve Architecture](#curve-architecture)
  - [Design Documentation](#design-documentation)
  - [CurveBS quick start](#curvebs-quick-start)
    - [Deploy an All-in-one experience environment](#deploy-an-all-in-one-experience-environment)
    - [FIO curve block storage engine](#fio-curve-block-storage-engine)
  - [CurveFS quick start](#curvefs-quick-start)
  - [Test environment configuration](#test-environment-configuration)
  - [Governance](#governance)
  - [Contribute us](#contribute-us)
  - [Code of Conduct](#code-of-conduct)
  - [LICENSE](#license)
  - [Release Cycle](#release-cycle)
  - [Branch](#branch)
  - [Feedback & Contact](#feedback--contact)

##  About Curve

**Curve** is a modern storage system developed by netease, currently supporting file storage(CurveFS) and block storage(CurveBS). Now it's hosted at [CNCF](https://www.cncf.io/) as a sandbox project.

The core application scenarios of CurveBS mainly include:
- the performance, mixed, capacity cloud disk or persistent volume of virtual machine/container, and remote disks of physical machines
- high-performance separation of storage and computation architecture: high-performance and low latency architecture based on RDMA+SPDK, supporting the separation deployment structure of various databases such as MySQL and Kafka


The core application scenarios of CurveFS mainly include:
- the cost-effective storage in AI training scene
- the hot and cold data automation layered storage in big data scenarios
- the cost-effective shared file storage on the public cloud: It can be used for business scenarios such as AI, big data, file sharing
- Hybrid storage: Hot data is stored in the local IDC, cold data is stored in public cloud

<details>
  <summary><b><font=5>High Performance | More stable | Easy Operation | Cloud Native</b></font></summary>

- High Performance : CurveBS vs CephBS

  CurveBS: v1.2.0

  CephBS: L/N
  Performance:
  CurveBS random read and write performance far exceeds CephBS in the block storage scenario.

  Environment：3 replicas on a 6-node cluster, each node has 20xSATA SSD, 2xE5-2660 v4 and 256GB memory.

  Single Vol：
  <image src="docs/images/1-nbd-en.png">

  Multi Vols：
  <image src="docs/images/10-nbd-en.png">

- More stable
  - The stability of the common abnormal Curve is better than that of Ceph in the block storage scenario.
    | Fault Case | One Disk Failure | Slow Disk Detect | One Server Failure | Server Suspend Animation |
    | :----: | :----: | :----: | :----: | :----: |
    | CephBS | jitter 7s | Continuous io jitter | jitter 7s | unrecoverable |
    | CurveBS | jitter 4s | no effect | jitter 4s | jitter 4s |

- Easy Operation
  - We have developed [CurveAdm](https://github.com/opencurve/curveadm/wiki) to help O&M staff.
    | tools  |CephAdm | CurveAdm|
    | :--: | :--: |:--: |
    | easy Installation |  ✔️ |   ✔️ |
    | easy Deployment| ❌(slightly more steps) |  ✔️ |
    | playground | ❌|   ✔️|
    | Multi-Cluster Management | ❌ |   ✔️ |
    | easy Expansion | ❌(slightly more steps)|   ✔️|
    |easy Upgrade |  ✔️ |   ✔️|
    |easy to stop service | ❌ |   ✔️|
    |easy Cleaning | ❌ |   ✔️ |
    |Deployment environment testing|  ❌ |  ✔️ |
    |Operational audit|  ❌ |   ✔️|
    |Peripheral component deployment| ❌ |   ✔️|
    |easy log reporting| ❌ |   ✔️|
    |Cluster status statistics reporting| ❌|   ✔️|
    |Error code classification and solutions|  ❌ |   ✔️|
  - Ops
    CurveBS ops is more friendly than CephBS in the block storage scenario.
    | Ops scenarios | Upgrade clients | Balance |
    | :----: | :----: | :----: |
    | CephBS | do not support live upgrade | via plug-in with IO influence |
    | CurveBS | support live upgrade with second jitter | auto with no influence on IO |

- Cloud Native
  - Please see [Our understanding of cloud native](https://github.com/opencurve/curve/wiki/Roadmap).

  </details>
<details>
  <summary><b><font=5>Docking OpenStack</b></font></summary>

- Please see [Curve-cinder](https://github.com/opencurve/curve-cinder).


</details>
<details>
  <summary><b><font=5>Docking Kubernates</b></font></summary>

- Use [Curve CSI Driver](https://github.com/opencurve/curve-csi), The plugin implements the Container Storage Interface(CSI) between Container Orchestrator(CO) and Curve cluster. It allows dynamically provisioning curve volumes and attaching them to workloads.
- For details of the documentation, see [CSI Curve Driver Doc](https://github.com/opencurve/curve-csi/blob/master/docs/README.md).
</details>
<details>
  <summary><b><font=5>Docking PolarDB | PG </b></font></summary>

- It serves as the underlying storage base for [polardb for postgresql](https://github.com/ApsaraDB/PolarDB-for-PostgreSQL) in the form of storage and computation separation, providing data consistency assurance for upper layer database applications, extreme elasticity scaling, and high performance HTAP.

- Deployment details can be found at [PolarDB | PG Advanced Deployment(CurveBS)](https://apsaradb.github.io/PolarDB-for-PostgreSQL/zh/deploying/storage-curvebs.html).


</details>
<details>
  <summary><b><font=5> More...</b></font></summary>

- Curve can also be used as cloud storage middleware using S3-compatible object storage as the data storage engine, providing cost-effective shared file storage for public cloud users.
</details>

## Curve Architecture

<div align=center> <image src="docs/images/Curve-arch.png" width=60%>
<div align=left>

<details>
  <summary><b><font=4>Curve on Hybrid Cloud</b></font></summary>

Curve supports deployment in private and public cloud environments, and can also be used in a hybrid cloud:
<div align=center> <image src="docs/images/Curve-deploy-on-premises-idc.png" width=60%>
<div align=left>

One of them, CurveFS shared file storage system, can be elasticly scaled to public cloud storage, which can provide users with greater capacity elasticity, lower cost, and better performance experience.

</details>

<div align=left>

<details>
  <summary><b><font=4>Curve on Public Cloud</b></font></summary>

In a public cloud environment, users can deploy CurveFS clusters to replace the shared file storage system provided by cloud vendors and use cloud disks for acceleration, which can greatly reduce business costs, with the following deployment architecture:
<div align=center>
 <image src="docs/images/Curve-deploy-on-public-cloud.png" width=55%>

</details>
<div align=left>

##  Design Documentation

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
- CurveFS documentations
  - [Architecture design](docs/cn/curvefs_architecture.md)
  - [Client design](docs/cn/curvefs-client-design.md)
  - [Metadata management](docs/cn/curvefs-metaserver-overview.md)
  - [Data caching](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%94%AF%E6%8C%81S3%20%E6%95%B0%E6%8D%AE%E7%BC%93%E5%AD%98%E6%96%B9%E6%A1%88.pdf)
  - [Space allocation](https://github.com/opencurve/curve-meetup-slides/blob/main/CurveFS/Curve%E6%96%87%E4%BB%B6%E7%B3%BB%E7%BB%9F%E7%A9%BA%E9%97%B4%E5%88%86%E9%85%8D%E6%96%B9%E6%A1%88.pdf)
  - [more details](https://github.com/opencurve/curve-meetup-slides/tree/main/CurveFS)

- CurveAdm
  - [CurveAdm Doc](https://github.com/opencurve/curveadm/wiki)

##  CurveBS quick start
In order to improve the operation and maintenance convenience of Curve, we designed and developed the [CurveAdm](https://github.com/opencurve/curveadm) project, which is mainly used for deploying and managing Curve clusters. Currently, it supports the deployment of CurveBS & CurveFS (scaleout, upgrade and other functions are under development), please refer to the [CurveAdm User Manual](https://github.com/opencurve/curveadm/wiki) for related documentation, and install the CurveAdm tool according to the manual before deploying the Curve cluster.


###  Deploy an All-in-one experience environment

Please refer to the [CurveBS cluster deployment steps](https://github.com/opencurve/curveadm/wiki/curvebs-cluster-deployment) in the CurveAdm user manual. For standalone experience, please use the "Cluster Topology File - Standalone Deployment" template.

[The command tools' instructions](docs/cn/curve_ops_tool.md)
### FIO Curve block storage engine
Fio Curve engine is added, you can clone https://github.com/opencurve/fio and compile the fio tool with our engine(depend on nebd lib), fio command line example:
```bash
$ ./fio --thread --rw=randwrite --bs=4k --ioengine=nebd --nebd=cbd:pool//pfstest_test_ --iodepth=10 --runtime=120 --numjobs=10 --time_based --group_reporting --name=curve-fio-test
```

If you have any questions during performance testing, please check the [Curve block storage performance tuning guide](docs/cn/Curve%E5%9D%97%E5%AD%98%E5%82%A8%E6%80%A7%E8%83%BD%E8%B0%83%E4%BC%98%E6%8C%87%E5%8D%97.md).

##  CurveFS quick start
Please use [CurveAdm](https://github.com/opencurve/curveadm/wiki) tool to deploy CurveFS，see [CurveFS Deployment Process](https://github.com/opencurve/curveadm/wiki/curvefs-cluster-deployment), and the [CurveFS Command Instructions](curvefs/src/tools#readme).

## Test environment configuration

Please refer to the [Test environment configuration](docs/cn/测试环境配置信息.md)

## Practical
- [CurveBS+NFS Build NFS Server](docs/practical/curvebs_nfs.md)

## Governance
See [Governance](https://github.com/opencurve/community/blob/master/GOVERNANCE.md).

##  Contribute us
Participation in the Curve project is described in the [Curve Developers Guidelines](developers_guide.md) and is subject to a [contributor contract](https://github.com/opencurve/curve/blob/master/CODE_OF_CONDUCT.md).
We welcome your contribution!

## Code of Conduct
Curve follows the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## LICENSE
Curve is distributed under the [**Apache 2.0 LICENSE**](LICENSE).

##  Release Cycle
- CURVE release cycle：Half a year for major version, 1~2 months for minor version

- Versioning format: We use a sequence of three digits and a suffix (x.y.z{-suffix}), x is the major version, y is the minor version, and z is for bugfix. The suffix is for distinguishing beta (-beta), RC (-rc) and GA version (without any suffix). Major version x will increase 1 every half year, and y will increase every 1~2 months. After a version is released, number z will increase if there's any bugfix.

## Branch
All the developments will be done under master branch. If there's any new version to establish, a new branch release-x.y will be pulled from the master, and the new version will be released from this branch.

## Feedback & Contact
- [Github Issues](https://github.com/openCURVE/CURVE/issues)：You are sincerely welcomed to issue any bugs you came across or any suggestions through Github issues. If you have any question you can refer to our FAQ or join our user group for more details.
- [FAQ](https://github.com/openCURVE/CURVE/wiki/CURVE-FAQ)：Frequently asked question in our user group, and we'll keep working on it.
- User group：We use Wechat group currently.
- [Double Week Meetings](https://github.com/opencurve/curve-meetup-slides/tree/main/2022): We have an online community meeting every two weeks which talk about what Curve is doing and planning to do. The time and links of the meeting are public in the user group and [Double Week Meetings](https://github.com/opencurve/curve-meetup-slides/tree/main/2022).


<img src="docs/images/curve-wechat.jpeg" style="zoom: 65%;" />
