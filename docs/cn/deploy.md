<head><meta charset="UTF-8"></head>

## 概述

ansible是一款自动化运维工具，curve-ansible 是基于 ansible playbook 功能编写的集群部署工具。本文档介绍如何快速上手体验 CURVE 分布式系统：1. 使用 curve-ansible 在单机上模拟生产环境部署步骤  2. 使用 curve-ansible 在多机上部署最小生产环境。关于curve-ansible的更多用法和说明请参见[curve-ansible README](../../curve-ansible/README.md)。安装过程中遇到文档中未说明的问题，请先使用最新版本的软件包，老版本ansible中存在的问题以及解决方法不会在本文档中出现。

## 特别说明
- 一些外部依赖是通过源码的方式安装的，安装的过程中从github下载包可能会超时，这时可以选择重试或手动安装，jemalloc手动安装的话要保证configure的prefix与server.ini和client.ini的lib_install_prefix一致
- 如果机器上开启了SElinux可能会报Aborting, target uses selinux but python bindings (libselinux-python) aren't installed，可以尝试安装libselinux-python，或者强行关闭selinux
- deploy_curve.yml用于部署一个全新的集群，集群成功搭建后不能重复跑，因为会**扰乱集群**。可以选择**启动**集群或者**清理**集群后重新部署，详细用法见[curve-ansible README](../../curve-ansible/README.md)。
- 部署的过程中，在chunkserver成功启动之前都可以任意重试，**chunkserver启动成功后重试**要额外注意，要带上--skip-tags format,因为这一步会把启动成功的chunkserver的数据给清理掉，从而扰乱集群。
- 需要用到curve-nbd功能的话，对内核有两方面的要求：一是要支持nbd模块，可以modprobe nbd查看nbd模块是否存在。二是nbd设备的block size要能够被设置为4KB。经验证，通过[DVD1.iso](http://mirrors.163.com/centos/8/isos/x86_64/CentOS-8.2.2004-x86_64-dvd1.iso)完整安装的CentOs8，内核版本是4.18.0-193.el8.x86_64，满足这个条件，可供参考。


## 单机部署

- 适用场景：希望用单台 Linux 服务器，体验 CURVE 最小的拓扑的集群，并模拟生产部署步骤。
我们提供了all-in-one的docker镜像，在这个docker镜像中，有全部的编译依赖和运行依赖。因此可以快速的部署自己编译好的代码，当然也可以选择从github下载最新的release包实施部署。
#### docker准备
执行下面的命令启动docker
   ```bash
   docker run --cap-add=ALL -v /dev:/dev -v /lib/modules:/lib/modules --privileged -it opencurve/curveintegration:centos8 /bin/bash
   ```
选择docker部署的话，下面的准备环境步骤可以直接跳过，开始实施部署即可。
#### 准备环境

准备一台部署主机，确保其软件满足需求:

- 推荐安装 Debian9 或者 Centos7/8（其他环境未经测试）
  - Linux 操作系统开放外网访问，用于下载 CURVE 的安装包
  - 部署需要创建一个有root权限的公共用户
  - 目前仅支持在 x86_64 (AMD64) 架构上部署 CURVE 集群
  - 安装 ansible 2.5.9，用于部署集群（**目前仅支持ansible 2.5.9版本，其他版本会有语法问题**）
  - 安装 docker 18.09 及以上， 用于部署快照克隆服务器

最小规模的 CURVE 集群拓扑：

| 实例        | 个数 | IP        | 配置                       |
| ----------- | ---- | --------- | -------------------------- |
| MDS         | 1    | 127.0.0.1 | 默认端口<br />全局目录配置 |
| Chunkserver | 3    | 127.0.0.1 | 默认端口<br />全局目录配置 |


##### CentOs7/8环境准备具体步骤
1. root用户登录机器，创建curve用户：

   ```bash
   $ adduser curve
   ```

2. 在root下设置curve用户免密sudo

   ```bash
   $ su  # 进入root用户
   $ 在/etc/sudoers.d下面创建一个新文件curve，里面添加一行：curve ALL=(ALL) NOPASSWD:ALL
   $ su curve  # 切换到curve用户
   $ sudo ls  # 测试sudo是否正确配置
   ```
3. 安装ansible 2.5.9
   ```bash
   $ sudo yum install python2  # 安装python2
   $ ln -s /usr/bin/python2 /usr/bin/python  # 设置默认使用python2
   $ pip2 install ansible==2.5.9  # 安装ansible
   $ ansible-playbook  # 如果没有报错的话说明安装成功，报错的话执行下面两步
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools
   ```

##### Debian9环境准备具体步骤
1. root用户登录机器，创建curve用户
   ```bash
   $ adduser curve
   ```
2. 设置curve用户免密sudo
   ```bash
   $ su  # 进入root用户
   $ apt install sudo  # 安装sudo，如果没有安装过的话
   $ 在/etc/sudoers.d下面创建一个新文件curve，里面添加一行：curve ALL=(ALL) NOPASSWD:ALL
   $ sudo -iu curve  # 切换到curve用户
   $ sudo ls  # 测试sudo是否正确配置
   ```
3. 安装ansible 2.5.9
   ```bash
   $ apt install python
   $ apt install python-pip
   $ pip install ansible==2.5.9
   $ ansible-playbook  # 如果没有报错的话说明安装成功，报错的话执行下面两步
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools
   ```

#### 实施部署

1. 切换到curve用户下执行以下操作

2. 获取tar包并解压

   有两种方式可以获得tar包：
      1. 从[github release页面](https://github.com/opencurve/curve/releases)下载稳定版本tar包
      2. 自行通过编译环境打tar包，该方式可以让您体验测试最新代码：[编译开发环境搭建](build_and_run.md)

   ```
   # 如下几个tar包可替换为其他版本（如您采用方式2自行打包，则不需要下载，拷贝相关tar包即可），下载命令仅供参考
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/curve_0.1.1+4b930380.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/nbd_0.1.1+4b930380.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/nebd_0.1.1+4b930380.tar.gz
   tar zxvf curve_0.1.1+4b930380.tar.gz
   tar zxvf nbd_0.1.1+4b930380.tar.gz
   tar zxvf nebd_0.1.1+4b930380.tar.gz
   cd curve/curve-ansible
   ```
3. 部署集群并启动服务

   ```
   ansible-playbook -i server.ini deploy_curve.yml
   ```

4. 如果需要使用快照克隆功能，需要有S3账号，可以使用[网易云的对象存储](https://www.163yun.com/product/nos)

   ```
   1. 在 server.ini 中，填写s3_nos_address，s3_snapshot_bucket_name，s3_ak和s3_sk。设置disable_snapshot_clone=false
   2. 安装快照克隆服务
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone_nginx
   ```

5. 执行命令查看当前集群状态，主要看以下几个状态：
   - Cluster status中的total copysets为100，unhealthy copysets为0
   - Mds status中current MDS不为空，offline mds list为空
   - Etcd status中的current etcd不为空，offline etcd list为空
   - Chunkserver status中的offline为0

   ```
   curve_ops_tool status
   ```

6. 安装 Nebd 服务和 NBD 包

   ```
   ansible-playbook -i client.ini deploy_nebd.yml
   ansible-playbook -i client.ini deploy_nbd.yml
   ansible-playbook -i client.ini deploy_curve_sdk.yml
   ```

8. 创建 CURVE 卷，并通过 NBD 挂载到本地。创建CURVE卷的时候可能会报Fail to listen，这个属于日志打印问题，不影响结果，可以忽略

   ```
   1. 创建 CURVE 卷： 命令为 curve create [-h] --filename FILENAME --length LENGTH --user USER， LENGTH >= 10。其中length单位为1GB。
      curve create --filename /test --length 10 --user curve
   2. 挂载卷
      sudo curve-nbd map cbd:pool//test_curve_
   3. 查看设备挂载情况（在docker部署的情况下，list-mapped会看不到，可以选择lsblk看一下是否有/dev/nbd0类型的卷）
      curve-nbd list-mapped
   ```



## 多机部署

- 适用场景：用多台 Linux 服务器，搭建 CURVE 最小的拓扑的集群，可以用于初步性能测试。

#### 准备环境

准备三台部署主机，确保其软件满足需求:

- 推荐安装 Debian9 或者 Centos7/8
- Linux 操作系统开放外网访问，用于下载 CURVE 的安装包
- 部署需要在每个机器上创建一个有root权限的公共用户
- 部署主机需要开放 CURVE 集群节点间所需ssh端口
- 目前仅支持在 x86_64 (AMD64) 架构上部署 CURVE 集群
- 选择三台机器中的一个作为中控机，安装 ansible 2.5.9，用于部署集群（**目前只支持ansible 2.5.9下的部署**）
- 安装 docker 18.09 及以上， 用于部署快照克隆服务器

同时，确保每个机器都至少一个数据盘可以用于格式化供chunkserver使用。

 CURVE 集群拓扑：

| 实例                                                         | 个数   | IP                                               | 端口 |
| ------------------------------------------------------------ | ------ | ------------------------------------------------ | ---- |
| MDS                                                          | 3      | 10.192.100.1<br />10.192.100.2<br />10.192.100.3 | 6666 |
| Chunkserver<br />(三个Server上分别挂10个盘，每个Server上启动10个Chunkserver用例) | 10 * 3 | 10.192.100.1<br />10.192.100.2<br />10.192.100.3 | 8200 |

##### CentOs7/8环境准备具体步骤
下面这些步骤要三台机器都操作：
1. root用户登录机器，创建curve用户：

   ```bash
   $ adduser curve
   ```

2. 在root下设置curve用户免密sudo

   ```bash
   $ su  # 进入root用户
   $ 在/etc/sudoers.d下面创建一个新文件curve，里面添加一行：curve ALL=(ALL) NOPASSWD:ALL
   $ su curve  # 切换到curve用户
   $ sudo ls  # 测试sudo是否正确配置
   ```
3. 检查其他依赖，未安装的需要手动安装：net-tools, openssl-1.1.1, perf, perl-podlators, make, gcc6.1, libstdc++.so.6.22

下面的步骤只需要在中控机上执行：
1. curve用户下配置ssh登陆到所有机器（包括自己），假设三台机器的ip分别为10.192.100.1,10.192.100.2,10.192.100.3
   ```bash
   $ su curve  # 切换到curve用户
   $ ssh-keygen  # 生成ssh秘钥
   $ ssh-copy-id curve@10.192.100.1  # 拷贝key到第一个机器
   $ ssh-copy-id curve@10.192.100.2  # 拷贝key到第二个机器
   $ ssh-copy-id curve@10.192.100.3  # 拷贝key到第三个机器
   $ ssh 10.192.100.1   # 挨个验证一下配置是否正确
   ```
2. 安装ansible 2.5.9
   ```bash
   $ sudo yum install python2  # 安装python2
   $ ln -s /usr/bin/python2 /usr/bin/python  # 设置默认使用python2
   $ pip2 install ansible==2.5.9  # 安装ansible
   $ ansible-playbook  # 如果没有报错的话说明安装成功，报错的话执行下面两步
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools

#### Debian9环境准备步骤
下面这些步骤要三台机器都操作：
1. root用户登录机器，创建curve用户
   ```bash
   $ adduser curve
   ```
2. 设置curve用户免密sudo
   ```bash
   $ su  # 进入root用户
   $ apt install sudo  # 安装sudo，如果没有安装过的话
   $ 在/etc/sudoers.d下面创建一个新文件curve，里面添加一行：curve ALL=(ALL) NOPASSWD:ALL
   $ sudo -iu curve  # 切换到curve用户
   $ sudo ls  # 测试sudo是否正确配置
   ```
下面的步骤只需要在中控机上执行：
1. curve用户下配置ssh登陆到所有机器（包括自己），假设三台机器的ip分别为10.192.100.1,10.192.100.2,10.192.100.3
   ```bash
   $ su curve  # 切换到curve用户
   $ ssh-keygen  # 生成ssh秘钥
   $ ssh-copy-id curve@10.192.100.1  # 拷贝key到第一个机器
   $ ssh-copy-id curve@10.192.100.2  # 拷贝key到第二个机器
   $ ssh-copy-id curve@10.192.100.3  # 拷贝key到第三个机器
   $ ssh 10.192.100.1   # 挨个验证一下配置是否正确
   ```
2. 安装ansible 2.5.9
   ```bash
   $ apt install python
   $ apt install python-pip
   $ pip install ansible==2.5.9
   $ ansible-playbook  # 如果没有报错的话说明安装成功，报错的话执行下面两步
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools
   ```

#### 实施部署
1. 切换到curve用户下执行以下操作

2. 获取tar包并解压

   有两种方式可以获得tar包：
      1. 从[github release页面](https://github.com/opencurve/curve/releases)下载稳定版本tar包
      2. 自行通过编译环境打tar包，该方式可以让您体验测试最新代码：[编译开发环境搭建](build_and_run.md)

   ```
   # 如下几个tar包可替换为其他版本（如您采用方式2自行打包，则不需要下载，拷贝相关tar包即可），下载命令仅供参考
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/curve_0.1.1+4b930380.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/nbd_0.1.1+4b930380.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/nebd_0.1.1+4b930380.tar.gz
   tar zxvf curve_0.1.1+4b930380.tar.gz
   tar zxvf nbd_0.1.1+4b930380.tar.gz
   tar zxvf nebd_0.1.1+4b930380.tar.gz
   cd curve/curve-ansible
   ```

3. 在中控机上修改配置文件

   **server.ini**

   ```
   [mds]
   mds1 ansible_ssh_host=10.192.100.1 // 改动
   mds2 ansible_ssh_host=10.192.100.2 // 改动
   mds3 ansible_ssh_host=10.192.100.3 // 改动

   [etcd]
   etcd1 ansible_ssh_host=10.192.100.1 etcd_name=etcd1 // 改动
   etcd2 ansible_ssh_host=10.192.100.2 etcd_name=etcd2 // 改动
   etcd3 ansible_ssh_host=10.192.100.3 etcd_name=etcd3 // 改动

   [snapshotclone]
   snap1 ansible_ssh_host=10.192.100.1 // 改动
   snap2 ansible_ssh_host=10.192.100.2 // 改动
   snap3 ansible_ssh_host=10.192.100.3 // 改动

   [snapshotclone_nginx]
   nginx1 ansible_ssh_host=10.192.100.1 // 改动
   nginx2 ansible_ssh_host=10.192.100.2 // 改动

   [chunkservers]
   server1 ansible_ssh_host=10.192.100.1 // 改动
   server2 ansible_ssh_host=10.192.100.2 // 改动
   server3 ansible_ssh_host=10.192.100.3 // 改动

   [mds:vars]
   mds_dummy_port=6667
   mds_port=6666
   mds_subnet=10.192.100.0/22                      // 改成想要起mds服务的ip对应的子网
   defined_healthy_status="cluster is healthy"
   mds_package_version="0.0.6.1+160be351"
   tool_package_version="0.0.6.1+160be351"
   # 启动命令是否用sudo
   mds_need_sudo=true
   mds_config_path=/etc/curve/mds.conf
   tool_config_path=/etc/curve/tools.conf
   mds_log_dir=/data/log/curve/mds
   topo_file_path=/etc/curve/topo.json

   [etcd:vars]
   etcd_listen_client_port=2379
   etcd_listen_peer_port=2380
   etcd_name="etcd"
   etcd_need_sudo=true
   defined_healthy_status="cluster is healthy"
   etcd_config_path=/etc/curve/etcd.conf.yml
   etcd_log_dir=/data/log/curve/etcd
   etcd_data_dir=/etcd/data
   etcd_wal_dir=/etcd/wal

   [snapshotclone:vars]
   snapshot_port=5556
   snapshot_dummy_port=8081
   snapshot_subnet=10.192.100.0/22                      // 改成想要启动mds服务的ip对应的子网
   defined_healthy_status="cluster is healthy"
   snapshot_package_version="0.0.6.1.1+7af4d6a4"
   snapshot_need_sudo=true
   snapshot_config_path=/etc/curve/snapshot_clone_server.conf
   snap_s3_config_path=/etc/curve/s3.conf
   snap_client_config_path=/etc/curve/snap_client.conf
   snap_tool_config_path=/etc/curve/snapshot_tools.conf
   client_register_to_mds=false
   client_chunkserver_op_max_retry=50
   client_chunkserver_max_rpc_timeout_ms=16000
   client_chunkserver_max_stable_timeout_times=64
   client_turn_off_health_check=false
   snapshot_clone_server_log_dir=/data/log/curve/snapshotclone

   [chunkservers:vars]
   wait_service_timeout=60
   env_name=pubt1
   check_copysets_status_times=1000
   check_copysets_status_interval=1
   cs_package_version="0.0.6.1+160be351"
   aws_package_version="1.0"
   defined_copysets_status="Copysets are healthy"
   chunkserver_base_port=8200
   chunkserver_format_disk=true        // 改动，为了更好的性能，实际生产环境需要将chunkserver上的磁盘全部预格式化，这个过程比较耗时1T的盘格式80%化大概需要1个小时
   chunk_alloc_percent=80              // 预创的chunkFilePool占据磁盘空间的比例，比例越大，格式化越慢
   # 每台机器上的chunkserver的数量
   chunkserver_num=3                                      // 改动，修改成机器上需要部署chunkserver的磁盘的数量
   chunkserver_need_sudo=true
   # 启动chunkserver要用到ansible的异步操作，否则ansible退出后chunkserver也会退出
   # 异步等待结果的总时间
   chunkserver_async=5
   # 异步查询结果的间隔
   chunkserver_poll=1
   chunkserver_conf_path=/etc/curve/chunkserver.conf
   chunkserver_data_dir=/data          // 改动，chunkserver想要挂载的目录，如果有两个盘，则会被分别挂载到/data/chunkserver0，/data/chunkserver1这些目录
   chunkserver_subnet=10.192.100.1/22                      // 改动
   chunkserver_s3_config_path=/etc/curve/cs_s3.conf
   # chunkserver使用的client相关的配置
   chunkserver_client_config_path=/etc/curve/cs_client.conf
   client_register_to_mds=false
   client_chunkserver_op_max_retry=3
   client_chunkserver_max_stable_timeout_times=64
   client_turn_off_health_check=false
   disable_snapshot_clone=true                           // 改动，这一项取决于是否需要使用快照克隆功能，需要的话设置为false，并提供s3_ak和s3_sk

   [snapshotclone_nginx:vars]
   snapshotcloneserver_nginx_dir=/etc/curve/nginx
   snapshot_nginx_conf_path=/etc/curve/nginx/conf/nginx.conf
   snapshot_nginx_lua_conf_path=/etc/curve/nginx/app/etc/config.lua
   nginx_docker_internal_port=80
   nginx_docker_external_port=5555

   [all:vars]
   need_confirm=true
   curve_ops_tool_config=/etc/curve/tools.conf
   need_update_config=true
   wait_service_timeout=10
   sudo_user=curve
   deploy_dir=/home/curve
   s3_ak=""                                            // 如果需要快照克隆服务，则修改成自己s3账号对应的值
   s3_sk=""                                            // 如果需要快照克隆服务，则修改成自己s3账号对应的值
   s3_nos_address=""                                   // 如果需要快照克隆服务，则修改成s3服务的地址
   s3_snapshot_bucket_name=""                          // 如果需要快照克隆服务，则修改成自己在s3上的桶名
   ansible_ssh_port=22
   curve_root_username=root                           // 改动，修改成自己需要的username，因为目前的一个bug，用到快照克隆的话用户名必须为root
   curve_root_password=root_password                  // 改动，修改成自己需要的密码
   lib_install_prefix=/usr/local
   bin_install_prefix=/usr
   ansible_connection=ssh                              // 改动

   ```

   **client.ini**

   ```
   [client]
   client1 ansible_ssh_host=10.192.100.1  // 修改成想要部署client的机器的ip，可以是server.ini中的机器，也可以是其他的机器，保证网络互通即可

   # 仅用于生成配置中的mds地址
   [mds]
   mds1 ansible_ssh_host=10.192.100.1  // 改成和server.ini中的mds列表一致即可
   mds2 ansible_ssh_host=10.192.100.2  // 改动
   mds3 ansible_ssh_host=10.192.100.3  // 改动

   [client:vars]
   ansible_ssh_port=1046
   nebd_package_version="1.0.2+e3fa47f"
   nbd_package_version=""
   sdk_package_version="0.0.6.1+160be351"
   deploy_dir=/usr/bin
   nebd_start_port=9000
   nebd_port_max_range=5
   nebd_need_sudo=true
   client_config_path=/etc/curve/client.conf
   nebd_client_config_path=/etc/nebd/nebd-client.conf
   nebd_server_config_path=/etc/nebd/nebd-server.conf
   nebd_data_dir=/data/nebd
   nebd_log_dir=/data/log/nebd
   curve_sdk_log_dir=/data/log/curve

   [mds:vars]
   mds_port=6666

   [all:vars]
   need_confirm=true
   need_update_config=true
   ansible_ssh_port=22
   lib_install_prefix=/usr/local
   bin_install_prefix=/usr
   ansible_connection=ssh    // 改动
   ```



   **group_vars/mds.yml**

   ```
   ---

   # 集群拓扑信息
   cluster_map:
     servers:
       - name: server1
         internalip: 10.192.100.1       // 部署chunkserver的机器对应的内部ip，用于curve集群内部（mds和chunkserver，chunkserver之间）通信
         internalport: 0                // 改动，多机部署情况下internalport必须是0，不然只有机器上对应端口的chunkserver才能够注册上
         externalip: 10.192.100.1       // 部署chunkserver的机器对应的外部ip，用于接受client的请求，可以设置成和internal ip一致
         externalport: 0                // 改动，多机部署情况下externalport必须是0，不然只有机器上对应端口的chunkserver才能够注册上
         zone: zone1
         physicalpool: pool1
       - name: server2
         internalip: 10.192.100.2       // 改动，原因参考上一个server
         internalport: 0                // 改动，原因参考上一个server
         externalip: 10.192.100.2       // 改动，原因参考上一个server
         externalport: 0                // 改动，原因参考上一个server
         zone: zone2
         physicalpool: pool1
       - name: server3
         internalip: 10.192.100.3       // 改动，原因参考上一个server
         internalport: 0                // 改动，原因参考上一个server
         externalip: 10.192.100.3       // 改动，原因参考上一个server
         externalport: 0                // 改动，原因参考上一个server
         zone: zone3
         physicalpool: pool1
     logicalpools:
       - name: logicalPool1
         physicalpool: pool1
         type: 0
         replicasnum: 3
         copysetnum: 300               // copyset数量与集群规模有关，建议平均一个chunkserver上100个copyset，比如三台机器，每台3个盘的话是3*3*100=900个copyset，除以三副本就是300个
         zonenum: 3
         scatterwidth: 0
   ```
   **group_vars/chunkservers.yml**
   修改成机器上具体的磁盘的名字列表，数量不固定。
   ```
   disk_list:
     - sda
     - sdb
     - sdc
   ```
   如果个别chunkserver的磁盘名字和其他的不同，需要单独拎出来放在host_vars下面。比如本例中，假设server1和server2上都是三个盘分别是sda，sdb，sdc和group_vars中的一致，而server3的是sdd，sde，sdf，sdg，那么需要在host_vars下面新增一个server3.yml，其中的内容为：
   ```
   disk_list:
     - sdd
     - sde
     - sdf
     - sdg
   ```

4. 部署集群并启动服务

   ```
   ansible-playbook -i server.ini deploy_curve.yml
   ```

5. 如果需要使用快照克隆功能，需要有S3账号，可以使用[网易云的对象存储](https://www.163yun.com/product/nos)  同上

   ```
   1. 在 server.ini 中，填写s3_nos_address，s3_snapshot_bucket_name，s3_ak和s3_sk disable_snapshot_clone=false
   2. 安装快照克隆服务
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone_nginx
   ```

6. 执行命令查看当前集群状态，主要看以下几个状态：
   - Cluster status中的total copysets为100，unhealthy copysets为0
   - Mds status中current MDS不为空，offline mds list为空
   - Etcd status中的current etcd不为空，offline etcd list为空
   - Chunkserver status中的offline为0
   ```
   curve_ops_tool status
   ```

7. 安装 Nebd 服务和 NBD 包

   ```
   ansible-playbook -i client.ini deploy_nebd.yml
   ansible-playbook -i client.ini deploy_nbd.yml
   ansible-playbook -i client.ini deploy_curve_sdk.yml
   ```

8. 在client的机器上创建 CURVE 卷，并通过 NBD 挂载到本地。创建CURVE卷的时候可能会报Fail to listen，这个属于日志打印问题，不影响结果，可以忽略。

   ```
   1. 创建 CURVE 卷： 命令为 curve create [-h] --filename FILENAME --length LENGTH --user USER， LENGTH >= 10
      curve create --filename /test --length 10 --user curve
   2. 挂载卷
      sudo curve-nbd map cbd:pool//test_curve_
   3. 查看设备挂载情况（在docker部署的情况下，list-mapped会看不到，可以选择lsblk看一下是否有/dev/nbd0类型的卷）
      curve-nbd list-mapped
   ```
