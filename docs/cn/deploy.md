<head><meta charset="UTF-8"></head>

## 概述

ansible是一款自动化运维工具，curve-ansible 是基于 ansible playbook 功能编写的集群部署工具。本文档介绍如何快速上手体验 CURVE 分布式系统：1. 使用 curve-ansible 在单机上模拟生产环境部署步骤  2. 使用 curve-ansible 在多机上部署最小生产环境

## 特别说明
- curve默认会将相关的库安装到/usr/lib下面，如果是CentOS系统，需要将client.ini和server.ini中的curve_lib_dir修改为/usr/lib64
- 一些外部依赖是通过源码的方式安装的，安装的过程中从github下载包可能会超时，这时可以选择重试或手动安装，jemalloc手动安装的话要保证configure的prefix与server.ini和client.ini的lib_install_prefix一致
- 如果机器上开启了SElinux可能会报Aborting, target uses selinux but python bindings (libselinux-python) aren't installed，可以尝试安装libselinux-python，或者强行关闭selinux
- deploy_curve.yml用于部署一个全新的集群，集群成功搭建后不能重复跑，因为会**扰乱集群**。想要清理集群的话需要手动停止mds，etcd，chunkserver并删掉/etcd目录（后续会完善清理脚本）。如果只是想启动服务的话，使用命令：
   ```
   ansible-playbook -i server.ini deploy_curve.yml --tags start
   ```
- 部署的过程中，在chunkserver成功启动之前都可以任意重试，**chunkserver启动成功后重试**要额外注意，要带上--skip-tags format,因为这一步会把启动成功的chunkserver的数据给清理掉，从而扰乱集群。


## 单机部署

- 适用场景：希望用单台 Linux 服务器，体验 CURVE 最小的拓扑的集群，并模拟生产部署步骤。

#### 准备环境

准备一台部署主机，确保其软件满足需求:

- 推荐安装 Debian 9 或者 Centos 8.0（其他环境未经测试）
  - Linux 操作系统开放外网访问，用于下载 CURVE 的安装包

最小规模的 CURVE 集群拓扑：

| 实例        | 个数 | IP        | 配置                       |
| ----------- | ---- | --------- | -------------------------- |
| MDS         | 1    | 127.0.0.1 | 默认端口<br />全局目录配置 |
| Chunkserver | 3    | 127.0.0.1 | 默认端口<br />全局目录配置 |

部署主机软件和环境要求：

- 部署需要创建一个有root权限的公共用户
- 部署主机需要开放 CURVE 集群节点间所需端口
- 目前仅支持在 x86_64 (AMD64) 架构上部署 CURVE 集群
- 安装 ansible 2.5.9，用于部署集群（**目前仅支持ansible 2.5.9版本，其他版本会有语法问题**）
- 安装 docker 18.09 及以上， 用于部署快照克隆服务器

#### 实施部署

0. ansible --version确认ansible版本是2.5.9

1. 以 ```root``` 用户登录中控机， 在中控机上创建 ```curve``` 用户， 确保能够通过 ```curve```用户ssh登录本机
   - 切换到curve用户ssh-keygen生成ssh key
   - ssh-copy-id curve@localhost

2. 下载tar包并解压

   ```
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/curve_0.1.1+4b930380.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/nbd_0.1.1+4b930380.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.1/nebd_0.1.1+4b930380.tar.gz
   tar zxvf curve_0.1.1+4b930380.tar.gz
   tar zxvf nbd_0.1.1+4b930380.tar.gz
   tar zxvf nebd_0.1.1+4b930380.tar.gz
   cd curve/curve-ansible
   ```
3. 准备inventory文件
如果是debian系统，则不需要改动，如果是CentOs系统，需要将client.ini和server.ini中的curve_lib_dir修改为/usr/lib64
  ```
  curve_lib_dir=/usr/lib64
  ```

4. 部署集群并启动服务

   ```
   ansible-playbook -i server.ini deploy_curve.yml
   ```

5. 如果需要使用快照克隆功能，需要有S3账号，可以使用[网易云的对象存储](https://www.163yun.com/product/nos)

   ```
   1. 在 server.ini 中，填写s3_ak和s3_sk=替换成自己账号对应的
   2. 安装快照克隆服务
      ansible-playbook -i server.ini deploy_snapshotcloneserver.yml
      ansible-playbook -i server.ini deploy_snapshotcloneserver_nginx.yml
   ```

6. 执行命令查看当前集群状态，主要看以下几个状态

   ```
   curve_ops_tool status
   ```

7. 安装 Nebd 服务和 NBD 包

   ```
   ansible-playbook -i client.ini deploy_nebd.yml
   ansible-playbook -i client.ini deploy_nbd.yml
   ansible-playbook -i client.ini deploy_curve_sdk.yml
   ```

8. 创建 CURVE 卷，并通过 NBD 挂载到本地

   ```
   1. 创建 CURVE 卷： 命令为 curve create [-h] --filename FILENAME --length LENGTH --user USER， LENGTH >= 10
      curve create --filename /test --length 10 --user curve
   2. 挂载卷
      sudo curve-nbd map cbd:pool//test_curve_
   3. 查看设备挂载情况
      curve-nbd list-mapped
   ```



## 多机部署

- 适用场景：用多台 Linux 服务器，搭建 CURVE 最小的拓扑的集群，可以用于初步性能测试。

#### 准备环境

准备三台部署主机，确保其软件满足需求:

- 推荐安装 Debian 9 或者 Centos 8.0
  - Linux 操作系统开放外网访问，用于下载 CURVE 的安装包

 CURVE 集群拓扑：

| 实例                                                         | 个数   | IP                                               | 端口 |
| ------------------------------------------------------------ | ------ | ------------------------------------------------ | ---- |
| MDS                                                          | 3      | 10.192.100.1<br />10.192.100.2<br />10.192.100.3 | 6666 |
| Chunkserver<br />(三个Server上分别挂10个盘，每个Server上启动10个Chunkserver用例) | 10 * 3 | 10.192.100.1<br />10.192.100.2<br />10.192.100.3 | 8200 |

部署主机软件和环境要求：

- 部署需要在每个机器上创建一个有root权限的公共用户
- 部署主机需要开放 CURVE 集群节点间所需ssh端口
- 目前仅支持在 x86_64 (AMD64) 架构上部署 CURVE 集群
- 选择三台机器中的一个作为中控机，安装 ansible 2.5.9，用于部署集群（**目前只支持ansible 2.5.9下的部署**）
- 安装 docker 18.09 及以上， 用于部署快照克隆服务器

#### 实施部署

1. 在三台机器上分别用 ```root``` 用户登录，创建 ```curve``` 用户， 并生成ssh key，保证这三台机器可以通过ssh互相登录

2. 选择三台机器中的一个作为中控机，在中控机上下载tar包 并 解压

   ```
   wget https://github.com/opencurve/curve/releases/download/v0.1.0/curve_0.1.0+55cecca7.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.0/nbd_0.1.0+55cecca7.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v0.1.0/nebd_0.1.0+55cecca7.tar.gz
   tar zxvf curve_0.1.0+55cecca7.tar.gz
   tar zxvf nbd_0.1.0+55cecca7.tar.gz
   tar zxvf nebd_0.1.0+55cecca7.tar.gz
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
   mds_subnet=10.192.100.0/22                      // 改动
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

   [snapshotclone:vars]
   snapshot_port=5556
   snapshot_dummy_port=8081
   snapshot_subnet=10.192.100.0/22                      // 改动
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
   aws_package_version="1.0"

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
   chunkserver_num=10                                      // 改动，修改成机器上的数据盘对应的数量
   chunkserver_need_sudo=true
   # 启动chunkserver要用到ansible的异步操作，否则ansible退出后chunkserver也会退出
   # 异步等待结果的总时间
   chunkserver_async=5
   # 异步查询结果的间隔
   chunkserver_poll=1
   chunkserver_conf_path=/etc/curve/chunkserver.conf
   chunkserver_data_dir=./data
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
   ansible_ssh_port=22
   curve_root_username=root                           // 改动，修改成自己需要的username，因为目前的一个bug，用到快照克隆的话用户名必须为root
   curve_root_password=root_password                  // 改动，修改成自己需要的密码
   curve_bin_dir=/usr/bin
   curve_lib_dir=/usr/lib                             // 如果是CentOs系统，则修改成/usrlib64
   curve_include_dir=/usr/include
   lib_install_prefix=/usr/local

   ```

   **client.ini**

   ```
   [client]
   client1 ansible_ssh_host=10.192.100.1  // 改动

   # 仅用于生成配置中的mds地址
   [mds]
   mds1 ansible_ssh_host=10.192.100.1  // 改动
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
   curve_bin_dir=/usr/bin
   curve_lib_dir=/usr/lib                             // 如果是CentOs系统，需要修改成/usr/lib64
   curve_include_dir=/usr/include
   curvefs_dir=/usr/curvefs
   ansible_ssh_port=22
   lib_install_prefix=/usr/local
   ```



   **group_vars/mds.yml**

   ```
   ---

   # 集群拓扑信息
   cluster_map:
     servers:
       - name: server1
         internalip: 10.192.100.1       // 改动
         internalport: 0                // 改动
         externalip: 10.192.100.1       // 改动
         externalport: 0                // 改动
         zone: zone1
         physicalpool: pool1
       - name: server2
         internalip: 10.192.100.2       // 改动
         internalport: 0                // 改动
         externalip: 10.192.100.1       // 改动
         externalport: 0                // 改动
         zone: zone2
         physicalpool: pool1
       - name: server3
         internalip: 10.192.100.3       // 改动
         internalport: 0                // 改动
         externalip: 10.192.100.1       // 改动
         externalport: 0                // 改动
         zone: zone3
         physicalpool: pool1
     logicalpools:
       - name: logicalPool1
         physicalpool: pool1
         type: 0
         replicasnum: 3
         copysetnum: 2000
         zonenum: 3
         scatterwidth: 0
   ```

4. 部署集群并启动服务

   ```
   ansible-playbook -i server.ini deploy_curve.yml
   ```

5. 如果需要使用快照克隆功能，需要有S3账号，可以使用[网易云的对象存储](https://www.163yun.com/product/nos)  同上

   ```
   1. 在 server.ini 中，填写s3_ak="" s3_sk="" disable_snapshot_clone=false
   2. 安装快照克隆服务
      ansible-playbook -i server.ini deploy_snapshotcloneserver.yml
      ansible-playbook -i server.ini deploy_snapshotcloneserver_nginx.yml
   ```

6. 执行命令查看当前集群状态

   ```
   curve_ops_tool status
   ```

7. 安装 Nebd 服务和 NBD 包

   ```
   ansible-playbook -i client.ini deploy_nebd.yml
   ansible-playbook -i client.ini deploy_nbd.yml
   ansible-playbook -i client.ini deploy_curve_sdk.yml
   ```

8. 在client的机器上创建 CURVE 卷，并通过 NBD 挂载到本地

   ```
   1. 创建 CURVE 卷： 命令为 curve create [-h] --filename FILENAME --length LENGTH --user USER， LENGTH >= 10
      curve create --filename /test --length 10 --user curve
   2. 挂载卷
      sudo curve-nbd map cbd:pool//test_curve_
   3. 查看设备挂载情况
      curve-nbd list-mapped
   ```
