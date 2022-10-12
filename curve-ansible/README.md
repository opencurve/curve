**curve-ansile项目当前已经不进行维护，请使用最新工具curveadm进行部署：https://github.com/opencurve/curveadm**

curve-ansible是用ansible编写的curve高性能存储系统远程部署升级工具，可以很方便的在一台主控机上做到部署升级集群所有节点。

##  一、软件依赖
ansible 2.5.9
python 2.7.13

## 二、使用说明
### 1、inventory文件说明
inventory文件有client.ini和server.ini，client.ini存放client的机器列表以及对应的变量，server.ini存放除了client以外的机器，包括etcd，mds，快照克隆，chunkserver等。机器列表格式：

``````ini
localhost ansible_ssh_host=127.0.0.1 chunkserver_num=14
``````

其中localhost为机器别名，127.0.0.1为服务所用ip。chunkserver_num是主机变量，inventory中的所有变量都定义成了组变量，比如

``````ini
[chunkservers:vars]
chunkserver_num=13
``````


表示所有chunkserver机器上的chunkserver数量都是13，但是可能会有个别chunkserver的数量不等于13，这时候在主机变量中指定一下就可以覆盖组变量中的值，就能够做到特化处理，其他变量同理。

### 2、配置管理
配置由ansible通过模板的方式统一分发，所有配置文件的模板都在roles/generate_config/templates下面。配置项的默认值在roles/generate_config/defaults/main.yml中，如果不想使用默认值，则在inventory或命令行中额外指定这个变量的指即可。

### 3、包管理
curve-ansible同时提供了debian包和tar包的方式安装curve软件包。默认使用tar包方式安装，如果想用debian包的方式，需要在inventory或者命令行中指定install_with_deb=true。（debian的方式目前不支持格式化指定盘，只支持格式化全部ATA盘）

- debian包的方式下，需要使用mk-deb.sh打debian包，并将debian包上传到debian源中，使其能够用apt-get命令安装。
- tar包的方式下，tar包可以是来自curve github release，也可以是自己执行mk-tar.sh脚本获取到的。

## 三、使用命令
### 1、集群部署
部署分为两部分，一个是curve集群部署，即服务端部署，另一个是客户端部署，提供了sdk、qemu和nbd三种方式。curve集群部署提供了一键部署以及部署单个服务的命令。单个服务部署应该遵循etcd->mds->chunkserver->snapshotclone的顺序。所有play和role都有tag，可以根据tag来选择只执行部分操作。
##### 1.1 部署前准备
部署前需要准备inventory文件，inventory文件里的机器列表以及其他变量，根据自己的需求设置好相应的值。为了更好地发挥磁盘性能，curve提供了磁盘预格式化的功能，如果需要预格式化，则需要将inventory中的chunkserver_format_disk设置为true。如果是单机部署或不希望格式化磁盘，则将chunkserver_format_disk设置为false即可。
    此外，还需把group_vars/mds.yml中的cluster_map设置成自己集群对应的拓扑结构。cluster_map格式如下：

```yaml
cluster_map:
  servers:
    - name: server1
      internalip: 127.0.0.1
      internalport: 8200
      externalip: 127.0.0.1
      externalport: 8200
      zone: zone1
      physicalpool: pool1
    - name: server2
      internalip: 127.0.0.1
      internalport: 8201
      externalip: 127.0.0.1
      externalport: 8201
      zone: zone2
      physicalpool: pool1
    - name: server3
      internalip: 127.0.0.1
      internalport: 8202
      externalip: 127.0.0.1
      externalport: 8202
      zone: zone3
      physicalpool: pool1
  logicalpools:
    - name: logicalPool1
      physicalpool: pool1
      type: 0
      replicasnum: 3
      copysetnum: 100
      zonenum: 3
      scatterwidth: 0
```

其中servers是chunkserver所在的机器列表。如果是单机部署，需要指定internalport和externalport为单机部署的chunkserver对应的端口。如果是多机部署，则需要把internalport和externalport都设置成0。需要注意这里zone的数量要大于等于logicalpools里面指定的zonenum。logicalpools是逻辑池的列表，除了名字，需要指定它所属的物理池、类型、副本数量、copyset数量zone数量以及scatterwidth。目前的类型只支持0，page file类型。

如果需要格式化磁盘，则需要额外在group_vars/chunkservers.yml中指定磁盘的列表。比如：

```yaml
disk_list:
  - sda
  - sdb
  - sdc
```

 group_vars/chunkservers.yml中指定的变量是所有chunkserver共用的，如果某个chunkserver上的磁盘列表跟其他的不一致，则需要在host_vars中额外给出。比如server.ini中chunkserver是下面三台机器：

```ini
[chunkservers]
server1 ansible_ssh_host=10.192.100.1
server2 ansible_ssh_host=10.192.100.2
server3 ansible_ssh_host=10.192.100.3
```

假设server1和server2对应的都是sda,sdb,sdc, server3使用的是sda,sdb,sdd,sde这四块盘。那么server1和server2的disk_list不需要在host_vars额外指出，而server3的disk_list需要额外指出。具体做法是在host_vars下面新建一个server3.yml。里面的内容为：

```
disk_list:
  - sda
  - sdb
  - sdd
  - sde
```

#### 1.2 一键部署curve集群
一键部署命令:

```shell
ansible-playbook deploy_curve.yml -i server.ini
```


快照克隆服务需要提供s3账号和密码才能够部署，因此一键部署默认不会部署快照克隆和快照克隆Nginx，deploy_curve.yml中根据角色打了tag，因此部署个别服务，只需要指定对应的tag即可，下面会一一列举。

#### 1.3 部署etcd
部署命令：

```shell
ansible-playbook deploy_curve.yml -i server.ini --tags etcd
```

mds和快照克隆都需要用到etcd，因此部署的第一步是部署etcd。默认etcd的版本是v3.4.0，如果需要更高版本的话在inventory或命令行中指定etcd_version即可。

#### 1.4 部署mds
部署命令：

```shell
ansible-playbook deploy_curve.yml -i server.ini --tags mds
```

#### 1.5 创建物理池
命令：

```shell
ansible-playbook deploy_curve.yml -i server.ini --tags create_physical_pool
```


mds成功启动后，必须先创建物理池才能启动chunkserver，否则chunkserver会因为注册失败而退出。

#### 1.6 部署chunkserver
部署命令：

```shell
ansible-playbook deploy_curve.yml -i server.ini --tags chunkserver
```

chunkserver和其他组件相比多了一个格式化的步骤，如果指定了chunkserver_format_disk，会将机器上指定的磁盘全部格式化，否则不格式化，只是创建一下data目录。

#### 1.7 创建逻辑池
命令：

```shell
ansible-playbook deploy_curve.yml -i server.ini --tags create_logical_pool
```


启动chunkserver之后，集群还不能立马服务，需要创建逻辑池（逻辑池中会创建copyset）才能真正开始服务。

#### 1.8 部署快照克隆
快照克隆服务器独立于其他组件，因此需要单独部署。
部署命令：

```shell
ansible-playbook deploy_curve.yml -i server.ini --tags snapshotclone
```


如果快照克隆服务器没有做高可用，只有一台机器，那么不需要部署Nginx，直接访问快照克隆服务器的服务端口即可开始使用。

#### 1.9 部署快照克隆Nginx
如果快照克隆服务器不止一台，那么需要部署Nginx才能够正常访问。
部署命令：

```shell
ansible-playbook deploy_curve.yml -i server.ini --tags snapshotclone_nginx
```

#### 1.10 部署监控
一键部署监控命令：
```shell
ansible-playbook deploy_monitor.yml -i server.ini
```

### 2、集群升级
目前curve的升级流程为先升级mds，后升级chunkserver和快照克隆最后升级client。使用ansible需要指定一台主控机，我们规定主控机为mds节点之一。ansible-playbook同时需要yml文件和inventory文件，yml文件规定了要做哪些操作，inventory指定了机器列表并定义了一些变量。yml文件在curve仓库的curve-ansible目录中，inventory每个环境一份，由用户自行管理。

#### 2.1 一键升级curve集群
一键升级命令：

```shell
ansible-playbook rolling_update_curve.yml -i server.ini
```

etcd不会经常更新，通常是etcd-daemon和配置文件可能需要更新，因此一键升级中不包含etcd的升级，升级的话需要额外操作。

#### 2.2 升级etcd
etcd升级需要需要先升级follower，再升级leader，这个逻辑在yml文件中已经包含了，使用的时候只需要交互确认一下即可。

  (1) `ansible-playbook rolling_update_curve.yml -i server.ini --tags etcd`
  (2) 此时会打出"Confirm restart etcd in pubt1-curve1. ENTER to continue or CTRL-C A to quit:"，确认无误
      （确认第一个是备）后输入回车继续
  (3) 重复上一步直到所有etcd重启完毕

#### 2.3 升级mds
mds升级同样需要先升级备，再升级主，这个逻辑在yml文件中已经包含了，使用的时候只需要交互确认一下即可。
  (1) `ansible-playbook rolling_update_curve.yml -i server.ini --tags mds`
  (2) 此时会打出"Confirm restart mds in pubt1-curve1. ENTER to continue or CTRL-C A to quit:"，确认无误
      （确认第一个是备）后输入回车继续
  (3) 重复上一步直到所有mds升级完毕

#### 2.4 升级chunkserver
升级chunkserver是按照zone分批重启的。升级过程中重启一批机器的时候，会把这批机器的名字用逗号分隔打出来。
  (1) `ansible-playbook rolling_update_curve.yml -i server.ini --tags chunkserver`
  (2) 此时会打出"Confirm restart chunkserver in pubt1-curve1, pubt1-curve2. ENTER to continue or CTRL-C A to quit:"
      确认无误（集群healthy，io恢复，列出来的机器在同一个zone）后输入回车继续
  (3) 重复上一步直到所有chunkserver升级完毕

#### 2.5 升级快照克隆
快照克隆升级需要先升级备，再升级主，这个逻辑在yml文件中已经包含了，使用的时候只需要交互确认一下即可。
  (1) `ansible-playbook rolling_update_curve.yml -i server.ini --tags snapshotclone`
  (2) 此时会打出"Confirm restart snapshotclone in pubt1-curve1. ENTER to continue or CTRL-C A to quit:"
      确认无误（确认第一个是备）后输入回车继续
  (3) 重复上一步直到所有快照克隆升级完毕

#### 2.6 升级nebd-server
  (1) `ansible-playbook rolling_update_nebd.yml -i client.ini`
  (2) 升级nebd-server也有类似的确认步骤，由于机器比较多，可以选择指定 --extra-vars "need_confirm=false"来关掉
      确认这一步

#### 2.7 升级curve-sdk
curve-sdk主要给cinder，nova等服务使用，只需要更新包和配置即可，不需要重启服务
  (1) `ansible-playbook rolling_update_curve_sdk.yml -i client.ini`

### 3、集群回退
升级过程中，可能会因为一些问题导致升级失败，这时候会有回退的需求。回退是一个危险操作，因为老版本不一定能够兼容新版本，所以要谨慎回退。回退的时候只回退软件版本，不回退配置文件，所以要保证配置文件是向前兼容的（即只增加字段，不删除字段）。回退和升级共享一个yml文件，区别仅在与软件版本不同，所以额外在命令行中指定版本即可。

**如果是tar包的方式，则只需要下载上一个版本的tar包，解压后执行和升级操作一样的步骤即可。** 下面是debian包的方式下的回退过程。

#### 3.1 回退mds
mds回退同样需要先重启备，再重启主，这个逻辑在yml文件中已经包含了，使用的时候只需要交互确认一下即可。
  (1) `ansible-playbook rolling_update_curve.yml -i server.ini --tags mds --extra-vars  "mds_package_version=0.0.5.3+4b11a64d tool_package_version=0.0.5.3+4b11a64d  need_update_config=false"`
  (2) 此时会打出"Confirm restart mds in pubt1-curve1. ENTER to continue or CTRL-C A to quit:"，确认无误
      （确认第一个是备）后输入回车继续
  (3) 重复上一步直到所有mds回退完毕

#### 3.2 回退快照克隆
快照克隆回退需要先重启备，再重启主，这个逻辑在yml文件中已经包含了，使用的时候只需要交互确认一下即可。
  (1) `ansible-playbook rolling_update_curve.yml -i server.ini --tags snapshotclone --extra-vars "snap_package_version=0.0.6.1+160be351 need_update_config=false"`
  (2) 此时会打出"Confirm restart snapshotclone in pubt1-curve1. ENTER to continue or CTRL-C A to quit:"
      确认无误（确认第一个是备）后输入回车继续
  (3) 重复上一步直到所有快照克隆回滚完毕

#### 3.3 回退chunkserver
  (1) `ansible-playbook rolling_update_curve.yml -i server.ini --tags chunkserver --extra-vars "cs_package_version=0.0.5.3+4b11a64d need_update_config=false"`
  (2) 此时会打出"Confirm restart chunkserver in pubt1-curve1. ENTER to continue or CTRL-C A to quit:"
      确认无误（集群healthy，io恢复）后输入回车继续
  (3) 重复上一步直到所有chunkserver回退完毕

#### 3.4 回退nebd-server
  (1) `ansible-playbook rolling_update_nebd.yml -i client.ini --extra-vars "nebd_package_version=1.0.1+5e87f36 need_update_config=false"`
  (2) 回退nebd-server也有类似的确认步骤，由于机器比较多，可以选择指定 --extra-vars "need_confirm=false"来关掉确认这一步

#### 3.5 回退curve-sdk
curve-sdk主要给cinder，nova等服务使用，只需要更新包和配置即可，不需要重启服务
  (1) `ansible-playbook rolling_update_curve_sdk.yml -i client.ini --extra-vars  "sdk_package_version=0.0.5.3+4b11a64d need_update_config=false"`

### 4、其他命令

#### 4.1 清理集群

清理集群主要是清理一些残留数据和日志，已安装的库和可执行程序不会清理。

（1）一键清理curve集群：

```shell
ansible-playbook clean_curve.yml -i server.ini
```

对应的tag和deploy_curve.yml一致，清理单个服务的命令不再赘述。

（2）清理nebd数据

```shell
ansible-playbook clean_nebd.yml -i client.ini
```

（3）清理curve-sdk数据

```shell
ansible-playbook clean_curve_sdk.yml -i client.ini
```

#### 4.2 启动集群

目前的服务还没有做到开机自启动中，所以机器重启可能会使服务退出，因此需要重新拉起。

（1）一键开启curve集群

```shell
ansible-playbook start_curve.yml -i server.ini
```

对应的tag和deploy_curve.yml一致，清理单个服务的命令不再赘述。

（2）启动nebd-server

```shell
ansible-playbook start_nebd_server.yml -i client.ini
```

#### 4.3 关闭集群

（1）一键关闭curve集群

```shell
ansible-playbook stop_curve.yml -i server.ini
```

对应的tag和deploy_curve.yml一致，清理单个服务的命令不再赘述。

（2）关闭nebd-server

```shell
ansible-playbook stop_nebd_server.yml -i client.ini
```

#### 4.4 重启集群
重启集群可以通过rolling_update_curve.yml脚本完成，但是需要额外在命令行指定一些参数。**需要注意，ansible的tag之间是并集的关系而不是交集，因此重启chunkserver不能指定--tags restart, chunkserver，这样指定后所有带restart的脚本都会执行，导致所有服务一起重启，因此针对单个服务，定义了额外的tag**

（1）一键重启curve集群

```shell
ansible-playbook -i server.ini rolling_update_curve.yml --tags restart --extra-vars restart_directly=true
```
（2）重启mds

```shell
ansible-playbook -i server.ini rolling_update_curve.yml --tags restart_mds --extra-vars restart_directly=true
```
（3）重启chunkserver

```shell
ansible-playbook -i server.ini rolling_update_curve.yml --tags restart_chunkserver --extra-vars restart_directly=true
```
（4）重启快照克隆

```shell
ansible-playbook -i server.ini rolling_update_curve.yml --tags restart_snapshotclone --extra-vars restart_directly=true
```
（5）重启etcd
```shell
ansible-playbook -i server.ini rolling_update_curve.yml --tags restart_etcd --extra-vars restart_directly=true
```
（6）重启nebd-server

```shell
ansible-playbook rolling_update_nebd.yml -i client.ini --tags restart
```

三、目录结构说明

```
.
├── common_tasks                                            # 放置可以复用的代码（可以理解为各种函数）
│   ├── check_chunkserver.yml                               # 检查chunkserver机器配置
│   ├── check_cluster_healthy_status.yml                    # 检查集群健康状态
│   ├── check_docker_exists.yml                             # 检查是否安装了docker
│   ├── check_if_nbd_exists_in_kernel.yml                   # 检查内核是否有nbd模块
│   ├── check_mds.yml                                       # 检查mds机器配置
│   ├── create_dir.yml                                      # 创建目录
│   ├── create_logical_pool.yml                             # 创建逻辑池
│   ├── create_physical_pool.yml                            # 创建物理池
│   ├── get_curve_version_from_metric.yml                   # 从metric获取curve版本
│   ├── get_distro_name.yml                                 # 获取系统版本
│   ├── get_nebd_version_from_metric.yml                    # 从metric获取nebd版本
│   ├── install_with_yum_apt.yml                            # 用apt或yum的方式安装
│   ├── start_service.yml                                   # 启动服务
│   ├── stop_service.yml                                    # 停止服务
│   ├── update_config_with_puppet.yml                       # 使用puppet更新配置
│   ├── update_package.yml                                  # 更新包
│   ├── wait_copysets_status_healthy.yml                    # 在一段时间内循环检查copyset健康状态
│   ├── wait_until_server_down.yml                          # 等待直到server停掉
│   └── wait_until_server_up.yml                            # 等待直到server起来
├── group_vars                                              # 组变量
│   ├── chunkservers.yml                                    # 属于chunkserver的变量
│   └── mds.yml                                             # 属于mds的变量
├── host_vars                                               # 主机变量, 其中的文件名要合inventory中定义的主机名一致
│   └── localhost.yml                                       # 属于localhost的变量，host_vars优先级高于group_vars
├── roles                                                   # roles也是用来存放可以复用的代码，一个role内的task存在关联
│   ├── clean                                               # 清理数据的role
│   │   └── tasks
│   │       ├── include
│   │       │   ├── clean_chunkserver_retain_chunkfilepool.yml  # 清理集群，但是保留chunkfilepool
│   │       │   ├── clean_chunkserver_totally.yml               # 完全清理集群
│   │       |   ├── clean_chunkserver_with_disk_format.yml      # 在格式化磁盘的情况下清理chunkserver（需要umount）
│   │       |   ├── clean_chunkserver_without_disk_format.yml   # 在不格式化的情况下清理chunkserver（删除目录即可）
│   │       |   ├── clean_chunkserver.yml                       # 清理chunkserver
│   │       |   ├── clean_curve_sdk.yml                         # 清理curve_sdk残留数据
│   │       |   ├── clean_etcd.yml                              # 清理etcd残留数据
│   │       |   ├── clean_mds.yml                               # 清理mds残留数据
│   │       |   ├── clean_nebd.yml                              # 清理nebd残留数据
│   │       |   ├── clean_snapshotcloneserver_nginx.yml         # 清理快照克隆Nginx数据
│   │       |   └── clean_snapshotcloneserver.yml               # 清理快照克隆
│   │       └── main.yml
│   ├── format_chunkserver                                  # 用来格式化chunkserver的role
│   │   ├── defaults
│   │   │   └── main.yml                                    # 存放带默认值的变量
│   │   └── tasks
│   │       ├── include
│   │       │   ├── prepare_chunkserver_with_disk_format.yml    # 使用格式化磁盘的方式准备data目录
│   │       │   └── prepare_chunkserver_without_disk_format.yml # 使用非格式化磁盘的方式准备data目录
│   │       └── main.yml
│   ├── prepare_software_env                                # 用来自动检查和准备curve所需软件环境
│   │   └── tasks
│   │       └── main.yml
│   ├── restart_service                                     # 用来重启服务的role
│   │   ├── tasks                                           # 存放重启服务的task，main.yml是入口，其他的被main引用
│   │   │   ├── include
│   │   │   │   ├── append_need_restart_cs_list.yml         # 根据版本判断chunkserver是否需要重启并追加到重启列表中
│   │   │   │   ├── restart_chunkserver.yml                 # 重启chunkserver
│   │   │   │   ├── restart_etcd.yml                        # 重启etcd
│   │   │   │   ├── restart_mds.yml                         # 重启etcd
│   │   │   │   ├── restart_nebd.yml                        # 重启nebd server
│   │   │   │   └── restart_snapshotclone.yml               # 重启快照克隆
│   │   │   └── main.yml                                    # main.yml的所有task会被include到使用role的地方
│   │   └── vars
│   │       └── main.yml                                    # main.yml的所有变量会被包含到使用role的地方
│   ├── install_package                                     # 安装软件包的role
│   │   ├── defaults
│   │   │   └── main.yml                                    # 存放带有默认值的变量
│   │   ├── files                                           # 存放文件
│   │   │   └── disk_uuid_repair.py
│   │   ├── tasks
│   │   │   ├── include
│   │   │   │   ├── copy_file_to_remote.yml                 # 将文件拷贝到远端
│   │   │   │   ├── install_curve-chunkserver.yml           # 安装chunkserver
│   │   │   │   ├── install_curve-mds.yml                   # 安装mds
│   │   │   │   ├── install_curve-monitor.yml
│   │   │   │   ├── install_curve-nbd.yml                   # 安装nbd
│   │   │   │   ├── install_curve-sdk.yml                   # 安装curve-sdk
│   │   │   │   ├── install_curve-snapshotcloneserver-nginx.yml  # 安装快照克隆使用的Nginx
│   │   │   │   ├── install_curve-snapshotcloneserver.yml   # 部署快照克隆
│   │   │   │   ├── install_curve-tools.yml                 # 安装部署工具
│   │   │   │   ├── install_daemon.yml                      # 安装daemon
│   │   │   │   ├── install_deb_package.yml                 # 安装debian包
│   │   │   │   ├── install_etcd.yml                        # 安装etcd
│   │   │   │   ├── install_jemalloc.yml                    # 安装jemalloc
│   │   │   │   ├── install_nebd.yml                        # 安装nebd
│   │   │   │   ├── install_with_source_code.yml            # 从源码安装
│   │   │   │   └── set_curve_lib_dir.yml                   # 根据操作系统设置库安装路径
│   │   │   └── main.yml
│   │   ├── templates                                       # 存放模板
│   │   │   ├── chunkserver_ctl.sh.j2                       # chunkserver启动脚本的模板
│   │   │   ├── chunkserver_deploy.sh.j2                    # chunkserver格式化脚本的模板
│   │   │   ├── curve-monitor.sh.j2                         # 启动监控服务的脚本模板
│   │   │   ├── etcd-daemon.sh.j2                           # etcd启动脚本的模板
│   │   │   ├── mds-daemon.sh.j2                            # mds启动脚本的模板
│   │   │   ├── nebd-daemon.j2                              # nebd-server启动脚本的模板
│   │   │   └── snapshot-daemon.sh.j2                       # 快照克隆启动脚本模板
│   │   └── vars
│   │       └── main.yml
│   ├── set_leader_and_follower_list                        # 设置leader和follower列表
│   │   ├── tasks                                           # 存放task，main.yml是入口，其他的被main引用
│   │   │   ├── include
│   │   │   │   ├── get_all_ip.yml                          # 获取节点的全部ip
│   │   │   │   ├── get_etcd_leader_ip.yml                  # 获取etcd leader的ip
│   │   │   │   ├── get_mds_leader_ip.yml                   # 获取mds leader的ip
│   │   │   │   └── get_snapshot_leader_ip.yml              # 获取快照克隆leader的ip
│   │   │   └── main.yml                                    # main.yml的所有task会被include到使用role的地方
│   │   └── vars
│   │       └── main.yml                                    # main.yml的所有变量会被包含到使用role的地方
│   ├── generate_config                                     # 更新配置文件的role
│   │   ├── defaults                                        # 存放有默认值的变量
│   │   │   └── main.yml
│   │   ├── tasks
│   │   │   ├── include
│   │   │   │   ├── generate_config_with_template.yml       # 根据模板生成配置文件
│   │   │   │   └── update_config_with_puppet.yml           # 使用puppet更新配置（内部使用）
│   │   │   └── main.yml
│   │   ├── templates                                       # 配置文件的模板
│   │   │   ├── chunkserver.conf.j2
│   │   │   ├── client.conf.j2
│   │   │   ├── docker-compose.yml.j2                       # docker config for curve monitor
│   │   │   ├── etcd.conf.yml.j2
│   │   │   ├── grafana.ini.j2
│   │   │   ├── mds.conf.j2
│   │   │   ├── nebd-client.conf.j2
│   │   │   ├── nebd-server.conf.j2
│   │   │   ├── nginx_config.lua.j2
│   │   │   ├── nginx.conf.j2
│   │   │   ├── prometheus.yml.j2
│   │   │   ├── s3.conf.j2
│   │   │   ├── snapshot_clone_server.conf.j2
│   │   │   ├── snapshot_tools.conf.j2
│   │   │   ├── tools.conf.j2
│   │   │   └── topo.json.j2
│   │   └── vars
│   │       └── main.yml
│   ├── grafana_settings                                  # set grafana datasource and dashboard
│   │   └── tasks
│   │       └── main.yml
│   ├── stop_service                                       # 停止服务的role
│   │   ├── tasks
│   │   │   ├── include
│   │   │   │   ├── stop_chunkserver.yml                  # 停止chunkserver
│   │   │   │   ├── stop_etcd.yml                         # 停止etcd
│   │   │   │   ├── stop_mds.yml                          # 停止mds
│   │   │   │   ├── start_nebd.yml                        # 停止nebd server
│   │   │   │   ├── start_snapshotcloneserver_nginx.yml   # 停止快照克隆Nginx
│   │   │   │   └── start_snapshotcloneserver.yml         # 停止快照克隆服务器
│   │   │   └── main.yml
│   │   └── vars
│   │       └── main.yml
│   └── start_service                                      # 启动服务的role
│       ├── tasks
│       │   ├── include
│       │   │   ├── start_chunkserver.yml                  # 启动chunkserver
│       │   │   ├── start_etcd.yml                         # 启动etcd
│       │   │   ├── start_mds.yml                          # 启动mds
│       │   │   ├── start_monitor.yml                      # 启动监控
│       │   │   ├── start_nebd.yml                         # 启动nebd
│       │   │   ├── start_snapshotcloneserver_nginx.yml    # 启动Nginx
│       │   │   └── start_snapshotcloneserver.yml          # 启动快照克隆
│       │   └── main.yml
│       └── vars
│           └── main.yml
├── rolling_update_nebd.yml                                 # 升级nebd-server
├── rolling_update_nbd.yml                                  # 升级curve-nbd
├── rolling_update_curve_sdk.yml                            # 升级curve sdk
├── rolling_update_curve.yml                                # 一键升级curve集群
├── check_ansible_version.yml                               # 检查ansible的版本
├── check_chunkserver.yml                                   # 检查chunkserver所在机器配置
├── check_mds.yml                                           # 检查mds所在机器配置
├── deploy_curve_sdk.yml                                    # 部署curve-sdk
├── deploy_curve.yml                                        # 一键部署curve集群
├── deploy_monitor.yml                                      # 部署监控服务
├── deploy_nbd.yml                                          # 部署nbd
├── deploy_nebd.yml                                         # 部署nebd
├── deploy_walpool.yml                                      # 准备walpool
├── release_disk_5%_space.yml                               # 释放chunkserver磁盘占用的空间
├── README                                                  # 本帮助文档
├── client.ini                                              # client的inventory文件
├── server.ini                                              # server的列表，包括mds，快照克隆，etcd，chunkserver
├── start_curve.yml                                         # 启动curve集群
├── start_nebd_server.yml                                   # 启动nebd-server
├── stop_curve.yml                                          # 停止curve集群
└── stop_nebd_server.yml                                    # 停止nebd-server

```
