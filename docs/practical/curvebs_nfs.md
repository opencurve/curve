[TOC]
## CurveBS系统部署

### 环境信息

主机名|系统|IP|用户名密码
-|-|-|-
主控机|centos7|172.20.0.62|root/XX....
server-node1|centos7|172.20.0.38|root/vT2dALVE
server-node2|centos7|172.20.0.39|root/W0lFAv1h
server-node3|centos7|172.20.0.58|root/YVcR8Iwl
client-node1|centos7|172.20.0.61|root/KR0c61Vs


### curveadm 部署
主控机器安装curveadm
```shell
# docker 安装，主控节点，工作节点均需要安装
yum install docker

# curveadm 安装
bash -c "$(curl -fsSL https://curveadm.nos-eastchina1.126.net/script/install.sh)"
```


### 准备配置文件

```yaml
hosts.yaml

global:
  user: root
  ssh_port: 22
  private_key_file: /root/.ssh/id_rsa

hosts:
  - host: server-node1
    hostname: 172.20.0.38
  - host: server-node2
    hostname: 172.20.0.39
  - host: server-node3
    hostname: 172.20.0.58
  - host: client-node1
    hostname: 172.20.0.61
```

### 准备主机列表
```shell
#添加主机列表
curveadm hosts commit hosts.yaml
```

```yaml
format.yaml

host:
  - server-node1
  - server-node2
  - server-node3
disk:
  - /dev/vdc:/data/chunkserver0:90  # device:mount_path:format_percent
```

### 首先需要主动拉取镜像，加速后续的操作
```shell
#nohup  docker pull opencurvedocker/curvebs:v1.2  >log 2>&1 &
docker pull opencurvedocker/curvebs:v1.2 
```


### 格式化磁盘
```shell
#格式化磁盘
curveadm format -f format.yaml

#查看格式化进度
curveadm format --status
```

```yaml
topology.yaml

kind: curvebs
global:
  container_image: opencurvedocker/curvebs:v1.2
  log_dir: ${home}/logs/${service_role}${service_host_sequence}
  data_dir: ${home}/data/${service_role}${service_host_sequence}
  s3.nos_address: <>
  s3.snapshot_bucket_name: <>
  s3.ak: <>
  s3.sk: <>
  report_usage: false
  variable:
    home: /root/curvebs
    node1: server-node1
    node2: server-node2
    node3: server-node3

etcd_services:
  config:
    listen.ip: ${service_host}
    listen.port: 2380
    listen.client_port: 2379
  deploy:
    - host: ${node1}
    - host: ${node2}
    - host: ${node3}

mds_services:
  config:
    listen.ip: ${service_host}
    listen.port: 6700
    listen.dummy_port: 7700
  deploy:
    - host: ${node1}
    - host: ${node2}
    - host: ${node3}

chunkserver_services:
  config:
    listen.ip: ${service_host}
    listen.port: 8200 # 8200,8201,8202
    data_dir: /data/chunkserver0  # /data/chunkserver0, /data/chunksever1
    copysets: 100
    fs.enable_renameat2: false #3.10 kernel当前内核版本不支持 renameat() 接口 用户可将 chunkserver 中的 fs.enable_renameat2 配置项设置成 false
  deploy:
    - host: ${node1}
    - host: ${node2}
    - host: ${node3}

snapshotclone_services:
  config:
    listen.ip: ${service_host}
    listen.port: 5555
    listen.dummy_port: 8081
    listen.proxy_port: 8080
  deploy:
    - host: ${node1}
    - host: ${node2}
    - host: ${node3}
```

### 添加集群
```shell
#添加集群
curveadm cluster add my-cluster -f topology.yaml
#删除集群
curveadm cluster rm my-cluster
#切换集群
curveadm cluster checkout my-cluster

#执行预检查
curveadm precheck
curveadm precheck --skip <item> #topology ssh permission kernel network date service

```

### 部署集群
```shell
#部署集群
curveadm deploy --skip snapshotclone

#查看集群运行情况
curveadm status

#进入服务容器
curveadm enter <Id>
#检查健康情况
curve bs status cluster
```


### 添加客户端配置
```yaml
client.yaml

kind: curvebs
container_image: opencurvedocker/curvebs:v1.2
mds.listen.addr: 172.20.0.38:6700,172.20.0.39:6700,172.20.0.58:6700
log_dir: /root/curve/curvebs/logs/client

```

### 添加客户端主机
```shell
curveadm hosts commit client.yaml
```


centos client ,server 安装nbd模块
```shell

yum install net-tools
yum install telnet
yum install wget
yum install rpm-build
yum install vim
yum install kernel kernel-devel kernel-headers -y
wget https://vault.centos.org/7.9.2009/os/Source/SPackages/kernel-3.10.0-1160.el7.src.rpm
useradd mockbuild
rpm -ihv kernel-3.10.0-1160.el7.src.rpm
cd ~/rpmbuild/SOURCES/
tar Jxvf linux-3.10.0-1160.el7.tar.xz -C /usr/src/kernels/
cd /usr/src/kernels/linux-3.10.0-1160.el7/
yum install gcc
yum install elfutils-libelf-devel
make mrproper
cp ../3.10.0-1160.76.1.el7.x86_64/Module.symvers .
cp /boot/config-3.10.0-1160.76.1.el7.x86_64 .config
make oldconfig
make prepare
make scripts
make CONFIG_BLK_DEV_NBD=m M=drivers/block

vim drivers/block/nbd.c
// sreq.cmd_type = REQ_TYPE_SPECIAL;
sreq.cmd_type = 7;

make CONFIG_BLK_DEV_NBD=m M=drivers/block
modinfo drivers/block/nbd.ko
cp drivers/block/nbd.ko /lib/modules/$(uname -r)/kernel/drivers/block/
depmod -a
modprobe nbd
lsmod | grep nbd

#设置开机自动加载nbd模块
cat > /etc/modules-load.d/nbd.conf <<EOF
# Load nbd.ko at boot
nbd
EOF
``` 


### 创建磁盘挂载
```shell
#挂载
curveadm map curvebsuser1:/vdq --host client-node1 -c client.yaml --create --size 10GB
#非创建挂载
curveadm map curvebsuser1:/vdq --host client-node1 -c client.yaml --size 10GB
#解除挂载
curveadm unmap curvebsuser1:/vdq --host client-node1
```
<strong>
注意: curvebsuser1:/vdq 如下信息只是记录在curve系统 不需要在client系统上创建, 注意次用户名不能用下划线
map nbd设备的时候，下划线会用来作为卷和用户的分隔符的
</strong>

### client 查看集群空间情况
```shell
curve bs list space
```

### curveadm 查看client挂载的设备情况
```shell
curveadm client status 
Get Client Status: [OK]  

Id            Kind     Host          Container Id  Status                           Aux Info                                          
--            ----     ----          ------------  ------                           --------                                          
c7b6d6ce8a77  curvebs  client-node1  7a35f31c5648  Exited (143) About an hour ago   {"user":"curvebs_user","volume":"/dev/curvebs_bs"}
f1225dfb6b94  curvebs  client-node1  4a81d6b2ac6f  Exited (143) About a minute ago  {"user":"curvebs_user","volume":"/vdq"}           
4d01b8991a8a  curvebs  client-node1  5008c7362d66  Exited (143) 25 minutes ago      {"user":"curvebsuser1","volume":"/vdq"}           
f846805e752e  curvebs  client-node1  c0c0ceb45459  Exited (143) 25 minutes ago      {"user":"curvebs_user","volume":"/vdw"}           
ce0dc05be2ab  curvebs  client-node1  cc33ba0b4806  Up 55 seconds                    {"user":"curvebsuser1","volume":"/vde"}     
```
查看到对应的client挂载情况，可以通过container id 重启client docker容器

之后可以进入容器，然后执行相应的手动挂载
```shell
curve-nbd map cbd:pool//vde_curvebsuser1_
```

### 挂载块设备
```shell 
#查看块设备
fdisk -l

Disk /dev/nbd1: 10.7 GB, 10737418240 bytes, 20971520 sectors
Units = sectors of 1 * 512 = 512 bytes
Sector size (logical/physical): 512 bytes / 512 bytes
I/O size (minimum/optimal): 512 bytes / 512 bytes
# 制作文件系统
mkfs.ext4 /dev/nbd1
# 挂载块设备
mkdir /opt/nbd1
mount /dev/nbd1 /opt/nbd1
```


### nfs-server 安装
```shell
yum install nfs-utils
# 设置开机自启动
systemctl enable nfs
# 启动nfs-server
systemctl start nfs
# 编辑exportfs
vim /etc/exports

# 加入导出的目录    
# no_all_squash: 可以使用普通用户授权
# no_root_squash: 可以使用 root 授权
# sync: 同步共享目录
# rw: 权限设置，可读可写
# 192.168.0.0/24: 客户端 IP 范围，* 代表所有，即没有限制
# /data: 共享目录位置
/opt/nbd0     *(rw,sync,no_root_squash,no_all_squash)
/opt/nbd1     *(rw,sync,no_root_squash,no_all_squash)

#执行导出,注意必须执行此步骤
exportfs -a
# 重启nfs
systemctl restart nfs
# 查看导出情况
showmount -e
```


### nfs-client 安装

```shell
# 创建目录
mkdir /opt/nbd1
# 挂载目录
mount -o rw -t nfs 172.20.0.61:/opt/nbd1 /opt/nbd1
# 取消挂载目录
umount /root/data/
```




### FAQ
1.client 端不需要创建user 和 /dev 块设备
2.可以在client端进入容器执行创建卷：
```shell
/bin/bash /curvebs/nebd/sbin/create.sh curvebsuser1 /vde 10
```
3. 可以进入leader mds查看相应的mds日志
```shell
curveadm status
curveadm enter <mds id>
tailf logs
```
4. 也可以去client端手动启
```shell
docker exec  curvebs-volume-0673e7a472e0dfa4121595b0b457b8f7 /bin/bash /curvebs/nebd/sbin/create.sh curvebsuser1 /vde 10
```
5. 注意，也可以手动使用 curve 工具检查。
```shell
curve bs list dir --path=/
```
6. lsblk可以看到nbd设备,或者fdisk 可以检查挂载的磁盘
7. 检查空间情况 curve bs list space
8. 手动挡的可以参考自动挂载命令，或者重新挂载命令：
```code 
https://github.com/opencurve/curve/blob/master/k8s/nbd/nbd-package/usr/bin/mount_curve_clouddisk.sh
```
9. client自动挂载思路
```shell
1. systemd enable 设置docker自启动
2. curveadm 挂载启动容器以restart always启动
3. curveadm 启动后需要固化数据，把user,/vde 这类信息固定下来
4. docker 启动后自动init执行相应的 map挂载
5. 配置nbd的自动加载
```
