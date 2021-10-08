[中文版](../cn/deploy.md)

## Overview

Ansible is an automated operation and maintenance tool, and curve-ansible is a cluster deployment tool developed based on the ansible playbook. This document introduces how to quickly experience the CURVE distributed system: 1. Use curve-ansible to simulate the deployment steps of the production environment on a single machine. 2. Use curve-ansible to deploy the minimal production environment on multiple machines. For more usage and instructions of curve-ansible, please refer to [curve-ansible README](../../curve-ansible/README.md). **This article is for the latest version of the software package, please use the latest version of the package. If you encounter problems that are not described in the document during the installation process, please feedback via issue.**

## Tips

- Some external dependencies are installed through source code. During the installation process, downloading packages from github may time out. At this time, you can choose to retry or install manually. If jemalloc is installed manually, make sure that the prefix in configure is consistent with the 'lib_install_prefix' in server.ini and client.ini.

- If SElinux is turned on on the machine, it may report 'Aborting, target uses selinux but python bindings (libselinux-python) aren't installed', you can try to install 'libselinux-python', or close selinux forcibly.

- 'deploy_curve.yml' is used to deploy a brand new cluster. After the cluster is successfully set up, it cannot be run repeatedly because it will **disrupt the cluster**. You can choose to **start** the cluster or **clear** the cluster to redeploy. For detailed usage, see [curve-ansible README](../../curve-ansible/README.md).

- During the deployment process, you can always try again before the chunkserver is successfully started. **if you still want retry after the chunkserver is successfully started**, please use the '--skip-tags format', since it will clean up the data of the chunkserver that has been successfully started, thereby disrupting the cluster.

- If you need to use the curve-nbd function, there are two requirements for the kernel: The first is support the nbd module, you can 'modprobe nbd' to check whether the nbd module exists. The second is that the block size of the nbd device must be able to be set to 4KB. It has been verified in that CentOs8, which has been completely installed by [DVD1.iso](http://mirrors.163.com/centos/8/isos/x86_64/CentOS-8.2.2004-x86_64-dvd1.iso), the kernel version is 4.18. 0-193.el8.x86_64, which satisfies this condition.

## Deploy on single machine

- Scenes：hope to use a single Linux server to experience the smallest cluster of CURVE and simulate the production deployment steps.

We provide an all-in-one docker image. In this docker image, there are all compilation dependencies and running dependencies. Therefore, you can quickly deploy your own compiled code, of course, you can also choose to download the latest release package from github for deployment.

#### Prepare docker
Execute the following command to start docker:
   ```bash
   docker run --cap-add=ALL -v /dev:/dev -v /lib/modules:/lib/modules --privileged -it opencurve/curveintegration:centos8 /bin/bash
   ```

If you choose docker deployment, the following steps of [Prepare environment](./deploy_en.md#prepare-environment) can be skipped directly, and you can [Start deployment](./deploy_en.md#start-deployment).

#### Prepare environment

Prepare a deployment host to ensure that its software meets the requirements:

- It is recommended to install Debian9 or Centos7/8 (other environments have not been tested)
- The Linux operating system is open to external network access for downloading the installation package of CURVE
- Deployment needs to create a public user with root privileges
- Currently only supports deploying CURVE clusters on x86_64 (AMD64) architecture
- Install ansible 2.5.9 for cluster deployment (**Currently only ansible 2.5.9 version is supported, other versions will have syntax problems**)
- Install docker 18.09 for deploy snapshot clone server

The smallest CURVE cluster topology:

| Instance | number | IP      | configure                       |
| ----------- | ---- | --------- | -------------------------- |
| MDS         | 1    | 127.0.0.1 | port<br /> global directory configuration |
| Chunkserver | 3    | 127.0.0.1 | port<br /> global directory configuration |


##### steps for CentOs7/8 environment preparation
1. Log in to the machine as the root user and create a curve user:：

   ```bash
   $ adduser curve
   ```

2. Set up the curve user without secret sudo under root

   ```bash
   $ su  # switch to root user
   $ create a new file 'curve' under '/etc/sudoers.d'，add a line in it：curve ALL=(ALL) NOPASSWD:ALL
   $ su curve  # switch to curve user
   $ sudo ls  # Test if sudo is configured correctly
   ```
3. Install ansible 2.5.9
   ```bash
   $ sudo yum install python2  # install python2
   $ ln -s /usr/bin/python2 /usr/bin/python  # set default use python2
   $ pip2 install ansible==2.5.9  # install ansible
   $ ansible-playbook  # If no error is reported, the installation is successful, if an error is reported, perform the following two steps
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools
   ```
4. Make sure there are the following packages in the source：net-tools, openssl>=1.1.1, perf, perl-podlators, make

##### steps for Debian9 environment preparation
1. Log in to the machine as the root user and create a curve user:：

   ```bash
   $ adduser curve
   ```

2. Set up the curve user without secret sudo

   ```bash
   $ su  # switch to root user
   $ apt install sudo  # install sudo，if not installed
   $ create a new file 'curve' under '/etc/sudoers.d'，add a line in it：curve ALL=(ALL) NOPASSWD:ALL
   $ sudo -iu curve  # switch to curve user
   $ sudo ls  # Test if sudo is configured correctly
   ```
3. Install ansible 2.5.9
   ```bash
   $ apt install python
   $ apt install python-pip
   $ pip install ansible==2.5.9
   $ ansible-playbook  # If no error is reported, the installation is successful, if an error is reported, perform the following two steps
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools
   ```
4. Make sure there are the following packages in the source：net-tools, openssl>=1.1.1, perf, perl-podlators, make

#### Start Deployment
1. Switch to curve user do the following step

2. Get tar package and unzip

   There are two way to get the tar package：
      1. Get from [github release page](https://github.com/opencurve/curve/releases) download stable release tar package
      2. Generate the tar package through the compilation environment by yourself, this way allows you to experience and testing the latest code：[Build compilation and development environment](build_and_run.md)

   ```
   # Get a tar package of a specific version（If you use method 2 to pack it yourself, you don’t need to download it, just copy the relevant tar package），The download command is for reference only, please use the latest version
   wget https://github.com/opencurve/curve/releases/download/v{version}/curve_{version}.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v{version}/nbd_{version}.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v{version}/nebd_{version}.tar.gz
   tar zxvf curve_{version}.tar.gz
   tar zxvf nbd_{version}.tar.gz
   tar zxvf nebd_{version}.tar.gz
   cd curve/curve-ansible
   ```
3. Deploy the cluster and start the service. If you need to use the snapshot clone features, please set disable_snapshot_clone=False in server.ini before executing the script.

   ```
   ansible-playbook -i server.ini deploy_curve.yml
   ```

4. If you need to use the snapshot clone features, you need a s3 acount, or use [netease objetct storage](https://www.163yun.com/product/nos)

   ```
   1. In server.ini，fill in the s3_endpoint，s3_bucket_name，s3_ak and s3_sk。
   2. Install the snapshotclone service 
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone_nginx
   ```

5. Execute commands to view the current cluster status, mainly watch the following status
   - In Cluster status, the total copysets must be 100，the unhealthy copysets must be 0
   - In Mds status, the current MDS must not be empty，the offline mds list must be empty
   - In Etcd status, the current etcd must not be empty ，the offline etcd list must be empty
   - In Chunkserver status, the offline chunkserver must be 0

   ```
   curve_ops_tool status
   ```

6. Install Nebd service和 NBD package

   ```
   ansible-playbook -i client.ini deploy_nebd.yml
   ansible-playbook -i client.ini deploy_nbd.yml
   ansible-playbook -i client.ini deploy_curve_sdk.yml
   ```

7. Create a CURVE volume and mount it locally via NBD。When creating a CURVE volume, it may report Fail to listen. This is a log printing problem and does not affect the results and can be ignored.

   ```
   1. Create a CURVE volume, the command is 'curve create [-h] --filename FILENAME --length LENGTH --user USER'， LENGTH >= 10。The unit of length is GB.
      curve create --filename /test --length 10 --user curve
   2. mount the volume
      sudo curve-nbd map cbd:pool//test_curve_
   3. Check the device is mount or not（In the docker environment，list-mapped will show nothing，you can use lsblk alternatively to see if there is a volume like /dev/nbd0）
      curve-nbd list-mapped
   ```


9. Deployment monitoring system (optional). For documentation on the monitoring system, see [curve monitoring system documentation](./monitor.md). The deployment of monitoring system requires docker orchestration. First of all, please make sure that docker and docker-compose are installed. Secondly, you need to download and decompress the tar package of curve-monitor in the same level directory of the curve package.

   ```
   ansible-playbook deploy_monitor.yml -i server.ini
   ```

## Deploy on multiple machines

- Scenes：Use multiple Linux servers to build a cluster with the smallest CURVE topology, which can be used for preliminary performance testing.

#### Prepare environment

Prepare a deployment host to ensure that its software meets the requirements:

- It is recommended to install Debian9 or Centos7/8
- The Linux operating system is open to external network access for downloading the installation package of CURVE
- Deployment needs to create a public user with root privileges
- The deployment host needs to open the required ssh port between the CURVE cluster nodes
- Currently only supports deploying CURVE clusters on x86_64 (AMD64) architecture
- Choose one of the three machines as the central control machine and install ansible 2.5.9 for cluster deployment (**Currently only supports deployment under ansible 2.5.9**) 
- Install docker 18.09 for deploy snapshot clone server

At the same time, ensure that each machine has at least one data disk that can be formatted for use by the chunkserver.

 CURVE cluster topology：

| Instance                                                     | number | IP                                               | port |
| ------------------------------------------------------------ | ------ | ------------------------------------------------ | ---- |
| MDS                                                          | 3      | 10.192.100.1<br />10.192.100.2<br />10.192.100.3 | 6666 |
| Chunkserver<br />(There are 10 disks on each of the three servers, and each server has 10 instances of Chunkserver) | 10 * 3 | 10.192.100.1<br />10.192.100.2<br />10.192.100.3 | 8200 |

##### steps for CentOs7/8 environment preparation
The following steps require three machines to operate:
1. Log in to the machine as the root user and create a curve user:：

   ```bash
   $ adduser curve
   ```

2. Set up the curve user without secret sudo

   ```bash
   $ su  # switch to root user
   $ create a new file 'curve' under '/etc/sudoers.d'，add a line in it：curve ALL=(ALL) NOPASSWD:ALL
   $ su curve  # switch to curve user
   $ sudo ls  # Test if sudo is configured correctly
   ```
3. Make sure there are the following packages in the source：net-tools, openssl>=1.1.1, perf, perl-podlators, make

The following steps only need to be executed on the central control machine：
1. Configure ssh to log in to all machines (including yourself) under the curve user, assuming that the IPs of the three machines are 10.192.100.1, 10.192.100.2, 10.192.100.3
   ```bash
   $ su curve  # switch to curve user
   $ ssh-keygen  # generate ssh secret key
   $ ssh-copy-id curve@10.192.100.1  # copy the key to the first machine
   $ ssh-copy-id curve@10.192.100.2  # copy the key to the second machine
   $ ssh-copy-id curve@10.192.100.3  # copy the key to the second machine
   $ ssh 10.192.100.1   # Verify that the configuration is correct one by one
   ```
2. Install ansible 2.5.9
   ```bash
   $ sudo yum install python2  # Install python2
   $ ln -s /usr/bin/python2 /usr/bin/python  # set default use python2
   $ pip2 install ansible==2.5.9  # Install ansible
   $ ansible-playbook  # If no error is reported, the installation is successful, if an error is reported, perform the following two steps
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools
3. Make sure there are the following packages in the source：net-tools, openssl>=1.1.1, perf, perl-podlators, make

#### steps for Debian9 environment preparation
The following steps require three machines to operate:
1. Log in to the machine as the root user and create a curve user:：
   ```bash
   $ adduser curve
   ```
2. Set up the curve user without secret sudo
   ```bash
   $ su  # switch to root user
   $ apt install sudo  # install sudo，if not installed
   $ create a new file 'curve' under '/etc/sudoers.d'，add a line in it：curve ALL=(ALL) NOPASSWD:ALL
   $ sudo -iu curve  # switch to curve user
   $ sudo ls  # Test if sudo is configured correctly
   ```

The following steps only need to be executed on the central control machine：
1. Configure ssh to log in to all machines (including yourself) under the curve user, assuming that the IPs of the three machines are 10.192.100.1, 10.192.100.2, 10.192.100.3
   ```bash
   $ su curve  # switch to curve user
   $ ssh-keygen  # generate ssh secret key
   $ ssh-copy-id curve@10.192.100.1  # copy the key to the first machine
   $ ssh-copy-id curve@10.192.100.2  # copy the key to the second machine
   $ ssh-copy-id curve@10.192.100.3  # copy the key to the second machine
   $ ssh 10.192.100.1   # Verify that the configuration is correct one by one
   ```
2. Install ansible 2.5.9
   ```bash
   $ apt install python
   $ apt install python-pip
   $ pip install ansible==2.5.9
   $ ansible-playbook  # If no error is reported, the installation is successful, if an error is reported, perform the following two steps
   $ pip install --upgrade pip
   $ pip install --upgrade setuptools
   ```

#### Start Deployment

1. Switch to curve user do the following step

2. Get tar package and unzip

   There are two way to get the tar package：
      1. Get from [github release page](https://github.com/opencurve/curve/releases) download stable release tar package
      2. Generate the tar package through the compilation environment by yourself, this way allows you to experience and testing the latest code：[Build compilation and development environment](build_and_run.md)

   ```
   # Get a tar package of a specific version（If you use method 2 to pack it yourself, you don’t need to download it, just copy the relevant tar package），The download command is for reference only, please use the latest version
   wget https://github.com/opencurve/curve/releases/download/v{version}/curve_{version}.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v{version}/nbd_{version}.tar.gz
   wget https://github.com/opencurve/curve/releases/download/v{version}/nebd_{version}.tar.gz
   tar zxvf curve_{version}.tar.gz
   tar zxvf nbd_{version}.tar.gz
   tar zxvf nebd_{version}.tar.gz
   cd curve/curve-ansible
   ```

3. Modify the configuration file on the control machine

   **server.ini**

   ```
   [mds]
   mds1 ansible_ssh_host=10.192.100.1 // need change
   mds2 ansible_ssh_host=10.192.100.2 // need change
   mds3 ansible_ssh_host=10.192.100.3 // need change

   [etcd]
   etcd1 ansible_ssh_host=10.192.100.1 etcd_name=etcd1 // need change
   etcd2 ansible_ssh_host=10.192.100.2 etcd_name=etcd2 // need change
   etcd3 ansible_ssh_host=10.192.100.3 etcd_name=etcd3 // need change

   [snapshotclone]
   snap1 ansible_ssh_host=10.192.100.1 // need change
   snap2 ansible_ssh_host=10.192.100.2 // need change
   snap3 ansible_ssh_host=10.192.100.3 // need change

   [snapshotclone_nginx]
   nginx1 ansible_ssh_host=10.192.100.1 // need change
   nginx2 ansible_ssh_host=10.192.100.2 // need change

   [zone1]
   server1 ansible_ssh_host=10.192.100.1 // need change

   [zone2]
   server2 ansible_ssh_host=10.192.100.2 // need change

   [zone3]
   server3 ansible_ssh_host=10.192.100.3 // need change

   # Please ensure that the number of machines in the zone is the same
   [chunkservers:children]
   zone1
   zone2                                // need change
   zone3                                // need change

   [monitor]
   localhost ansible_ssh_host=127.0.0.1  // The machine where the monitoring system is deployed can be modified as needed

   [mds:vars]
   mds_dummy_port=6667
   mds_port=6666
   mds_subnet=10.192.100.0/22                      // Subnet served by mds
   defined_healthy_status="cluster is healthy"
   mds_package_version="0.0.6.1+160be351"
   tool_package_version="0.0.6.1+160be351"
   # Whether to start the command with sudo
   mds_need_sudo=True
   mds_config_path=/etc/curve/mds.conf
   mds_log_dir=/data/log/curve/mds
   topo_file_path=/etc/curve/topo.json

   [etcd:vars]
   etcd_listen_client_port=2379
   etcd_listen_peer_port=2380
   etcd_name="etcd"
   etcd_need_sudo=True
   defined_healthy_status="cluster is healthy"
   etcd_config_path=/etc/curve/etcd.conf.yml
   etcd_log_dir=/data/log/curve/etcd
   etcd_data_dir=/etcd/data
   etcd_wal_dir=/etcd/wal

   [snapshotclone:vars]
   snapshot_port=5556
   snapshot_dummy_port=8081
   snapshot_subnet=10.192.100.0/22                      // Subnet served by snapshotclone server
   defined_healthy_status="cluster is healthy"
   snapshot_package_version="0.0.6.1.1+7af4d6a4"
   snapshot_need_sudo=True
   snapshot_config_path=/etc/curve/snapshot_clone_server.conf
   snap_s3_config_path=/etc/curve/s3.conf
   snap_client_config_path=/etc/curve/snap_client.conf
   client_register_to_mds=False
   client_chunkserver_op_max_retry=50
   client_chunkserver_max_rpc_timeout_ms=16000
   client_chunkserver_max_stable_timeout_times=64
   client_turn_off_health_check=False
   snapshot_clone_server_log_dir=/data/log/curve/snapshotclone

   [chunkservers:vars]
   wait_service_timeout=60
   check_copysets_status_times=1000
   check_copysets_status_interval=1
   cs_package_version="0.0.6.1+160be351"
   defined_copysets_status="Copysets are healthy"
   chunkserver_base_port=8200
   chunkserver_format_disk=True        // need change, For better performance, the actual production environment needs to pre-format all the disks on the chunkserver. This process takes about 1 hour to format 80% of the 1T disk.
   auto_get_disk_list=False             // need change, If you need to automatically get the disk to be formatted, instead of specifying it in group_vars, change it to true
   get_disk_list_cmd="lsscsi |grep ATA|awk '{print $7}'|awk -F/ '{print $3}'"   // need change, the command to automatically get the disk list to be formatted.
   chunk_alloc_percent=80              // need change, The percentage of disk space occupied by pre-formatted chunkFilePool, the larger the ratio, the slower the format.
   wal_segment_alloc_percent=10        // need change, if use walpool features
   chunkserver_num=3                                      // need change, the number of chunkservers on each machine
   chunkserver_need_sudo=True
   chunkserver_conf_path=/etc/curve/chunkserver.conf
   chunkserver_data_dir=/data          // need change，The directory that chunkserver wants to mount. If there are two disks, they will be mounted to the directories /data/chunkserver0 and /data/chunkserver1 respectively.
   chunkserver_subnet=10.192.100.1/22                      // need change
   global_enable_external_server=True
   chunkserver_external_subnet=10.192.0.1/22               // need change，if the machine has two ips, set it to be different from chunkserver_subnet. If there is only one, keep the same.
   chunkserver_s3_config_path=/etc/curve/cs_s3.conf
   chunkserver_client_config_path=/etc/curve/cs_client.conf
   client_register_to_mds=False
   client_chunkserver_op_max_retry=3
   client_chunkserver_max_stable_timeout_times=64
   client_turn_off_health_check=False
   disable_snapshot_clone=True                           // need change, this item depends on whether you need to use the snapshot clone server, if necessary, set to False, and provide s3_ak and s3_sk
   chunk_size=16777216
   chunkserver_walfilepool_segment_size=8388608
   retain_pool=False                                     // need change，if set to True, chunkfilepool can be retained, which can be accelerated when cleaning and redeploying
   walfilepool_use_chunk_file_pool=True

   [snapshotclone_nginx:vars]
   snapshotcloneserver_nginx_dir=/etc/curve/nginx
   snapshot_nginx_conf_path=/etc/curve/nginx/conf/nginx.conf
   snapshot_nginx_lua_conf_path=/etc/curve/nginx/app/etc/config.lua
   nginx_docker_internal_port=80
   nginx_docker_external_port=5555

   [monitor:vars]
   monitor_work_dir=/etc/curve/monitor
   monitor_retention_time=7d
   monitor_retention_size=256GB
   prometheus_listen_port=9090
   prometheus_scrape_interval=3s
   prometheus_evaluation_interval=15s
   grafana_username=curve
   grafana_password=123
   grafana_listen_port=3000
   monitor_package_version="0.0.6.1.1+7af4d6a4"

   [all:vars]
   need_confirm=True
   curve_ops_tool_config=/etc/curve/tools.conf
   snap_tool_config_path=/etc/curve/snapshot_tools.conf
   wait_service_timeout=20
   deploy_dir="${HOME}"
   s3_ak=""                                            // If you need a snapshot clone service, modify it to the value corresponding to your s3 account
   s3_sk=""                                            // If you need a snapshot clone service, modify it to the value corresponding to your s3 account
   s3_endpoint=""                                   // If you need a snapshot clone service，modify it to the address of the s3 service
   s3_bucket_name=""                          // If you need a snapshot clone service, modify it to your own bucket name on s3
   ansible_ssh_port=22
   curve_root_username=root
   curve_root_password=root_password                  // need change
   lib_install_prefix=/usr/local
   bin_install_prefix=/usr
   ansible_connection=ssh                              // need change
   check_health=False
   clean_log_when_clean=False                          // clean the log or not
   snapshot_nginx_vip=127.0.0.1
   nginx_docker_external_port=5555
   curve_bin_dir=/usr/bin
   start_by_daemon=True                                // start by daemon or not
   sudo_or_not=True                                    // If the current user can execute sudo, it can be changed to False
   ansible_become_user=curve
   ansible_become_flags=-iu curve
   update_config_with_puppet=False
   service_async=5
   service_poll=1
   install_with_deb=False
   restart_directly=False

   ```

   **client.ini**

   ```
   [client]
   client1 ansible_ssh_host=10.192.100.1  // Modify the ip of the machine where you want to deploy the client, which can be the machine in server.ini or other machines, just to ensure network interoperability
   [mds]
   mds1 ansible_ssh_host=10.192.100.1  // Change it to be consistent with the mds list in server.ini
   mds2 ansible_ssh_host=10.192.100.2  // need change
   mds3 ansible_ssh_host=10.192.100.3  // need change

   [client:vars]
   ansible_ssh_port=1046
   nebd_package_version="1.0.2+e3fa47f"
   nbd_package_version=""
   sdk_package_version="0.0.6.1+160be351"
   deploy_dir=/usr/bin
   nebd_start_port=9000
   nebd_port_max_range=5
   nebd_need_sudo=True
   client_config_path=/etc/curve/client.conf
   nebd_client_config_path=/etc/nebd/nebd-client.conf
   nebd_server_config_path=/etc/nebd/nebd-server.conf
   nebd_data_dir=/data/nebd
   nebd_log_dir=/data/log/nebd
   curve_sdk_log_dir=/data/log/curve
   py_client_config_path=/etc/curve/py_client.conf
   clean_log_when_clean=false

   [mds:vars]
   mds_port=6666

   [all:vars]
   need_confirm=True
   need_update_config=True
   update_config_with_puppet=False
   ansible_ssh_port=22
   lib_install_prefix=/usr/local
   bin_install_prefix=/usr
   ansible_connection=ssh            // need change
   wait_service_timeout=20
   curve_bin_dir=/usr/bin
   start_by_daemon=True              // start nebd-server by daemon or not
   ```



   **group_vars/mds.yml**

   ```
   ---

   # cluster topology
   cluster_map:
     servers:
       - name: server1
         internalip: 10.192.100.1       // The internal ip corresponding to the machine where the chunkserver is deployed is used for communication within the curve cluster (between mds and chunkserver and chunkserver)
         internalport: 0                // The internalport must be 0 in the case of multi-machine deployment, otherwise only the chunkserver of the corresponding port on the machine can be registered
         externalip: 10.192.100.1       // The external ip corresponding to the machine where the chunkserver is deployed is used to accept client requests and can be set to be consistent with the internal ip
         externalport: 0                // The externalport must be 0 in the case of multi-machine deployment, otherwise only the chunkserver of the corresponding port on the machine can be registered
         zone: zone1
         physicalpool: pool1
       - name: server2
         internalip: 10.192.100.2       // need change, refer to the previous server
         internalport: 0                // need change, refer to the previous server
         externalip: 10.192.100.2       // need change, refer to the previous server
         externalport: 0                // need change, refer to the previous server
         zone: zone2
         physicalpool: pool1
       - name: server3
         internalip: 10.192.100.3       // need change, refer to the previous server
         internalport: 0                // need change, refer to the previous server
         externalip: 10.192.100.3       // need change, refer to the previous server
         externalport: 0                // need change, refer to the previous server
         zone: zone3
         physicalpool: pool1
     logicalpools:
       - name: logicalPool1
         physicalpool: pool1
         type: 0
         replicasnum: 3
         copysetnum: 300               // The number of copysets is related to the cluster size. It is recommended to average 100 copysets on a chunkserver. For example, for three machines, each with 3 disks is 3*3*100=900 copysets, divided by three copies is 300
         zonenum: 3
         scatterwidth: 0
   ```
   **group_vars/chunkservers.yml**
   Modified to the name list of specific disks on the machine, the number is not fixed.
   ```
   disk_list:
     - sda
     - sdb
     - sdc
   ```
   If the disk name of an individual chunkserver is different from others, it needs to be extracted separately and placed under host_vars. For example, in this example, assuming that the three disks on server1 and server2 are the same as sda, sdb, sdc, and group_vars, and server3 is sdd, sde, sdf, and sdg, then a server3 needs to be added under host_vars. yml, where the content is:
   ```
   disk_list:
     - sdd
     - sde
     - sdf
     - sdg
   ```

4. Deploy the cluster and start the service. The default concurrency of ansible is 5. If there are more than 5 machines, each step will be operated on the first 5 machines first, and then the next 5 machines will be operated after completion. If you want to increase the speed, you can add the -f [concurrent number] option after ansible-playbook.

   ```
   ansible-playbook -i server.ini deploy_curve.yml
   ```

5. If you need to use the snapshot clone features, you need a s3 acount, or use [netease objetct storage](https://www.163yun.com/product/nos)

   1. In server.ini，fill in the s3_endpoint，s3_bucket_name，s3_ak and s3_sk,
 disable_snapshot_clone=False
   2. Install the snapshotclone service
   ```
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone
      ansible-playbook -i server.ini deploy_curve.yml --tags snapshotclone_nginx
   ```

6. Execute commands to view the current cluster status, mainly watch the following status
   - In Cluster status, the total copysets must be 100，the unhealthy copysets must be 0
   - In Mds status, the current MDS must not be empty，the offline mds list must be empty
   - In Etcd status, the current etcd must not be empty ，the offline etcd list must be empty
   - In Chunkserver status, the offline chunkserver must be 0

   ```
   curve_ops_tool status
   ```

7. Install Nebd service和 NBD package

   ```
   ansible-playbook -i client.ini deploy_nebd.yml
   ansible-playbook -i client.ini deploy_nbd.yml
   ansible-playbook -i client.ini deploy_curve_sdk.yml
   ```

8. Create a CURVE volume and mount it locally via NBD。When creating a CURVE volume, it may report Fail to listen. This is a log printing problem and does not affect the results and can be ignored.

   ```
   1. Create a CURVE volume, the command is 'curve create [-h] --filename FILENAME --length LENGTH --user USER'， LENGTH >= 10。The unit of length is GB.
      curve create --filename /test --length 10 --user curve
   2. mount the volume
      sudo curve-nbd map cbd:pool//test_curve_
   3. Check the device is mount or not（In the docker environment，list-mapped will show nothing，you can use lsblk alternatively to see if there is a volume like /dev/nbd0）
      curve-nbd list-mapped
   ```
