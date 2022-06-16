#!/usr/bin/env python
# -*- coding: utf8 -*-

#curve info
curve_workspace = "/var/lib/jenkins/workspace/curve_failover/"
chunkserver_start_script = "./deploy/local/chunkserver/start_chunkservers_locally.sh"
chunkserver_stop_script = "./deploy/local/chunkserver/stop_chunkservers_locally.sh"
mds_start = curve_workspace + "bazel-bin/src/mds/main/curvemds"
snapshot_server_start = curve_workspace + "bazel-bin/src/snapshotcloneserver/snapshotcloneserver"
curvefs_tool = curve_workspace + "bazel-bin/tools/curvefsTool"
createfile_tool = curve_workspace + "bazel-bin/src/tools/createFile"
libcurve_workflow = curve_workspace + "bazel-bin/test/client/fake/curve_client_workflow"
chunkserver_log_dir = curve_workspace + "deploy/local/chunkserver/log/"
snapshot_clone_server_conf = curve_workspace + "conf/snapshot_clone_server.conf"
client_conf = curve_workspace + "conf/client.conf"

# volume info
default_vol_size = 5
default_vol_max = 4000
default_vol_extend_size = 10

# file info
file_name = "/1"
old_name = "/1"
new_name = "/2"
file_name_with_parent = "/lzw/1"
old_name_with_parent = "/lzw/1"
new_name_with_parent = "/lzw/2"
file_name_no_parent = "/xyz/test"
dir_path = "/lzw"
dir_path_with_parent = "/lzw/test"
dir_path_no_parent = "/abc/test"
user_name = "userinfo"
pass_word = ""
root_name = "root"
root_password = "root_password"
root_error_password = "root_error_password"
size = 10737418240
small_size = 1073741824
big_size = 5497558138880
new_size = 21474836480
buf = "aaaaaaaa"*512
buf_list = []
offset = 0
length = 4096
write_stopped = False
# mds info
mds_ip = "127.0.0.1"
mds_port = 6666
mds_listen = "-listenAddr=127.0.0.1:6666"

#abnormal test
chunkserver_list = ["10.182.26.16","10.182.26.17","10.182.26.18","10.182.26.34","10.182.26.35","10.182.26.36"]
mds_list = ["10.182.26.25","10.182.26.16","10.182.26.17"]
etcd_list = ["10.182.26.16","10.182.26.17","10.182.26.18"]
client_list = ["10.182.26.25"]
chunkserver_reset_list = ["10.182.26.34","10.182.26.35","10.182.26.36"]
mds_reset_list = ["10.182.26.16","10.182.26.17"]
abnormal_user = "nbs"
pravie_key_path = "/home/nbs/rsa/id_rsa"
abnormal_db_host = "10.182.2.25"
recover_time = 3600
offline_timeout = 100
vol_uuid = ""
thrash_map = True
thrash_thread = []
recover_vol_md5 = ""
#snapshot_test
snap_server_list = ["10.182.26.25","10.182.26.16","10.182.26.17"]
snap_version = '2019-08-01'
snapshot_size = 10
snapshot_timeout = 1200
snapshot_thrash = ""
snapshot_volid = ""
snapshot_vip = "10.182.26.25"
# db info
db_host = "127.0.0.1"
db_port = 3306
db_user = "root"
db_pass = "qwer"
mds_db_name = "curve_mds"
snap_db_name = "curve_snapshot"
curve_sql = "./src/repo/curve_mds.sql"
snap_sql = "./src/repo/curve_snapshot.sql"


#fs test cfg
fs_cfg_path = "/var/lib/jenkins/workspace/ansibe-conf-fs"
fs_test_client = ["10.182.4.48"]
fs_mount_path = "/home/nbs/failover/"
fs_mount_dir = ["test1","test2"]
fs_md5check_dir = ["test1"]
fs_mds = ["10.182.26.34","10.182.26.35","10.182.26.36"]
fs_metaserver = ["10.182.26.34","10.182.26.35","10.182.26.36"]
fs_etcd = ["10.182.26.34","10.182.26.35","10.182.26.36"]
md5_check = []
fs_md5check_thread = ""
fs_mount_thread = ""
fs_use_curvebs = False
thrash_fs_mount = True
thrash_fs_mdtest = True
thrash_mount_host = "pubbeta2-nova48-3"
# chunkserver mount point
cs_0 = curve_workspace + "0"
cs_1 = curve_workspace + "1"
cs_2 = curve_workspace + "2"
cs_num = 3
# chunkserver log dir
cs_log = "deploy/local/chunkserver/log/"
mysql_log = "/var/log/mysql/mysql.log"

# topology info
cluster_map = "./tools/topo_example.json"
physicalpool_name = "pool1"
physical_op = "create_physicalpool"

# flag
clean_before = True
clean_after = True
sudo_flag = True
sudo_way = "-iu"

# logicalpool info
copyset_num = 64
logical_op = "create_logicalpool"

# libcurve workflow
fake_mds_false = "false"
fake_chunkserver_false = "false"

#nova_host
nova_host ="10.182.2.25"
nova_user = "nbs"
ssh_key = "/home/nbs/rsa/id_rsa"
#vm_host
vm_host = "60.191.87.67"
vm_stability_host = "60.191.87.92"
vm_user = "root"
fio_iosize = "4" #4k

# snapshot test param
snapshot_file_name = "/lc"
snapshot_s3_object_location = "snapshot_test_chunk_data@s3"

#curve thrash 
#image_id = "94f54f29-af1e-4afd-acc9-8481c560356b"
image_id = "ecd25866-b3d1-4e84-bd74-ddd5544f1809"
avail_zone = "dongguan1.curve2:pubbeta2-curve2.dg.163.org"
vm_prefix = ""

level1 = [('test_kill_chunkserver_num',1),\
          ('test_kill_chunkserver_num',2),\
          ('test_stop_chunkserver_host'),\
          ('test_ipmitool_restart_chunkserver'),\
          ('test_outcs_recover_copyset'),\
          ('test_kill_mds',1),\
          ('test_kill_etcd',1),\
          ('test_reboot_nebd')]
level2 = [('reboot_curve_vm'),\
#          ('test_ipmitool_restart_client'),\
          ('test_chunkserver_cpu_stress',95),\
          ('test_mds_cpu_stress',95),\
          ('test_client_cpu_stress',95),\
          ('test_chunkserver_mem_stress',95),\
          ('test_mds_mem_stress',95),\
          ('test_client_mem_stress',95)]
level3 = [('test_cs_loss_package',5),\
          ('test_stop_chunkserver_host'),\
          ('test_kill_chunkserver_num',1),\
          ('test_ipmitool_restart_chunkserver'),\
          ('test_kill_mds',3),\
          ('test_kill_etcd',3)]
