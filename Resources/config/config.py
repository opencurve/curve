#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
基本配置信息,若配置新的环境，需要更改此文件
'''
#curve info
curve_workspace = "/root/workspace/curve/curve_multijob/"
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

#test
# db info
db_host = "127.0.0.1"
db_port = 3306
db_user = "root"
db_pass = "qwer"
mds_db_name = "curve_mds"
snap_db_name = "curve_snapshot"

# chunkserver mount point
cs_0 = curve_workspace + "0"
cs_1 = curve_workspace + "1"
cs_2 = curve_workspace + "2"
cs_num = 3
# chunkserver log dir
cs_log = "deploy/local/chunkserver/log/"
mysql_log = "/var/log/mysql/mysql.log"

# topology info
cluster_map = "./tools/topo_example.txt"
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










