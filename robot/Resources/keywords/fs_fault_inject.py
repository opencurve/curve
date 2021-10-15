#!/usr/bin/env python
# -*- coding: utf8 -*-

import subprocess
from config import config
from logger.logger import *
from lib import shell_operator
import random
import time
import threading
import time
import mythread
import test_curve_stability_nbd
import re
import string
import types

def check_fs_cluster_ok():
    return 1

def check_mount_ok():
    try:
        test_client = config.fs_test_client[0]
        ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
        for test_dir in config.fs_mount_dir:
            ori_cmd = "stat -c %a " +  "%s/%s"%(config.fs_mount_path,test_dir)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0,"stat dir  %s fail,error is %s"%(test_dir,rs[1])
            permission = "".join(rs[1]).strip()
            assert permission == "1777","dir mount fail,permission is %s"%(test_dir,permission)
        ssh.close()
    except Exception:
        logger.error(" mount test dir fail.")
        raise

def check_fuse_process(mount_dir):
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep fuse_client"


def start_fs_vdbench():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "cd /home/nbs/tools/vdbench && sudo nohup ./vdbench -jn -f profile &"
    rs = shell_operator.ssh_background_exec2(ssh, ori_cmd)
    time.sleep(5)
    ssh.close()

def check_vdbench_output():
    try:
        ssh = shell_operator.create_ssh_connect(config.fs_test_client[0], 1046, config.abnormal_user)
        ori_cmd = "grep \"Vdbench execution completed successfully\" /home/nbs/tools/vdbench/output -R"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] == []:
            t = time.time()
            ori_cmd = "mv /home/nbs/tools/vdbench/output /home/nbs/vdbench-output/output-%d"%int(t)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert False,"vdbench test error,save log to test client /home/nbs/vdbench-output/output-%d"%int(t)
    except Exception as e:
        ssh.close()
        raise
    ssh.close()



