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
import re
import string
import types

def check_fs_cluster_ok():
    return 1

def check_fuse_mount_success():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    for mount_dir in config.fs_mount_dir:
        grep_cmd = "ps -ef | grep curve-fuse | grep %s | grep -v grep | awk '{print $2}'  " % mount_dir
        rs = shell_operator.ssh_exec(ssh,grep_cmd)
        pid = "".join(rs[1]).strip()
        logger.info("pid=%s" %pid)
        if pid:
            logger.debug("process is: %s" % pid)
            try:
                ori_cmd = "stat -c %a " +  "%s/%s"%(config.fs_mount_path,mount_dir)
                rs = shell_operator.ssh_exec(ssh, ori_cmd)
                assert rs[3] == 0,"stat dir  %s fail,error is %s"%(mount_dir,rs[1])
                permission = "".join(rs[1]).strip()
                assert permission == "1777","dir mount fail,permission is %s"%(mount_dir,permission)
            except Exception: 
                logger.error(" mount test dir fail.")
                raise
        else:
            assert False,"process %s not exsits" % mount_dir
    ssh.close()

def get_fuse_pid(mount_dir):
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    grep_cmd = "ps -ef | grep curve-fuse | grep %s | grep -v grep | awk '{print $2}'  " % mount_dir
    rs = shell_operator.ssh_exec(ssh,grep_cmd)
    pid = "".join(rs[1]).strip()
    logger.info("pid=%s" %pid)
    return pid

def start_fs_vdbench():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "cd /home/nbs/tools/vdbench && sudo nohup ./vdbench -jn -f profile &"
    rs = shell_operator.ssh_background_exec2(ssh, ori_cmd)
    time.sleep(5)
    ssh.close()

def write_check_data():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    test_dir = config.fs_mount_path
    size = "1G"
    for name in config.fs_mount_dir:
        ori_cmd = "fio -directory=%s -ioengine=psync \
                                      -rw=randwrite -bs=4k   \
                                      -direct=1 -group_reporting=1 \
                                      -fallocate=none -time_based=1  \
                                      -name=%s -numjobs=1  \
                                      -iodepth=128 -nrfiles=1  \
                                      -size=%s -runtime=60"%(test_dir,name,size)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"write check data fail,error is %s"%rs[1]
        time.sleep(5)
        ori_cmd = "md5sum " + test_dir + name + ".0.0"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        md5 = "".join(rs[1]).split(" ")[0]
        config.md5_check.append(md5)
    logger.info("file md5 is %s"%config.md5_check)
    ssh.close()

def cp_check_data():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    test_dir = config.fs_mount_path
    operator = ["cp","mv"]
    for name in config.fs_mount_dir:
        ori_cmd = random.choice(operator) + ' ' + test_dir + name + ".0.0" + ' ' + test_dir + name
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"cp fail"
    ssh.close()

def checksum_data():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    test_dir = config.fs_mount_path
    check_list = []
    for name in config.fs_mount_dir:
        ori_cmd = "md5sum " + test_dir + name  + '/' + name + ".0.0"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        md5 = "".join(rs[1]).split(" ")[0]
        check_list.append(md5)
    logger.info("check file md5 is %s"%check_list)
    assert check_list == config.md5_check,"md5 check fail,begin is %s,end is %s"%(config.md5_check,check_list)

def start_fs_fio():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "sudo supervisorctl stop all && sudo supervisorctl reload"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    time.sleep(5)
    ssh.close()

def check_fuse_iops(limit=1):
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "sudo netstat -lntp |grep curve-fuse |awk '{print $4}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    for port in rs[1]:
        port = port.strip()
        logger.info("get port %s ops" %port)
        ori_cmd = "sudo curl -s http://" + port + "/vars" +  " | grep \'user_write_bps :\'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"get bps fail,rs is %s"%rs[1]
        write_bps = "".join(rs[1]).strip().split(":")[-1]
        logger.info("now port %s bps is %s"%(port,write_bps))
        if write_bps.isdigit():
            assert int(write_bps) > limit,"get port %s user_write_bps %s is lower than %d"%(port,write_bps,limit)

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

def kill_process(host,process_name):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep  | grep %s | awk '{print $2}' | sudo xargs kill -9"%process_name
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"kill %s fail,error is %s"%(process_name,rs[2])

def test_kill_process(process_name,num=1):
    logger.info("|------begin test kill process %s num %d------|"%(process_name,num))
    if process_name == "mds":
      process_list = list(config.fs_mds)
    elif process_name == "metaserver":
      process_list = list(config.fs_metaserver)
    elif process_name == "etcd":
      process_list = list(config.fs_etcd)
    try:
        for i in range(0,num):
            host = random.choice(process_list)
            logger.info("process ip is %s"%host)
            kill_process(host,process_name)
            process_list.remove(host)
    except Exception as e:
        logger.error("kill process %s %sfail"%(process_name,host))
        raise
    return host

def test_start_process(process_name):
    try:
        cmd = "cd curvefs && make start only=%s"%process_name
        ret = shell_operator.run_exec(cmd)
        assert ret == 0 ,"start %s fail"%process_name
    except Exception as e:
        raise

def clean_fs_kernel_log():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "sudo logrotate -vf /etc/logrotate.d/rsyslog"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0," rollback log fail, %s"%rs[1]
    ssh.close()

def wait_op_finish():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    cmd = "sudo supervisorctl stop all"
    shell_operator.ssh_exec(ssh, cmd)
    ori_cmd1 = "ps -ef|grep -v grep | grep fio"
    ori_cmd2 = "ps -ef|grep -v grep | grep vdbench"
    starttime = time.time()
    while time.time() - starttime < 2000:
        rs1 = shell_operator.ssh_exec(ssh, ori_cmd1)
        rs2 = shell_operator.ssh_exec(ssh, ori_cmd2)
        if rs1[1] == [] and rs2[1] == []:
            return True
        else:
            logger.debug("fio & vdbench is running")
            time.sleep(30)
    assert False,"vdbench and fio is running timeout,pid is %s,%s"%(rs1[1],rs2[1])
    ssh.close()

def check_fs_io_error():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "sudo grep \'error\' /var/log/kern.log -R"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] != []:
        ori_cmd = "sudo logrotate -vf /etc/logrotate.d/rsyslog"
        shell_operator.ssh_exec(ssh, ori_cmd)
        assert False," rwio error,log is %s"%rs[1]
    ssh.close()

def clean_corefile():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "sudo mv /corefile/core*  /corefile/backup/"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    ssh.close()

def check_corefile():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    ori_cmd = "sudo find /corefile -name core*"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] != []:
        assert False,"/corefile have coredump file,is %s"%rs[1]

def get_test_dir_file_md5():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    test_dir = os.path.join(config.fs_mount_path,config.fs_mount_dir[0])
    ori_cmd ="cd " + test_dir +  " && find ./ ! -name 'md5*' -type f -print0 | xargs -0 md5sum  > md5_1"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"get file md5_1 error, output %s"%rs[1]
    
def check_test_dir_file_md5():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    test_dir = os.path.join(config.fs_mount_path,config.fs_mount_dir[0])
    ori_cmd ="cd " + test_dir +  " && find ./ ! -name 'md5*' -type f -print0 | xargs -0 md5sum  > md5_2"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"get file md5_2 error, output %s"%rs[1]
    ori_cmd = "cd " + test_dir + " && diff md5_1 md5_2"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"diff md5 file fail, output %s"%rs[1]
    assert rs[1] == [],"check fio test dir file md5 fail,diff is %s"%rs[1]

def multi_mdtest_exec(numjobs,filenum,filesize):
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    test_dir = os.path.join(config.fs_mount_path,config.fs_mount_dir[1])
    ori_cmd = "mpirun --allow-run-as-root -np %d mdtest -n %d -w %d -e %d -y -u -i 3 -N 1 -F -R -d %s"%(numjobs,filenum,filesize,filesize,test_dir)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"mdtest error, output %s"%rs[1]
