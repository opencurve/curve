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
import fault_inject
import mythread

def check_fs_cluster_ok():
    return 1

def check_fuse_mount_success(fs_mount_dir=config.fs_mount_dir):
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    for mount_dir in fs_mount_dir:
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
    cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 robot/Resources/config/profile \
     %s:~/tools/vdbench/"%(config.pravie_key_path,test_client)
    shell_operator.run_exec2(cmd)
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
    port_list = []
    for mnt in config.fs_mount_dir:
        ori_cmd = "ps -ef|grep %s | grep curve-fuse |  grep -v grep | awk '{print $2}'"%mnt
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        pid = "".join(rs[1]).strip()
        ori_cmd = "sudo netstat -lntp |grep %s |awk '{print $4}'"%pid
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        port_list.append("".join(rs[1]).strip())
    for port in port_list:
#        port = port.strip()
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
    while time.time() - starttime < 2400:
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
    ori_cmd = "sudo ls /corefile |grep core"
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


def test_fuse_client_mem_stress(stress=80):
    client_host = config.fs_test_client[0]
    logger.info("|------begin test fuse mem stress,host %s------|"%(client_host))
    cmd = "free -g |grep Mem|awk \'{print $2}\'"
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    all_mem = int("".join(rs[1]).strip())
    stress = all_mem * stress / 100
    fault_inject.inject_mem_stress(ssh,stress)
    return ssh

def test_fuse_client_cpu_stress(stress=80):
#    client_host = random.choice(config.client_list)
    client_host = config.fs_test_client[0]
    logger.info("|------begin test fuse client cpu stress,host %s------|"%(client_host))
    cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 robot/Resources/keywords/cpu_stress.py \
     %s:~/"%(config.pravie_key_path,client_host)
    shell_operator.run_exec2(cmd)
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    fault_inject.inject_cpu_stress(ssh,stress)
    return ssh

def test_fs_process_delay_package(process_name,ms):
    if process_name == "mds":
        process_list = list(config.fs_mds)
    elif process_name == "metaserver":
        process_list = list(config.fs_metaserver)
    elif process_name == "etcd":
        process_list = list(config.fs_etcd)
    elif process_name == "fuseclient":
        process_list = list(config.fs_test_client)
    test_host = random.choice(process_list)
    ssh = shell_operator.create_ssh_connect(test_host, 1046, config.abnormal_user)
    dev = fault_inject.get_hostip_dev(ssh,test_host)
    logger.info("|------begin test host %s dev %s delay package------|"%(test_host,dev))
    try:
        fault_inject.package_delay_all(ssh, dev, ms)
        fault_inject.show_tc_inject(ssh,dev)
#        check_nbd_iops(1)
    except Exception as e:
        raise
    finally:
        time.sleep(60)
        fault_inject.cancel_tc_inject(ssh,dev)

def test_fs_process_loss_package(process_name,percent):
    if process_name == "mds":
        process_list = list(config.fs_mds)
    elif process_name == "metaserver":
        process_list = list(config.fs_metaserver)
    elif process_name == "etcd":
        process_list = list(config.fs_etcd)
    elif process_name == "fuseclient":
        process_list = list(config.fs_test_client)
    test_host = random.choice(process_list)
    ssh = shell_operator.create_ssh_connect(test_host, 1046, config.abnormal_user)
    dev = fault_inject.get_hostip_dev(ssh,test_host)
    logger.info("|------begin test host %s dev %s loss package------|"%(test_host,dev))
    try:
        fault_inject.package_loss_all(ssh, dev, percent)
        fault_inject.show_tc_inject(ssh,dev)
#        check_nbd_iops(1)
    except Exception as e:
        raise
    finally:
        time.sleep(60)
        fault_inject.cancel_tc_inject(ssh,dev)

def wait_fuse_exit(fusename=""):
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    if fusename == "":
        ori_cmd = "ps -ef|grep fuse | grep -v grep"
    else:
        ori_cmd = "ps -ef|grep %s | grep -v grep"%fusename
    i = 0
    while i < 300:
       rs = shell_operator.ssh_exec(ssh, ori_cmd)
       if rs[1] == []:
           break
       i = i + 5
       time.sleep(10)
    assert rs[1] == [],"fuse client not exit in 300s,process is %s"%rs[1]

def multi_mdtest_exec(ssh,test_dir):
    test_dir = os.path.join(config.fs_mount_path,test_dir)
    filenum_list = [100,200,300,500]
    filesize_list = [1024,4096,10240,20480,102400]
    filenum = random.choice(filenum_list)
    filesize = random.choice(filesize_list)
    ori_cmd = "mpirun --allow-run-as-root -np 8 mdtest -n %d -w %d -e %d -y -u -i 3 -N 1 -F -R -d %s"%(filenum,filesize,filesize,test_dir)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    logger.debug("mpirun mdtest output is %s"%rs[1])
    assert rs[3] == 0,"mdtest error, output %s"%rs[1]

def mount_umount_test():
    test_client = config.fs_test_client[0]
    ssh = shell_operator.create_ssh_connect(test_client, 1046, config.abnormal_user)
    test_dir = ["test3"]
    t = 0
    while config.thrash_fs_mount:
        multi_mdtest_exec(ssh,test_dir[0])
        ori_cmd = "sudo umount " + config.fs_mount_path + test_dir[0]
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"umount %s fail,error is %s"%(test_dir,rs[1])
        wait_fuse_exit(test_dir[0])
        time.sleep(5)
        cmd = "cd curvefs && make mount only=client hosts=%s"%config.thrash_mount_host
        ret = shell_operator.run_exec(cmd)
        time.sleep(10)
        check_fuse_mount_success(test_dir)
        time.sleep(2)
        t += 1
    return t

def loop_mount_umount():
    thread = mythread.runThread(mount_umount_test)
    logger.debug("thrash mount %s")
    config.fs_mount_thread = thread
    thread.start()

def stop_loop_mount():
    try:
        if config.fs_mount_thread == []:
            assert False," loop mount umount not up"
        t = config.fs_mount_thread
        config.thrash_fs_mount = False
        logger.info("set thrash_fs_mount to false")
        assert t.exitcode == 0,"mount/umount error"
        result = t.get_result()
        logger2.info("mount umount test time is %d"%result)
        assert result > 0,"test mount fail,result is %d"%result
    except:
        raise
