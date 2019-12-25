#!/usr/bin/env python
# -*- coding: utf8 -*-

import subprocess
from config import config
from logger import logger
from lib import shell_operator
from lib import db_operator
import threading
import random
import time
import mythread

def add_config():
    etcd = []
    for host in config.etcd_list:
        etcd.append(host + ":12379")
    etcd_addrs = ",".join(etcd)
    # add mds config
    for host in config.mds_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo rm *.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 conf/mds.conf %s:~/"%\
            (config.pravie_key_path,host)
        shell_operator.run_exec2(cmd)
        ori_cmd = R"sed -i 's/mds.listen.addr=127.0.0.1:6666/mds.listen.addr=%s:6666/g' mds.conf"%host
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        #change offline time
        ori_cmd = R"sed -i 's/mds.heartbeat.offlinetimeoutMs=.*/mds.heartbeat.offlinetimeoutMs=%d/g' mds.conf"%(config.offline_timeout*1000)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        #change scheduler time
        ori_cmd = R"sed -i 's/mds.copyset.scheduler.intervalSec=.*/mds.copyset.scheduler.intervalSec=0/g' mds.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        ori_cmd = R"sed -i 's/mds.replica.scheduler.intervalSec=.*/mds.replica.scheduler.intervalSec=0/g' mds.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        ori_cmd = R"sed -i 's/mds.recover.scheduler.intervalSec=.*/mds.recover.scheduler.intervalSec=0/g' mds.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        ori_cmd = R"sed -i 's/mds.leader.scheduler.intervalSec=.*/mds.leader.scheduler.intervalSec=5/g' mds.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        # change topology update time
        ori_cmd = R"sed -i 's/mds.topology.TopologyUpdateToRepoSec=.*/mds.topology.TopologyUpdateToRepoSec=1/g' mds.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        #add mysql conf
        ori_cmd = R"sed -i 's/mds.DbUrl=localhost/mds.DbUrl=%s/g' mds.conf"%(config.abnormal_db_host)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host
        #add etcd conf
        ori_cmd = R"sed -i 's/mds.etcd.endpoint=127.0.0.1:2379/mds.etcd.endpoint=%s/g' mds.conf"%(etcd_addrs)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s mds config fail"%host

        ori_cmd = "sudo mv mds.conf /etc/curve/"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"mv %s mds conf fail"%host
    # add client config
        mds_addrs = []
    for host in config.mds_list:
        mds_addrs.append(host + ":6666")
    addrs = ",".join(mds_addrs)
    for host in config.client_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo rm *.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 conf/client.conf %s:~/"%\
            (config.pravie_key_path,host)
        shell_operator.run_exec2(cmd)
        ori_cmd = R"sed -i 's/mds.listen.addr=127.0.0.1:6666/mds.listen.addr=%s/g' client.conf"%(addrs)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s client config fail"%host
        ori_cmd = "sudo mv client.conf /etc/curve/"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"mv %s client conf fail"%host
    # add chunkserver config
    addrs = ",".join(mds_addrs)
    for host in config.chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo rm *.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 conf/chunkserver.conf.example %s:~/chunkserver.conf"%\
            (config.pravie_key_path,host)
        shell_operator.run_exec2(cmd)
        #change global ip
        ori_cmd = R"sed -i 's/global.ip=127.0.0.1/global.ip=%s/g' chunkserver.conf"%host
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
        #change global subnet
        subnet=host+"/24"
        ori_cmd = R"sed -i 's#global.subnet=127.0.0.0/24#global.subnet=%s#g' chunkserver.conf"%subnet
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
        #change mds ip
        ori_cmd = R"sed -i 's/mds.listen.addr=127.0.0.1:6666/mds.listen.addr=%s/g' chunkserver.conf"%(addrs)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
 
        ori_cmd = R"sed -i 's/chunkserver.snapshot_throttle_throughput_bytes=.*/chunkserver.snapshot_throttle_throughput_bytes=104857600/g' chunkserver.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
        
        ori_cmd = R"sed -i 's/trash.expire_afterSec=.*/trash.expire_afterSec=0/g' chunkserver.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
         
        ori_cmd = R"sed -i 's/trash.scan_periodSec=.*/trash.scan_periodSec=10/g' chunkserver.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
        #open use snapshot
 #       ori_cmd = R"sed -i 's/clone.disable_curve_client=true/clone.disable_curve_client=false/g' chunkserver.conf"
 #       rs = shell_operator.ssh_exec(ssh, ori_cmd)
 #       assert rs[3] == 0,"change host %s chunkserver config fail"%host
 #       ori_cmd = R"sed -i 's/clone.disable_s3_adapter=true/clone.disable_s3_adapter=false/g' chunkserver.conf"
 #       rs = shell_operator.ssh_exec(ssh, ori_cmd)
 #       assert rs[3] == 0,"change host %s chunkserver config fail"%host
        ori_cmd = R"sed -i 's#curve.config_path=conf/client.conf#curve.config_path=/etc/curve/conf/client.conf#g' chunkserver.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
        ori_cmd = R"sed -i 's#s3.config_path=conf/s3.conf#s3.config_path=/etc/curve/conf/s3.conf#g' chunkserver.conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"change host %s chunkserver config fail"%host
        ori_cmd = "sudo mv chunkserver.conf /etc/curve/"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"mv %s chunkserver conf fail"%host
    # add s3 and client conf
    client_host = random.choice(config.client_list)
    cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 %s:/etc/curve/client.conf ."%\
            (config.pravie_key_path,client_host)
    shell_operator.run_exec2(cmd)
    for host in config.chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 conf/s3.conf client.conf %s:~/"%\
                            (config.pravie_key_path,host)
        shell_operator.run_exec2(cmd)
        ori_cmd = "sudo mv s3.conf /etc/curve/conf && sudo mv client.conf /etc/curve/conf"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"mv %s s3 conf fail"%host

def clean_env():
    host_list = config.client_list + config.mds_list + config.chunkserver_list 
    host_list = list(set(host_list))
    for host in host_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd1 = "sudo tc qdisc del dev bond0.106 root"
        shell_operator.ssh_exec(ssh, ori_cmd1)
        ori_cmd2 = "ps -ef|grep -v grep | grep memtester | awk '{print $2}'| sudo xargs kill -9"
        shell_operator.ssh_exec(ssh, ori_cmd2)
        ori_cmd3 = "ps -ef|grep -v grep | grep cpu_stress.py | awk '{print $2}'| sudo xargs kill -9"
        shell_operator.ssh_exec(ssh, ori_cmd3)    

def destroy_mds():
    for host in config.mds_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "ps -ef|grep -v grep | grep -v sudo | grep curve-mds | awk '{print $2}'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] == []:
            logger.debug("mds not up")
            continue
        pid = "".join(rs[1]).strip()
        kill_cmd = "sudo kill -9 %s"%pid
        rs = shell_operator.ssh_exec(ssh,kill_cmd)
        logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[1])))
        assert rs[3] == 0,"kill mds fail"

def destroy_etcd():
    for host in config.etcd_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "ps -ef|grep -v grep | grep etcd | awk '{print $2}'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] == []:
            logger.debug("etcd not up")
            continue
        pid = "".join(rs[1]).strip()
        kill_cmd = "sudo kill -9 %s"%pid
        rs = shell_operator.ssh_exec(ssh,kill_cmd)
        logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[1])))
        assert rs[3] == 0,"kill etcd fail"

def destroy_snapshotclone_server():
    for host in config.snap_server_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "ps -ef|grep -v grep |grep -v sudo | grep snapshotcloneserver | awk '{print $2}'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] == []:
            logger.debug("snapshotcloneserver not up")
            continue
        pid = "".join(rs[1]).strip()
        kill_cmd = "sudo kill -9 %s"%pid
        rs = shell_operator.ssh_exec(ssh,kill_cmd)
        logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[1])))
        assert rs[3] == 0,"kill snapshotcloneserver fail"
  
def initial_chunkserver(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    try:
        kill_cmd = "ps -ef|grep -v grep | grep -w chunkserver |grep -v sudo | awk '{print $2}' | sudo xargs kill -9"
        logger.debug("stop host %s chunkserver" % host)
        rs = shell_operator.ssh_exec(ssh, kill_cmd)
#        assert rs[3] == 0
        time.sleep(30)
        ori_cmd = "ps -ef|grep -v grep | grep -w curve-chunkserver | awk '{print $2}'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[1] == [], "kill chunkserver fail"
        ori_cmd = "bash delete.sh"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        logger.debug("delete dat ,return is %s"%rs[1])
        assert rs[3] == 0,"rm %s dat fail"%host
        ssh.close()
    except Exception as e:
        logger.error("%s" % e)
        raise
    return 0

def drop_all_chunkserver_dat():
    thread = []
    for host in config.chunkserver_list:
        t = mythread.runThread(initial_chunkserver, host)
        thread.append(t)
        logger.debug("%s %s" % (initial_chunkserver, host))
    for t in thread:
        t.start()
    for t in thread:
        logger.debug("drop cs dat get result is %d" % t.get_result())
        assert t.get_result() == 0

def drop_abnormal_test_db():
    try:
        cmd_list = ["DROP TABLE curve_logicalpool;", "DROP TABLE curve_copyset;", \
                    "DROP TABLE curve_physicalpool;", "DROP TABLE curve_zone;", \
                    "DROP TABLE curve_server;", "DROP TABLE curve_chunkserver;", \
                    "DROP TABLE curve_session;",  "DROP TABLE client_info;"]
        cmd_list_2 = ["DROP TABLE clone;","DROP TABLE snapshot;"]
        for cmd in cmd_list:
            conn = db_operator.conn_db(config.abnormal_db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
            db_operator.exec_sql(conn, cmd)
            logger.debug("drop table %s" %cmd)
        for cmd in cmd_list_2:
            conn = db_operator.conn_db(config.abnormal_db_host, config.db_port, config.db_user, config.db_pass, config.snap_db_name)
            db_operator.exec_sql(conn, cmd)
            logger.debug("drop table %s" %cmd)
    except Exception:
        logger.error("drop db fail.")
        raise

def install_deb():
    try:
#        mkdeb_url =  config.curve_workspace + "mk-deb.sh"
#        exec_mkdeb = "bash %s"%mkdeb_url
#        shell_operator.run_exec2(exec_mkdeb)
        cmd = "ls %scurve-mds*.deb"%config.curve_workspace
        mds_deb = shell_operator.run_exec2(cmd)
        version = mds_deb.split('+')[1]
        for host in config.mds_list:
            cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 %s*.deb %s:~/"%\
                  (config.pravie_key_path,config.curve_workspace,host)
            shell_operator.run_exec2(cmd)
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            ori_cmd = "sudo dpkg -i --force-overwrite  *%s* aws-sdk_1.0_amd64.deb"%version
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0,"mds install deb fail,error is %s"%rs
            rm_deb = "rm *%s*"%version
            shell_operator.ssh_exec(ssh, rm_deb)

        for host in config.chunkserver_list:
            cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 %s*.deb %s:~/" %\
                  (config.pravie_key_path,config.curve_workspace,host)
            shell_operator.run_exec2(cmd)
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            ori_cmd = "sudo dpkg -i --force-overwrite curve-chunkserver*%s* curve-tools*%s* aws-sdk_1.0_amd64.deb"%(version,version)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0, "chunkserver install deb fail,error is %s"%rs
            rm_deb = "rm *%s*"%version
            shell_operator.ssh_exec(ssh, rm_deb)
    except Exception:
        logger.error("install deb fail.")
        raise


def add_config_file():
    for host in config.mds_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo cp -r /etc/curve-bak /etc/curve"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"add host %s config fail,error is %s"%(host,rs[2])
    for host in config.chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo cp -r /etc/curve-bak /etc/curve"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"add host %s config fail,error is %s"%(host,rs[2])

def start_abnormal_test_services():
    try:
        for host in config.etcd_list:
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            ori_cmd = "sudo rm -rf /etcd/default.etcd"
            shell_operator.ssh_exec(ssh, ori_cmd)
            etcd_cmd = "cd etcdrun && sudo nohup  ./run.sh new &"
            shell_operator.ssh_background_exec2(ssh, etcd_cmd)
            ori_cmd = "ps -ef|grep -v grep | grep -w etcd | awk '{print $2}'"
            time.sleep(2)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            logger.debug("etcd pid is %s"%rs[1])
            assert rs[1] != [], "up etcd fail"
        for host in config.mds_list:
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            mds_cmd = "sudo nohup /usr/bin/curve-mds --confPath=/etc/curve/mds.conf &"
            shell_operator.ssh_background_exec2(ssh, mds_cmd)
            time.sleep(1)
            ori_cmd = "ps -ef|grep -v grep | grep -v sudo | grep -w curve-mds | awk '{print $2}'"
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[1] != [], "up mds fail"
            logger.debug("mds pid is %s"%rs[1])
        for host in config.snap_server_list:
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            ori_cmd = "cd snapshot/temp && sudo nohup ./snapshotcloneserver -conf=./snapshot_clone_server.conf &"
            shell_operator.ssh_background_exec2(ssh, ori_cmd)
    except Exception:
        logger.error("up servers fail.")
        raise

def get_copyset_num():
    conn = db_operator.conn_db(config.abnormal_db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
    sql = R"select * from curve_copyset;"
    copyset = db_operator.query_db(conn, sql)
    logger.info("now copyset num is %d"%copyset["rowcount"])
    return int(copyset["rowcount"])

def create_pool():
    ssh = shell_operator.create_ssh_connect(config.mds_list[0], 1046, config.abnormal_user)
    mds = []
    mds_addrs = ""
    for mds_host in config.mds_list:
        mds.append(mds_host + ":6666")
        mds_addrs = ",".join(mds)
    physical_pool = "curve-tool -cluster_map=topo.txt -mds_addr=%s\
            -physicalpool_name=pool1 -op=create_physicalpool"%(mds_addrs)
    rs = shell_operator.ssh_exec(ssh, physical_pool)
    if rs[3] == 0:
        logger.info("create physical pool sucess")
    else:
        assert False,"create physical fail ,msg is %s"%rs[2]
    for host in config.chunkserver_list:
        ssh2 = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo nohup ./chunkserver_ctl.sh start all &"
        shell_operator.ssh_background_exec2(ssh2, ori_cmd)
    time.sleep(120)
    logical_pool = "curve-tool -copyset_num=4000  -mds_addr=%s\
     -physicalpool_name=pool1 -op=create_logicalpool"%(mds_addrs)
    rs = shell_operator.ssh_exec(ssh, logical_pool)
    i = 0
    while i < 300:
       num = get_copyset_num()
       if num == 4000:
           break
       i = i + 5
       time.sleep(5)
    assert num == 4000,"create copyset fail,now copyset num is %d"%num

def restart_cinder_server():
    client_host = random.choice(config.client_list)
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    ori_cmd = "sudo service cinder-volume restart"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[1] == [],"rs is %s"%rs

def wait_cinder_server_up():
    cinder_host = config.nova_host
    ssh = shell_operator.create_ssh_connect(cinder_host, 1046, config.abnormal_user)
    ori_cmd = R"source OPENRC && cinder get-host-list --all-services | grep pool1 | grep curve2 | awk '{print $16}'"
    i = 0
    while i < 360:
       rs = shell_operator.ssh_exec(ssh, ori_cmd)
       status = "".join(rs[1]).strip()
       if status == "up":
           break
       i = i + 5
       time.sleep(5)
    assert status == "up","up curve2 cinder service fail,please check"
    if status == "up":
       time.sleep(60)



    
