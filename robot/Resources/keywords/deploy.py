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

def backup_config():
    for host in config.mds_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo rm -rf /etc/curve-bak && sudo cp -r /etc/curve /etc/curve-bak"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"backup host %s config fail"%host
    for host in config.chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo rm -rf /etc/curve-bak && sudo cp -r /etc/curve /etc/curve-bak"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"backup host %s config fail"%host

def destroy_mds():
    for host in config.mds_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "ps -ef|grep -v grep | grep -v sudo | grep curve-mds | awk '{print $2}'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] == []:
            logger.error("mds not up")
            return
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
            logger.error("etcd not up")
            return
        pid = "".join(rs[1]).strip()
        kill_cmd = "sudo kill -9 %s"%pid
        rs = shell_operator.ssh_exec(ssh,kill_cmd)
        logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[1])))
        assert rs[3] == 0,"kill etcd fail"

def initial_chunkserver(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    try:
        kill_cmd = "sudo ./chunkserver_stop.sh all"
        logger.debug("stop host %s chunkserver" % host)
        rs = shell_operator.ssh_exec(ssh, kill_cmd)
#        assert rs[3] == 0
        time.sleep(2)
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
                    "DROP TABLE curve_session;", "DROP TABLE client_info;"]
        for cmd in cmd_list:
            conn = db_operator.conn_db(config.abnormal_db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
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
            ori_cmd = "sudo dpkg -i *%s"%version
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0,"mds install deb fail"
        for host in config.chunkserver_list:
            cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 %s*.deb %s:~/" %\
                  (config.pravie_key_path,config.curve_workspace,host)
            shell_operator.run_exec2(cmd)
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            ori_cmd = "sudo dpkg -i curve-chunkserver*%s curve-tools*%s"%(version,version)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0, "chunkserver install deb fail"
            rm_deb = "rm *%s"%version
            shell_operator.ssh_exec(ssh, rm_deb)
    except Exception:
        logger.error("install deb fail.")
        raise


def add_config_file():
    for host in config.mds_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo cp -r /etc/curve-bak /etc/curve"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"add host %s config fail"%host
    for host in config.chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo cp -r /etc/curve-bak /etc/curve"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"add host %s config fail"%host

def start_abnormal_test_services():
    try:
        for host in config.mds_list:
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            ori_cmd = "rm -rf etcd_log && mkdir etcd_log"
            shell_operator.ssh_exec(ssh, ori_cmd)
            etcd_cmd = "cd etcd_log && nohup etcd &"
            shell_operator.ssh_background_exec2(ssh, etcd_cmd)
            mds_cmd = "sudo nohup /usr/bin/curve-mds --confPath=/etc/curve/mds.conf &"
            shell_operator.ssh_background_exec2(ssh, mds_cmd)
            ori_cmd = "ps -ef|grep -v grep | grep -w etcd | awk '{print $2}'"
            time.sleep(2)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            logger.debug("etcd pid is %s"%rs[1])
            assert rs[1] != [], "up etcd fail"
            ori_cmd = "ps -ef|grep -v grep | grep -v sudo | grep -w curve-mds | awk '{print $2}'"
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[1] != [], "up mds fail"
            logger.debug("mds pid is %s"%rs[1])
        for host in config.chunkserver_list:
            ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
            ori_cmd = "sudo nohup ./chunkserver_start.sh all %s 8200 &"%host
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
    mds_host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(mds_host, 1046, config.abnormal_user)
    physical_pool = "curve-tool -cluster_map=topo.txt  -mds_ip=%s -mds_port=6666\
     -physicalpool_name=pool1 -op=create_physicalpool"%(mds_host)
    rs = shell_operator.ssh_exec(ssh, physical_pool)
    assert rs[1] == []
    time.sleep(120)
    logical_pool = "curve-tool -copyset_num=6000 -mds_ip=%s -mds_port=6666\
     -physicalpool_name=pool1 -op=create_logicalpool"%(mds_host)
    rs = shell_operator.ssh_exec(ssh, logical_pool)
    i = 0
    while i < 300:
       num = get_copyset_num()
       if num == 6000:
           break
       i = i + 5
       time.sleep(5)
    assert num == 6000,"create copyset fail,now copyset num is %d"%num

def restart_cinder_server():
    client_host = random.choice(config.client_list)
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    ori_cmd = "sudo service cinder-volume restart"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[1] == []
