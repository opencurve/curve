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

def block_ip(chain):
    ori_cmd = "iptables -I %s 2>&1" % chain
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    #rc = shell_operator.run_exec(cmd)


def cancel_block_ip(chain):
    ori_cmd = "iptables -I %s 2>&1" % chain
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    # rc = shell_operator.run_exec(cmd)

def net_work_delay(dev, time):
    ori_cmd = "tc qdisc add dev %s root netem delay %dms 2>&1" % (dev, time)
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    # rc = shell_operator.run_exec(cmd)

def package_loss_all(ssh,dev, percent):
    ori_cmd = "sudo tc qdisc add dev %s root netem loss %d%% 2>&1" % (dev, percent)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"error is %s"%rs[2]
    # rc = shell_operator.run_exec(cmd)

def package_delay_all(ssh, dev,ms):
    ori_cmd = "sudo tc qdisc add dev %s root netem delay %dms" % (dev, ms)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"error is %s"%rs[2]
    # rc = shell_operator.run_exec(cmd)

def cancel_tc_inject(ssh,dev):
    ori_cmd = "sudo tc qdisc del dev %s root" % dev
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"error is %s"%rs[2]
    # rc = shell_operator.run_exec(cmd)

def show_tc_inject(ssh,dev):
    ori_cmd = "sudo tc qdisc show dev %s " % dev
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"error is %s"%rs[2]
    # rc = shell_operator.run_exec(cmd)

def package_reorder_all(dev, ms, percent1, percent2):
    ori_cmd = "tc qdisc change dev %s root netem delay %s reorder %d%% %d%%" % (dev, ms, percent1, percent2)
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    # rc = shell_operator.run_exec(cmd)

def package_duplicate_all(dev, percent):
    ori_cmd = "tc qdisc add dev %s root netem duplicate %d%%" % (dev, percent)
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    # rc = shell_operator.run_exec(cmd)


def eth_down_for_a_monent(dev, time):
    ori_cmd = "ip link set %s down 2>&1 && sleep %d 2>&1 && ip link set %s up 2>&1" % (dev, time)
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    # rc = shell_operator.run_exec(cmd)


def add_rate_limit(dev, downlink, uplink):
    ori_cmd = "wget -N -P /tmp nos.netease.com/nfit-software/taaslimit.sh 2>&1 && chmod a+rx /tmp/taaslimit.sh 2>&1 " \
              "&& mv /tmp/taaslimit.sh /sbin/taaslimit 2>&1 && chown root:root /sbin/taaslimit && taaslimit %s %d %d 2>&1" % (dev, downlink, uplink)
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    # rc = shell_operator.run_exec(cmd)

def del_rate_limit(dev):
    ori_cmd = "taaslimit clear %s 2>&1" %(dev)
    cmd = shell_operator.gen_remote_cmd(config.ssh_user, config.ssh_hostname, 1046, config.ssh_key, ori_cmd,
                                        sudo_flag=True, sudo_way="")
    print cmd
    # rc = shell_operator.run_exec(cmd)

def inject_cpu_stress(ssh,stress=50):
    cmd = "sudo nohup python cpu_stress.py %d &"%stress
    shell_operator.ssh_background_exec2(ssh,cmd)
    cmd = "ps -ef|grep -v grep | grep cpu_stress.py | awk '{print $2}'"
    rs = shell_operator.ssh_exec(ssh,cmd)
    assert rs[1] != [],"up cpu stress fail"

def del_cpu_stress(ssh):
    cmd = "ps -ef|grep -v grep | grep cpu_stress.py | awk '{print $2}'"
    rs = shell_operator.ssh_exec(ssh,cmd) 
    if rs[1] == []:
        logger.info("no cpu stress running")
        return
    cmd = "ps -ef|grep -v grep | grep cpu_stress.py | awk '{print $2}'| sudo xargs kill -9"
    rs = shell_operator.ssh_exec(ssh,cmd)
    assert rs[3] == 0,"stop cpu stess fail"

def inject_mem_stress(ssh,stress):
    cmd = "sudo nohup /usr/local/stress/memtester/bin/memtester %dG > memtest.log  &"%stress
    shell_operator.ssh_background_exec2(ssh,cmd)
    cmd = "ps -ef|grep -v grep | grep memtester | awk '{print $2}'"
    rs = shell_operator.ssh_exec(ssh,cmd)
    assert rs[1] != [],"up memster stress fail"

def del_mem_stress(ssh):
    cmd = "ps -ef|grep -v grep | grep memtester | awk '{print $2}'"
    rs = shell_operator.ssh_exec(ssh,cmd)
    if rs[1] == []:
        logger.info("no memtester stress running")
        return
    cmd = "ps -ef|grep -v grep | grep memtester | awk '{print $2}'| sudo xargs kill -9"
    rs = shell_operator.ssh_exec(ssh,cmd)
    assert rs[3] == 0,"stop memtester stess fail"

def inject_clock_offset(ssh,time):
    cmd = "sudo date -s `date -d \"+%d min\" | awk \'{print $4}\'`" % time
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"inject clock offet fail,return is %s"%rs[2]

def del_clock_offset(ssh,time):
    cmd = "sudo date -s `date -d \"-%d min\" | awk \'{print $4}\'`" % time
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0, "del clock offet fail,return is %s" % rs[2]

def listen_network_stress(ip):
    ori_cmd = "iperf -s"
    ssh = shell_operator.create_ssh_connect(ip, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
#    assert rs[3] == 0,"up iperf fail: %s"%rs[1]

def inject_network_stress(ip):
    ori_cmd = "iperf -c %s -b 20000M -t 10 -p 5001"%ip
    ssh = shell_operator.create_ssh_connect(ip, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"inject iperf fail: %s"%rs[2]

def stop_network_stress(ip):
    ori_cmd = "ps -ef|grep iperf |grep -v grep| awk '{print $2}' | sudo xargs kill -9"
    ssh = shell_operator.create_ssh_connect(ip, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"stop iperf fail: %s"%rs[2]
    ori_cmd = "ps -ef|grep iperf |grep -v grep"
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[1] == [],"stop iperf fail,pid %s"%rs[1]

def ipmitool_cycle_restart_host(ssh):
    ori_cmd = "sudo ipmitool chassis power cycle"
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"cycle restart host fail,return is %s"%rs

def ipmitool_reset_restart_host(ssh):
    ori_cmd = "sudo ipmitool chassis power reset"
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"reset restart host fail,return is %s"%rs

def get_hostip_dev(ssh,hostip):
    ori_cmd = "ip a|grep %s | awk '{print $7}'"%hostip
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"error is %s"%rs[2]
    return "".join(rs[1]).strip()

def clear_RecycleBin():
    host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "curve_ops_tool clean-recycle --isTest"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"clean RecyclenBin fail,msg is %s"%rs[1]
    starttime = time.time()
    ori_cmd = "curve_ops_tool list -fileName=/RecycleBin |grep Total"
    while time.time() - starttime < 180:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if "".join(rs[1]).strip() == "Total file number: 0" and rs[3] == 0:
            break
        else:
            logger.debug("deleting")
            if rs[3] != 0:
                logger.debug("list /RecycleBin fail,error is %s"%rs[1])
            time.sleep(3) 
    assert rs[3] == 0,"delete /RecycleBin fail,error is %s"%rs[1]

def loop_map_unmap_file():
    thread = []
    for i in range(1):
        filename = "nbdthrash" + str(i)
        t = mythread.runThread(test_curve_stability_nbd.nbd_all, filename)
        thread.append(t)
        logger.debug("thrash map unmap %s" %filename)

    config.thrash_thread = thread
    for t in thread:
        t.start()
   # logger.debug("get result is %d" % t.get_result())
   # assert t.get_result() == 0

def stop_map_unmap():
    try:
        if config.thrash_thread == []:
            assert False,"map umap not up"
        thread = config.thrash_thread
        config.thrash_map = False
        logger3.info("set thrash_map to false")
        time = 0
        for t in thread:
            assert t.exitcode == 0,"map/umap thread error"
            result = t.get_result()
            logger.debug("thrash map/umap time is %d"%result)
            assert result > 0,"map/umap thread error"
            time = time + result
        logger.info("map/umap all time is %d"%time)
    except:
        raise   

def stop_rwio():
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    ori_cmd = "sudo supervisorctl stop all"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"stop rwio fail,rs is %s"%rs[1]
    ori_cmd = "ps -ef|grep -v grep | grep -w /home/nbs/vdbench50406/profile | awk '{print $2}'| sudo xargs kill -9"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    time.sleep(3)
    ssh.close()

def run_rwio():
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    ori_cmd =  "lsblk |grep nbd0 | awk '{print $1}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    output = "".join(rs[1]).strip()
    if output != "nbd0":
        logger.error("map is error")
        assert  False,"output is %s"%output
    ori_cmd =  "lsblk |grep nbd1 | awk '{print $1}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    output = "".join(rs[1]).strip()
    if output != "nbd1":
        logger.error("map is error")
        assert  False,"output is %s"%output
    ori_cmd = "sudo supervisorctl stop all && sudo supervisorctl reload"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    ori_cmd = "sudo nohup /home/nbs/vdbench50406/vdbench -jn -f /home/nbs/vdbench50406/profile &"
    rs = shell_operator.ssh_background_exec2(ssh, ori_cmd)
    #write 60s io
    time.sleep(60)
#    assert rs[3] == 0,"start rwio fail"
    ssh.close()

def write_full_disk(fio_size):
    ori_cmd = "sudo fio -name=/dev/nbd0 -direct=1 -iodepth=32 -rw=write -ioengine=libaio -bs=1024k -size=%dG -numjobs=1 -time_based"%int(fio_size)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"write fio fail"
   
def get_chunkserver_id(host,cs_id):
    client_host = config.client_list[0]
    logger.info("|------begin get chunkserver %s id %d------|"%(host,cs_id))
    cmd = "curve_ops_tool chunkserver-list | grep %s |grep -w chunkserver%d"%(host,cs_id)
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    chunkserver_info = "".join(rs[1]).strip().split(',')
    chunkserver_id = re.findall(r"\d+",chunkserver_info[0])
    if chunkserver_id != []:
        return int(chunkserver_id[0])
    else:
        return -1

def get_cs_copyset_num(host,cs_id):
    client_host = config.client_list[0]
    cs_number = int(cs_id) + 8200
    cmd = "curve_ops_tool check-chunkserver -chunkserverAddr=%s:%d |grep 'total copysets'"%(host,cs_number)
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    chunkserver_info = "".join(rs[1]).strip().split(',')
    chunkserver_id = re.findall(r"\d+",chunkserver_info[0])
    if chunkserver_id != []:
        return int(chunkserver_id[0])
    else:
        return -1 

def stop_vm(ssh,uuid):
    stop_cmd = "source OPENRC && nova stop %s"%uuid
    rs = shell_operator.ssh_exec(ssh, stop_cmd)
    assert rs[3] == 0,"stop vm fail,error is %s"%rs[2]
    time.sleep(5)

def start_vm(ssh,uuid):
    start_cmd = "source OPENRC && nova start %s"%uuid
    rs = shell_operator.ssh_exec(ssh, start_cmd)
    assert rs[3] == 0,"start vm fail,error is %s"%rs[2]

def restart_vm(ssh,uuid):
    restart_cmd = "source OPENRC && nova reboot %s"%uuid
    rs = shell_operator.ssh_exec(ssh, restart_cmd)
    assert rs[3] == 0,"reboot vm fail,error is %s"%rs[2]

def check_vm_status(ssh,uuid):
    ori_cmd = "source OPENRC && nova list|grep %s|awk '{print $6}'"%uuid
    i = 0
    while i < 180:
       rs = shell_operator.ssh_exec(ssh, ori_cmd)
       if "".join(rs[1]).strip() == "ACTIVE":
           return True
       elif "".join(rs[1]).strip() == "ERROR":
           return False
       else:
           time.sleep(5)
           i = i + 5
    assert False,"start vm fail"

def check_vm_vd(ip,nova_ssh,uuid):
    i = 0
    while i < 300:
        try:
            ssh = shell_operator.create_ssh_connect(ip, 22, config.vm_user)
            ori_cmd = "lsblk |grep vdc | awk '{print $1}'"
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            output = "".join(rs[1]).strip()
            if output == "vdc":
                ori_cmd = "source OPENRC &&  nova reboot %s --hard"%uuid
                shell_operator.ssh_exec(nova_ssh,ori_cmd)
            elif output == "":
                break
        except:
            i = i + 5
            time.sleep(5)
    assert rs[3] == 0,"start vm fail,ori_cmd is %s" % rs[1]

def init_vm():
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    ori_cmd = "source OPENRC && nova list|grep %s | awk '{print $2}'"%config.vm_host
    ori_cmd2 = "source OPENRC && nova list|grep %s | awk '{print $2}'"%config.vm_stability_host
    try:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        rs2 = shell_operator.ssh_exec(ssh, ori_cmd2)
        logger.debug("exec %s" % ori_cmd)
        logger.debug("exec %s" % ori_cmd2)
        uuid = "".join(rs[1]).strip()
        uuid2 = "".join(rs2[1]).strip()

        for i in range(1,10):
            ori_cmd = "bash curve_test.sh delete"
            shell_operator.ssh_exec(ssh, ori_cmd)
            ori_cmd = "source OPENRC &&  nova reboot %s --hard"%uuid
            ori_cmd2 = "source OPENRC &&  nova reboot %s --hard"%uuid2
            rs = shell_operator.ssh_exec(ssh,ori_cmd)
            rs2 = shell_operator.ssh_exec(ssh,ori_cmd2)
            time.sleep(60)
            rs1 = check_vm_status(ssh,uuid)
            rs2 = check_vm_status(ssh,uuid2)
            if rs1 == True and rs2 == True:
                break
        assert rs1 == True,"hard reboot vm fail"
        assert rs2 == True,"hard reboot vm fail"

        check_vm_vd(config.vm_host,ssh,uuid)
        check_vm_vd(config.vm_stability_host,ssh,uuid2)
    except:
        logger.error("init vm error")
        raise
    ssh.close()


def remove_vm_key():
    cmd = "ssh-keygen -f ~/.ssh/known_hosts -R %s"%config.vm_host
    shell_operator.run_exec(cmd)
    print cmd

def attach_new_vol(fio_size,vdbench_size):
    ori_cmd = "bash curve_test.sh create %d %d"%(int(fio_size),int(vdbench_size))
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"attach vol fail,return is %s"%rs[2]
    logger.info("exec cmd %s"%ori_cmd)
    get_vol_uuid()
    ssh.close()

def detach_vol():
    stop_rwio()
    ori_cmd = "bash curve_test.sh delete"
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"retcode is %d,error is %s"%(rs[3],rs[2])
    logger.info("exec cmd %s"%ori_cmd)
    ssh.close()

def clean_nbd():
    for client_ip in config.client_list:
        logger.info("|------begin test clean client %s------|"%(client_ip))
        cmd = "sudo curve-nbd list-mapped |grep nbd"
        ssh = shell_operator.create_ssh_connect(client_ip, 1046, config.abnormal_user)
        rs = shell_operator.ssh_exec(ssh, cmd)
        if rs[1] != []:
            for nbd_info in rs[1]:
                nbd = re.findall("/dev/nbd\d+",nbd_info)
                cmd = "sudo curve-nbd unmap " + nbd[0]
                rs = shell_operator.ssh_exec(ssh, cmd)
                assert rs[3] == 0,"unmap %s fail,error is %s"%(nbd,rs[2])
        cmd = "ps -ef|grep curve-nbd|grep -v grep | awk '{print $2}' | sudo xargs kill -9"
        rs = shell_operator.ssh_exec(ssh, cmd)
        return


def map_nbd():
    client_host = config.client_list[0]
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    cmd = "curve create --filename /fiofile --length 10 --user test --stripeUnit 2097152  --stripeCount 4"
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"create /fiofile fail：%s"%rs[2]
    cmd = "curve create --filename /vdbenchfile --length 10 --user test --stripeUnit 2097152  --stripeCount 4"
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"create /vdbenchfile fail：%s"%rs[2]
    time.sleep(3)
    cmd = "sudo curve-nbd map cbd:pool1//fiofile_test_ >/dev/null 2>&1"
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"map fiofile fail：%s"%rs[2]
    cmd = "sudo curve-nbd map cbd:pool1//vdbenchfile_test_ >/dev/null 2>&1"
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"map vdbenchfile fail：%s"%rs[2]

def delete_nbd():
    client_host = config.client_list[0]
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    cmd = "curve delete --filename /fiofile --user test"
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"delete /fiofile fail：%s"%rs[2]
    cmd = "curve delete --filename /vdbenchfile --user test"
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"delete /vdbenchfile fail：%s"%rs[2]

def check_host_connect(ip):
    cmd = "ping %s -w3"%ip
    status = shell_operator.run_exec(cmd)
    if status == 0:
        return True
    else:
        return False

def get_chunkserver_status(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    grep_cmd = "bash /home/nbs/chunkserver_ctl.sh status all"
    rs = shell_operator.ssh_exec(ssh,grep_cmd)
    chunkserver_lines = rs[1]
    logger.debug("get lines is %s"%chunkserver_lines)
    up_cs = [int(i.split()[0][11:]) for i in filter(lambda x: "active" in x, chunkserver_lines)]
    down_cs = [int(i.split()[0][11:]) for i in filter(lambda x: "down" in x, chunkserver_lines)]
    return {'up':up_cs, 'down':down_cs}
    ssh.close()

def kill_mult_cs_process(host,num):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    operate_cs = []
    for i in range(0,num):
        try:
           cs_status = get_chunkserver_status(host)
           up_cs = cs_status["up"]
           if up_cs == []:
               raise Exception("no chunkserver up") 
        except Exception as e:
           logger.debug("cs_status is %s"%cs_status)
           logger.error("%s"%e)
           raise AssertionError()
        logger.debug("cs_status is %s"%cs_status)
        cs = random.choice(up_cs)
        ori_cmd = "ps -ef|grep -v grep | grep -w chunkserver%d | awk '{print $2}' && \
        ps -ef|grep -v grep | grep -w /etc/curve/chunkserver.conf.%d |grep -v sudo | awk '{print $2}'"%(cs,cs)
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        logger.debug("exec %s"%ori_cmd)
        pid_chunkserver = "".join(rs[1]).strip()
        logger.info("test kill host %s chunkserver %s"%(host,cs))
        kill_cmd = "sudo kill -9 %s"%pid_chunkserver
        rs = shell_operator.ssh_exec(ssh,kill_cmd)
        logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[2])))
        assert rs[3] == 0,"kill chunkserver fail"
        up_cs.remove(cs)
        operate_cs.append(cs)
    ssh.close()
    return operate_cs

def start_mult_cs_process(host,num):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    operate_cs = []
    for i in range(0,num):
        try:
           cs_status = get_chunkserver_status(host)
           down_cs = cs_status["down"]
           if down_cs == []:
               raise Exception("no chunkserver down") 
        except Exception as e:
           logger.error("%s"%e)
           assert False
           #raise AssertionError()
        logger.debug("cs_status is %s"%cs_status)
        cs = random.choice(down_cs)
        if get_cs_copyset_num(host,cs) == 0:
            ori_cmd = "sudo rm -rf /data/chunkserver%d/chunkserver.dat"%(cs)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0
        ori_cmd = "sudo /home/nbs/chunkserver_ctl.sh start %d"%cs
        logger.debug("exec %s"%ori_cmd)
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        assert rs[3] == 0,"start chunkserver fail,error is %s"%rs[1]
        time.sleep(2)
        ori_cmd = "ps -ef|grep -v grep | grep -w chunkserver%d | awk '{print $2}' && \
        ps -ef|grep -v grep | grep -w /etc/curve/chunkserver.conf.%d |grep -v sudo | awk '{print $2}'" % (cs, cs)
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        if rs[1] == []:
            assert False,"up chunkserver fail"
        down_cs.remove(cs)
        operate_cs.append(cs)
    ssh.close()
    return operate_cs

def up_all_cs():
    operate_cs = []
    for host in config.chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        try:
           cs_status = get_chunkserver_status(host)
           down_cs = cs_status["down"]
           if down_cs == []:
               continue
        except Exception as e:
           logger.error("%s"%e)
           assert False
           #raise AssertionError()
        logger.debug("cs_status is %s"%cs_status)
        cs = random.choice(down_cs)
        for cs in down_cs:
            if get_cs_copyset_num(host,cs) == 0:
                ori_cmd = "sudo rm -rf /data/chunkserver%d/chunkserver.dat;sudo rm -rf /data/chunkserver%d/copysets;\
                sudo rm -rf /data/chunkserver%d/recycler"%(cs,cs,cs)
                rs = shell_operator.ssh_exec(ssh, ori_cmd)
                assert rs[3] == 0
            ori_cmd = "sudo /home/nbs/chunkserver_ctl.sh start %d"%cs
            logger.debug("exec %s"%ori_cmd)
            rs = shell_operator.ssh_exec(ssh,ori_cmd)
            assert rs[3] == 0,"start chunkserver fail"
            time.sleep(2)
            ori_cmd = "ps -ef|grep -v grep | grep -w chunkserver%d | awk '{print $2}' && \
            ps -ef|grep -v grep | grep -w /etc/curve/chunkserver.conf.%d |grep -v sudo | awk '{print $2}'" % (cs, cs)
            rs = shell_operator.ssh_exec(ssh,ori_cmd)
            if rs[1] == []:
                assert False,"up chunkserver fail"
        ssh.close()

def stop_host_cs_process(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    try:
        cs_status = get_chunkserver_status(host)
        up_cs = cs_status["up"]
        if up_cs == []:
            raise Exception("no chunkserver up")
    except Exception as e:
        logger.error("%s"%e)
        raise AssertionError()
    logger.debug("cs_status is %s"%cs_status)
    ori_cmd = "ps -ef|grep -v grep | grep -w curve-chunkserver |grep -v sudo | awk '{print $2}' | sudo xargs kill -9"
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.debug("exec %s"%ori_cmd)
    print "test kill host %s chunkserver %s"%(host,up_cs)
    assert rs[3] == 0,"kill chunkserver fail"
    ssh.close()

def start_host_cs_process(host,csid=-1):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    cs_status = get_chunkserver_status(host)
    down_cs = cs_status["down"]
    if down_cs == []:
        return
#    for cs in down_cs:
#        ori_cmd = "sudo nohup curve-chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_sync=true\
#                     -conf=/etc/curve/chunkserver.conf.%d 2>/data/log/chunkserver%d/chunkserver.err &"%(cs,cs)
#        shell_operator.ssh_background_exec(ssh,ori_cmd)
#        logger.debug("exec %s"%ori_cmd)
    if csid == -1:
        ori_cmd = "sudo /home/nbs/chunkserver_ctl.sh start all"
    else:
        if get_cs_copyset_num(host,csid) == 0:
            ori_cmd = "sudo rm -rf /data/chunkserver%d/chunkserver.dat"%(csid)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0
        ori_cmd = "sudo /home/nbs/chunkserver_ctl.sh start %d" %csid
    print "test up host %s chunkserver %s"%(host, down_cs)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"start chunkserver fail,error is %s"%rs[1]
    ssh.close()

def restart_mult_cs_process(host,num):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    for i in range(0, num):
        try:
            cs_status = get_chunkserver_status(host)
            up_cs = cs_status["up"]
            if up_cs == []:
                raise Exception("no chunkserver up")
        except Exception as e:
            logger.error("%s" % e)
            raise AssertionError()
        logger.debug("cs_status is %s" % cs_status)
        cs = random.choice(up_cs)
        ori_cmd = "ps -ef|grep -v grep | grep -w chunkserver%d | awk '{print $2}' && \
        ps -ef|grep -v grep | grep -w /etc/curve/chunkserver.conf.%d |grep -v sudo | awk '{print $2}'" % (cs, cs)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        logger.debug("exec %s" % ori_cmd)
        pid_chunkserver = "".join(rs[1]).strip()
        logger.info("test kill host %s chunkserver %s" % (host, cs))
        kill_cmd = "sudo kill -9 %s" % pid_chunkserver
        rs = shell_operator.ssh_exec(ssh, kill_cmd)
        logger.debug("exec %s,stdout is %s" % (kill_cmd, "".join(rs[2])))
        ori_cmd = "sudo /home/nbs/chunkserver_ctl.sh start %d" % cs
        shell_operator.ssh_exec(ssh, ori_cmd)
        logger.debug("exec %s" % ori_cmd)
        logger.info("test up host %s chunkserver %s" % (host, cs))
        time.sleep(2)
        ori_cmd = "ps -ef|grep -v grep | grep -w chunkserver%d | awk '{print $2}' && \
        ps -ef|grep -v grep | grep -w /etc/curve/chunkserver.conf.%d |grep -v sudo | awk '{print $2}'" % (cs, cs)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] == []:
            assert False, "up chunkserver fail"
        up_cs.remove(cs)

def kill_mds_process(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep | grep -v sudo | grep curve-mds | awk '{print $2}'"
    pids = shell_operator.ssh_exec(ssh, ori_cmd)
    if pids[1] == []:
        logger.debug("mds not up")
        return
    for pid in pids[1]:
        pid = pid.strip()
        kill_cmd = "sudo kill -9 %s"%pid
        rs = shell_operator.ssh_exec(ssh,kill_cmd)
        logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[1])))
        assert rs[3] == 0,"kill mds fail,process is %s"%pid

def start_mds_process(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep | grep curve-mds | awk '{print $2}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] != []:
        logger.debug("mds already up")
        return
    up_cmd = "sudo nohup /usr/bin/curve-mds --confPath=/etc/curve/mds.conf &"
    shell_operator.ssh_background_exec2(ssh, up_cmd)
    logger.debug("exec %s"%(up_cmd))
    time.sleep(2)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] == []:
        assert False, "mds up fail"

def kill_etcd_process(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep  | grep etcd | awk '{print $2}'"
    pids = shell_operator.ssh_exec(ssh, ori_cmd)
    if pids[1] == []:
        logger.debug("etcd not up")
        return
    for pid in pids[1]:
        pid = pid.strip()
        kill_cmd = "sudo kill -9 %s"%pid
        rs = shell_operator.ssh_exec(ssh,kill_cmd)
        logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[1])))
        assert rs[3] == 0,"kill etcd fail"

def start_etcd_process(host):
#    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
#    ori_cmd = "ps -ef|grep -v grep | grep etcd | awk '{print $2}'"
#    rs = shell_operator.ssh_exec(ssh, ori_cmd)
#    if rs[1] != []:
#        logger.debug("etcd already up")
#        return
#    mkdir_cmd = "sudo rm -rf /etcd/default.etcd"
#    rs = shell_operator.ssh_exec(ssh, mkdir_cmd)
#    up_cmd = " cd etcdrun && sudo nohup  ./run.sh existing &"
 #   shell_operator.ssh_background_exec2(ssh, up_cmd)
#    logger.debug("exec %s"%(up_cmd))
#    time.sleep(2)
#    rs = shell_operator.ssh_exec(ssh, ori_cmd)
#    if rs[1] == []:
#        assert False, "etcd up fail"
    try:
       cmd = "ansible-playbook -i curve/curve-ansible/server.ini curve/curve-ansible/start_curve.yml --tags etcd"
       ret = shell_operator.run_exec(cmd)
       assert ret == 0 ,"ansible start etcd fail"
    except Exception:
       logger.error("ansible start etcd fail.")
       raise       
       
def stop_mysql_process(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep | grep mysql"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] == []:
        logger.debug("mysql not up")
        return
    ori_cmd = "sudo killall mysqld"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    logger.debug("exec %s,stdout is %s"%(ori_cmd,"".join(rs[1])))
    assert rs[3] == 0,"stop mysql fail"

def start_mysql_process(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep | grep mysql"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] != []:
        logger.debug("mysql already up")
        return
    start_cmd = "sudo /home/nbs/mysql/Percona-Server-5.7.26-debain9/bin/mysqld_safe --defaults-file=/home/nbs/mysql/my.cnf &"
    rs = shell_operator.ssh_background_exec2(ssh, start_cmd)
#    assert rs[3] == 0,"start mysql fail"
    time.sleep(2)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] == []:
        assert False, "mysql up fail"

def get_cluster_iops():
    return 100

def exec_deleteforce():
    client_list = config.client_list
    host = random.choice(client_list)
    cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 robot/Resources/keywords/deleteforce-test.py %s:~/"%(config.pravie_key_path,host)
    shell_operator.run_exec2(cmd)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "sudo cp ~/deleteforce-test.py /usr/curvefs/"
    shell_operator.ssh_exec(ssh, ori_cmd)
    ori_cmd = "sudo python /usr/curvefs/deleteforce-test.py"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    logger.info("exec deleteforce return is %s"%rs[1])
    assert rs[3] == 0,"rc is %d"%rs[3]
    
def get_all_chunk_num():
    chunkserver_list = config.chunkserver_list
    num = 0
    for host in chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        cs_status = get_chunkserver_status(host)
        cs_list = cs_status["up"] + cs_status["down"]
        for cs in cs_list:
            ori_cmd = "ls /data/chunkserver%d/chunkfilepool/ |wc -l"%cs
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0
            num = num + int("".join(rs[1]).strip())
        logger.info("now num is %d"%(num)) 
    return num


def check_nbd_iops(limit_iops=3000):
    ssh = shell_operator.create_ssh_connect(config.client_list[0],1046, config.abnormal_user)
    ori_cmd = "iostat -d nb0 1 2 |grep nb0 | awk 'END {print $6}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    kb_wrtn = "".join(rs[1]).strip()
    iops = int(kb_wrtn) / int(config.fio_iosize)
    logger.info("now nbd0 iops is %d with 4k randrw"%iops)
    assert iops >= limit_iops,"vm iops not ok,is %d"%iops

def check_chunkserver_online(num=120):
    host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "curve_ops_tool chunkserver-status | grep chunkserver"
    
    starttime = time.time()
    i = 0
    while time.time() - starttime < 300:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[3] != 0:
            logger.debug("get chunkserver status fail,rs is %s"%rs[1])
            time.sleep(10)
            continue
        status = "".join(rs[1]).strip()
        online_num = re.findall(r'(?<=online = )\d+',status)
        logger.info("chunkserver online num is %s"%online_num)
        if int(online_num[0]) != num:
            logger.debug("chunkserver online num is  %s"%online_num)
            time.sleep(10)
        else:
            break
    if int(online_num[0]) != num:
        ori_cmd = "curve_ops_tool chunkserver-list -checkHealth=false -checkCSAlive | grep OFFLINE"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        logger.error("chunkserver offline list is %s"%rs[1])
        assert int(online_num[0]) == num,"chunkserver online num is %s"%online_num

def wait_health_ok():
    host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "curve_ops_tool status | grep \"cluster is\""
    starttime = time.time()
    check = 0
    while time.time() - starttime < config.recover_time:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        health = "".join(rs[1]).strip()
        if health == "cluster is healthy" and rs[3] == 0:
            check = 1
            break
        else:
            ori_cmd2 = "curve_ops_tool copysets-status -detail | grep \"unhealthy copysets statistic\""
            rs2 = shell_operator.ssh_exec(ssh, ori_cmd2)
            health = rs2[1]
            logger.debug("copysets status is %s"%health)
            time.sleep(10)
    assert check == 1,"cluster is not healthy in %d s"%config.recover_time

def rapid_leader_schedule():
    host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "curve_ops_tool check-operator -opName=change_peer | grep \"Operator num is\""
    starttime = time.time()
    check = 0
    while time.time() - starttime < config.recover_time:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        operatorNum = "".join(rs[1]).strip()
        if operatorNum == "Operator num is 0" and rs[3] == 0:
            check = 1
            break
        else:
            ori_cmd2 = "curve_ops_tool check-operator -opName=change_peer"
            rs2 = shell_operator.ssh_exec(ssh, ori_cmd2)
            logger.debug("operator status is %s"%rs2[1])
            time.sleep(10)
    assert check == 1,"change operator num is not 0 in %d s"%config.recover_time
    ori_cmd = "curve_ops_tool rapid-leader-schedule"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"rapid leader schedule not ok"
    ori_cmd = "curve_ops_tool check-operator -opName=transfer_leader -leaderOpInterval=1| grep \"Operator num is\""
    starttime = time.time()
    while time.time() - starttime < 60:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        operatorNum = "".join(rs[1]).strip()
        if operatorNum == "Operator num is 0" and rs[3] == 0:
            break
        else:
            time.sleep(1)

def wait_cluster_healthy(limit_iops=8000):
    check_chunkserver_online()
    host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "curve_ops_tool status | grep \"cluster is\""
    starttime = time.time()
    check = 0
    while time.time() - starttime < config.recover_time:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        health = "".join(rs[1]).strip()
        if health == "cluster is healthy" and rs[3] == 0:
            check = 1
            break
        else:
            time.sleep(30)
    if check != 1:
        ori_cmd2 = "curve_ops_tool status"
        rs2 = shell_operator.ssh_exec(ssh, ori_cmd2)
        cluster_status = "".join(rs2[1]).strip()
        logger.debug("cluster status is %s"%cluster_status)
        ori_cmd2 = "curve_ops_tool copysets-status -detail"
        rs2 = shell_operator.ssh_exec(ssh, ori_cmd2)
        copysets_status = "".join(rs2[1]).strip()
        logger.debug("copysets status is %s"%copysets_status)
        assert check == 1,"cluster is not healthy in %d s,cluster status is:\n %s,copysets status is:\n %s"%(config.recover_time,cluster_status,copysets_status)
    rapid_leader_schedule() 
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    i = 0
    while i < 300:
        ori_cmd = "iostat -d nb0 1 2 |grep nb0 | awk 'END {print $6}'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        kb_wrtn = "".join(rs[1]).strip()
        iops = int(kb_wrtn) / int(config.fio_iosize)
        logger.info("vm iops is %d"%iops)
        if iops >= limit_iops:
            break
        i = i + 2
        time.sleep(2)
    assert iops >= limit_iops,"vm iops not ok in 300s"

def clean_kernel_log():
    for host in config.client_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo logrotate -vf /etc/logrotate.d/rsyslog"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0," rollback log fail, %s"%rs[1]
        ssh.close()

def check_io_error():
    for host in config.client_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        ori_cmd = "sudo grep \'I/O error\' /var/log/kern.log -R"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] != []:
            ori_cmd = "sudo logrotate -vf /etc/logrotate.d/rsyslog"
            shell_operator.ssh_exec(ssh, ori_cmd)
            assert False," rwio error,log is %s"%rs[1]
        ssh.close()

def check_copies_consistency():
    host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmdpri = "curve_ops_tool check-consistency -filename=/fiofile \
                  -check_hash="
    check_hash = "false"
    ori_cmd = ori_cmdpri + check_hash
    i = 0
    try:
        stop_rwio()
        while i < 600:
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            if rs[3] == 0:
                break
            logger.info("check_hash false return is %s,return code is %d"%(rs[1],rs[3]))
            time.sleep(3)
            i = i + 3
        if rs[3] != 0:
            assert False,"exec check_hash false fail,return is %s"%rs[1]
        check_hash = "true"
        ori_cmd = ori_cmdpri + check_hash
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        logger.debug("exec %s,stdout is %s"%(ori_cmd,"".join(rs[1])))
        if rs[3] == 0:
            print "check consistency ok!"
        else:
            message = eval(rs[1][2])
            groupId = message["groupId"]
            chunkID = message["chunkID"]
            hosts = message["hosts"]
            chunkservers = message["chunkservers"]
            for i in range(0,3):
                host = hosts[i]
                chunkserver = chunkservers[i]
                ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
                ori_cmd = "sudo cp /data/%s/copysets/%s/data/chunk_%s /data/log/%s"%(chunkserver,groupId,chunkID,chunkserver)
                rs = shell_operator.ssh_exec(ssh, ori_cmd)
                if rs[3] != 0:
                    logger.error("cp chunk fail,is %s"%rs[1])
            assert False,"checkconsistecny fail,error is %s"%("".join(rs[1]).strip())
#        check_data_consistency()
    except:
        logger.error("check consistency error")
#        run_rwio()
        raise
#    run_rwio()

def check_data_consistency():
    try:
        #wait run 60s io
        #time.sleep(60)
        ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
        ori_cmd = "grep \"Data Validation error\" /home/nbs/output/ -R  && \
                grep \"Data Validation error\" /home/nbs/nohup.out"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] != []:
            t = time.time()
            ori_cmd = "mv /home/nbs/output /home/nbs/vdbench-output/output-%d && mv /home/nbs/nohup.out /home/nbs/nohup-%d"%(int(t),int(t))
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            ori_cmd = "mkdir output && touch nohup.out"
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
#            logger.error("find error in %s"%rs[1])
            assert False,"find data consistency error,save log to vm /root/vdbench-output/output-%d"%int(t)
    except Exception as e:
        ssh.close()
        raise
    ssh.close()

def test_kill_chunkserver_num(num):
    start_iops = get_cluster_iops()
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test kill chunkserver num %d,host %s------|"%(num,chunkserver_host))
    try:
#    check_chunkserver_status(chunkserver_host)
        kill_mult_cs_process(chunkserver_host,num)
        end_iops = get_cluster_iops()
        if float(end_iops)/float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        logger.error("error:%s"%e)
        start_mult_cs_process(chunkserver_host,num)
        raise 
    return chunkserver_host

def test_start_chunkserver_num(num,host=None):
    start_iops = get_cluster_iops()
    if host == None:
       chunkserver_host = random.choice(config.chunkserver_list)
    else:
        chunkserver_host = host
    logger.info("|------begin test start chunkserver num %d,host %s------|"%(num,chunkserver_host))
    try:
        start_mult_cs_process(chunkserver_host,num)
        end_iops = get_cluster_iops()
        if float(end_iops)/float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise 

def test_outcs_recover_copyset():
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test out one chunkserver,host %s------|"%(chunkserver_host))
    try:
        cs_list = kill_mult_cs_process(chunkserver_host,1)
        begin_num = get_cs_copyset_num(chunkserver_host,cs_list[0])
        #time.sleep(config.recover_time)
        i = 0
        time.sleep(5)
        while i < config.recover_time:
            check_nbd_iops()
            i = i + 60
            num = get_cs_copyset_num(chunkserver_host,cs_list[0])
            time.sleep(60)
            if num == 0:
                break
            logger.info("cs copyset num is %d"%num)
        if num != 0:
        #    assert num != 0
            raise Exception("host %s chunkserver %d not recover to 0 in %d,now is %d"%(chunkserver_host,cs_list[0],config.recover_time,num))
    except Exception as e:
#        raise AssertionError()
        logger.error("error is %s"%e)
        cs_list = start_host_cs_process(chunkserver_host,cs_list[0])
        raise
    return chunkserver_host,begin_num

def test_upcs_recover_copyset(host,copyset_num):
    if host == None:
        chunkserver_host = random.choice(config.chunkserver_list)
    else:
        chunkserver_host = host
    logger.info("|------begin test up one chunkserver,host %s------|"%(chunkserver_host))
    try:
        cs_list = start_mult_cs_process(chunkserver_host,1)
        time.sleep(10)
        #time.sleep(config.recover_time)
        i = 0
        while i < config.recover_time:
            check_nbd_iops()
            i = i + 60
            time.sleep(60)
            num = get_cs_copyset_num(chunkserver_host,cs_list[0])
            logger.info("cs copyset num is %d"%num)
            if abs(num - copyset_num) <= 10:
                break
        if abs(num - copyset_num) > 10:
            logger.error("get host %s chunkserver %d copyset num is %d"%(chunkserver_host,cs_list[0],num))
            raise Exception(
                "host %s chunkserver %d not recover to %d in %d,now is %d" % \
            (chunkserver_host, cs_list[0],copyset_num,config.recover_time,num))
    except Exception as e:
        logger.error("error is :%s"%e)
        raise 
    return chunkserver_host

def stop_all_cs_not_recover():
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test stop all chunkserver,host %s------|"%(chunkserver_host))
    try:
        stop_host_cs_process(chunkserver_host)
        list = get_chunkserver_status(chunkserver_host)
        down_list = list["down"]
        dict = {}
        for cs in down_list:
            num = get_cs_copyset_num(chunkserver_host,cs)
            dict[cs] = num
        time.sleep(config.offline_timeout + 10)
        check_nbd_iops()
        for cs in dict:
            num = get_cs_copyset_num(chunkserver_host,cs)
            if num != dict[cs]:
            #    assert num != 0
                raise Exception("stop all chunkserver not recover fail,cs id %d,copysets num from %d to %d" % (cs,dict[cs],num))
    except Exception as e:
        #        raise AssertionError()
        logger.error("error is %s" % e)
        cs_list = start_host_cs_process(chunkserver_host)
        raise
    start_host_cs_process(chunkserver_host)

def pendding_all_cs_recover():
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test pendding all chunkserver,host %s------|"%(chunkserver_host))
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    ssh_mds = shell_operator.create_ssh_connect(config.mds_list[0], 1046, config.abnormal_user)
    try:
        stop_host_cs_process(chunkserver_host)
        list = get_chunkserver_status(chunkserver_host)
        down_list = list["down"]
        csid_list = []
        time.sleep(config.offline_timeout + 60)
        mds = []
        for host in config.mds_list:
            mds.append(host + ":6666")
        mds_addrs = ",".join(mds)
        for cs in down_list:
            chunkserver_id = get_chunkserver_id(chunkserver_host,cs)
            assert chunkserver_id != -1
            csid_list.append(chunkserver_id)
            pendding_cmd = "sudo curve-tool -mds_addr=%s -op=set_chunkserver \
                    -chunkserver_id=%d -chunkserver_status=pendding"%(mds_addrs,chunkserver_id)
            rs = shell_operator.ssh_exec(ssh_mds,pendding_cmd)
            assert rs[3] == 0,"pendding chunkserver %d fail,rs is %s"%(cs,rs)
        time.sleep(180)
        test_kill_mds(2)
        i = 0
        while i < config.recover_time:
            check_nbd_iops()
            i = i + 60
            time.sleep(60)
            for cs in down_list:
                num = get_cs_copyset_num(chunkserver_host,cs)
                if num != 0:
                    break
            if num == 0:
                break
        if num != 0:
            logger.error("exist chunkserver %d copyset %d"%(chunkserver_id,num))
            raise Exception("pendding chunkserver fail")
    except Exception as e:
        #        raise AssertionError()
        logger.error("error is %s" % e)
        test_start_mds()
        cs_list = start_host_cs_process(chunkserver_host)
        raise
    test_start_mds()
    for cs in down_list:
        start_host_cs_process(chunkserver_host,cs)
    time.sleep(60)
    list = get_chunkserver_status(chunkserver_host)
    up_list = list["up"]
    for cs in up_list:
        i = 0
        while i < config.recover_time:
            i = i + 10
            time.sleep(10)
            num = get_cs_copyset_num(chunkserver_host,cs)
            logger.info("cs copyset num is %d"%num)
            if num > 0:
                break
        if num == 0:
            logger.error("get host %s chunkserver %d copyset num is %d"%(chunkserver_host,cs,num))
            raise Exception(
                "host %s chunkserver %d not recover to %d in %d,now is %d" % \
            (chunkserver_host, cs,1,config.recover_time,num))


def test_suspend_recover_copyset():
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test suspend recover,host %s------|"%(chunkserver_host))
    try:
        cs_list = kill_mult_cs_process(chunkserver_host,1)
        begin_num = get_cs_copyset_num(chunkserver_host,cs_list[0])
        #time.sleep(config.recover_time)
        i = 0
        time.sleep(config.offline_timeout - 5)
        while i < config.recover_time:
            check_nbd_iops()
            i = i + 1
            num = get_cs_copyset_num(chunkserver_host,cs_list[0])
            time.sleep(1)
            logger.info("now cs copyset num is %d,begin_num is %d"%(num,begin_num))
            if num > 0 and abs(begin_num - num) > 10 :
                break
            elif num == 0:
               cs_list = start_host_cs_process(chunkserver_host,cs_list[0]) 
               assert False,"copyset is 0"
        start_host_cs_process(chunkserver_host)
        i = 0
        while i < config.recover_time:
            check_nbd_iops()
            i = i + 60
            num = get_cs_copyset_num(chunkserver_host,cs_list[0])
            time.sleep(60)
            logger.info("cs copyset num is %d"%num)
            if abs(num - begin_num) < 10:
                break
        if abs(num - begin_num) > 10:
            raise Exception(
                "host %s chunkserver %d not recover to %d in %d,now is %d" % \
            (chunkserver_host, cs_list[0],begin_num,config.recover_time,num))
    except Exception as e:
#        raise AssertionError()
        logger.error("error is %s"%e)
        cs_list = start_host_cs_process(chunkserver_host)
        raise

def test_suspend_delete_recover_copyset():
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test suspend delete recover,host %s------|"%(chunkserver_host))
    try:
        cs_list = kill_mult_cs_process(chunkserver_host,1)
        begin_num = get_cs_copyset_num(chunkserver_host,cs_list[0])
        #time.sleep(config.recover_time)
        i = 0
        time.sleep(10)
        while i < config.recover_time:
            check_nbd_iops()
            i = i + 1
            num = get_cs_copyset_num(chunkserver_host,cs_list[0])
            time.sleep(1)
            logger.info("now cs copyset num is %d,begin_num is %d"%(num,begin_num))
            if num > 0 and abs(begin_num - num) > 10 :
                break
            elif num == 0:
               cs_list = start_host_cs_process(chunkserver_host,cs_list[0]) 
               assert False,"copyset is 0"
        start_host_cs_process(chunkserver_host,cs_list[0])
        time.sleep(300)
        i = 0
        while i < config.recover_time:
            check_nbd_iops()
            i = i + 60
            num = get_cs_copyset_num(chunkserver_host,cs_list[0])
            time.sleep(60)
            logger.info("cs copyset num is %d"%num)
            if abs(num - begin_num) < 10:
                break
        if abs(num - begin_num) > 10:
            raise Exception(
                "host %s chunkserver %d not recover to %d in %d,now is %d" % \
            (chunkserver_host, cs_list[0],begin_num,config.recover_time,num))
    except Exception as e:
#        raise AssertionError()
        logger.error("error is %s"%e)
        cs_list = start_host_cs_process(chunkserver_host)
        raise

def test_kill_mds(num=1):
    start_iops = get_cluster_iops()
    logger.info("|------begin test kill mds num %d------|"%(num))
    mds_ips = list(config.mds_list)
    try:
        for i in range(0,num):
            mds_host = random.choice(mds_ips)
            logger.info("mds ip is %s"%mds_host)
            kill_mds_process(mds_host)
            end_iops = get_cluster_iops()
            if float(end_iops)/float(start_iops) < 0.9:
                raise Exception("client io is slow, = %d more than 5s" % (end_iops))
            mds_ips.remove(mds_host)
    except Exception as e:
        logger.error("kill mds %s fail"%mds_host)
        raise 
    return mds_host

def test_start_mds():
    start_iops = get_cluster_iops()
    try:
        logger.info("mds list is %s"%config.mds_list)
        for mds_host in config.mds_list:
            start_mds_process(mds_host)
            end_iops = get_cluster_iops()
            if float(end_iops) / float(start_iops) < 0.9:
                raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise 

def test_start_snap():
    start_iops = get_cluster_iops()
    try:
        logger.info("snap list is %s"%config.snap_server_list)
        for snap_host in config.snap_server_list:
            start_snap_process(snap_host)
            end_iops = get_cluster_iops()
            if float(end_iops) / float(start_iops) < 0.9:
                raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise 

def start_snap_process(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep | grep curve-snapshotcloneserver | awk '{print $2}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] != []:
        logger.debug("snap already up")
        return
    up_cmd = "cd snapshot/temp && sudo nohup curve-snapshotcloneserver -conf=/etc/curve/snapshot_clone_server.conf &"
    shell_operator.ssh_background_exec2(ssh, up_cmd)
    logger.debug("exec %s"%(up_cmd))
    time.sleep(2)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] == []:
        assert False, "snap up fail"

def test_round_restart_mds():
    logger.info("|------begin test round restart mds------|")
    start_iops = get_cluster_iops()
    mds_list = list(config.mds_list)
    try:
        for mds_host in mds_list:
            kill_mds_process(mds_host)
            time.sleep(2)
            start_mds_process(mds_host)
            end_iops = get_cluster_iops()
            if float(end_iops)/float(start_iops) < 0.9:
                raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        logger.error("round restart mds %s fail"%mds_host)
        raise

def test_kill_etcd(num=1):
    logger.info("|------begin test kill etcd num %d------|"%(num))
    start_iops = get_cluster_iops()
    etcd_ips = list(config.etcd_list)
    try:
        for i in range(0,num):
            etcd_host = random.choice(etcd_ips)
            logger.info("etcd ip is %s"%etcd_host)
            kill_etcd_process(etcd_host)
            end_iops = get_cluster_iops()
            if float(end_iops)/float(start_iops) < 0.9:
                raise Exception("client io is slow, = %d more than 5s" % (end_iops))
            etcd_ips.remove(etcd_host)
    except Exception as e:
        logger.error("kill etcd %s fail"%etcd_host)
        raise
    return etcd_host

def test_start_etcd():
    start_iops = get_cluster_iops()
    try:
        for etcd_host in config.etcd_list:
            start_etcd_process(etcd_host)
            end_iops = get_cluster_iops()
            if float(end_iops) / float(start_iops) < 0.9:
                raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise 

def test_round_restart_etcd():
    logger.info("|------begin test round restart etcd------|")
    start_iops = get_cluster_iops()
    etcd_list = list(config.etcd_list)
    try:
        for etcd_host in etcd_list:
            kill_etcd_process(etcd_host)
            time.sleep(6)
            start_etcd_process(etcd_host)
            end_iops = get_cluster_iops()
            if float(end_iops)/float(start_iops) < 0.9:
                raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        logger.error("round restart etcd %s fail"%etcd_host)
        raise

def test_kill_mysql():
    logger.info("|------begin test kill mysql------|")
    start_iops = get_cluster_iops()
    mysql_host = random.choice(config.mds_list)
    try:
        stop_mysql_process(mysql_host)
        end_iops = get_cluster_iops()
        if float(end_iops)/float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        start_mysql_process(mysql_host)
        raise
    return mysql_host

def test_start_mysql(host):
    start_iops = get_cluster_iops()
    mysql_host = host
    try:
        start_mysql_process(mysql_host)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise

def test_stop_chunkserver_host():
    start_iops = get_cluster_iops()
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test stop chunkserver host %s------|"%(chunkserver_host))
    try:
        stop_host_cs_process(chunkserver_host)
        end_iops = get_cluster_iops()
        if float(end_iops)/float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        start_host_cs_process(chunkserver_host)
        raise e
    return chunkserver_host

def test_start_chunkserver_host(host=None):
    start_iops = get_cluster_iops()
    if host == None:
       chunkserver_host = random.choice(config.chunkserver_list)
    else:
        chunkserver_host = host
    try:
        start_host_cs_process(chunkserver_host)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise e

def test_restart_chunkserver_num(num):
    start_iops = get_cluster_iops()
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test restart chunkserver num %d,host %s------|"%(num,chunkserver_host))
    try:
        restart_mult_cs_process(chunkserver_host,num)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise e

def stop_scheduler():
    ssh = shell_operator.create_ssh_connect(config.mds_list[0], 1046, config.abnormal_user)
    for mds_host in config.mds_list:
        logger.info("|------begin stop copyset scheduler %s------|"%(mds_host))
        cmd = "curl -L %s:6666/flags/enableCopySetScheduler?setvalue=false"%mds_host
        rs = shell_operator.ssh_exec(ssh,cmd)
    time.sleep(180)

def test_start_all_chunkserver():
    start_iops = get_cluster_iops()
    try:
        for chunkserver_host in config.chunkserver_list:
           start_host_cs_process(chunkserver_host)
           end_iops = get_cluster_iops()
           if float(end_iops) / float(start_iops) < 0.9:
               raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise e

def test_stop_all_chunkserver():
    start_iops = get_cluster_iops()
    logger.info("|------begin test stop all chunkserver------|")
    try:
        for chunkserver_host in config.chunkserver_list:
            stop_host_cs_process(chunkserver_host)
            end_iops = get_cluster_iops()
            if float(end_iops)/float(start_iops) < 0.9:
               raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        test_start_all_chunkserver()
        raise e

def test_kill_diff_host_chunkserver():
    start_iops = get_cluster_iops()
    chunkserver_list = list(config.chunkserver_list)
    chunkserver_host1 = random.choice(chunkserver_list)
    chunkserver_list.remove(chunkserver_host1)
    chunkserver_host2 = random.choice(chunkserver_list)
    logger.info("|------begin test kill diff host chunkserver,host1 %s,host2 %s------|"%(chunkserver_host1,chunkserver_host2))
    try:
        kill_mult_cs_process(chunkserver_host1, 1)
        kill_mult_cs_process(chunkserver_host2, 1)
        time.sleep(5)
    # io hang ....

        end_iops = get_cluster_iops()
        check_nbd_iops(0)
     #   logger.error("kill diff host chunkserver,end iops is %d"%(end_iops))
     #   if float(end_iops) / float(start_iops) < 0.9:
     #   raise Exception("client io is slow, = %d more than 5s" % (end_iops))
     #   assert False
    except Exception as e:
        raise e
    finally:
        start_mult_cs_process(chunkserver_host1, 1)
        start_mult_cs_process(chunkserver_host2, 1)

def test_reboot_nebd():
    client_host = random.choice(config.client_list)
    logger.info("|------begin test reboot nebd %s------|"%(client_host))
    cmd = "sudo nebd-daemon restart"
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"reboot nebd daemon fail,return is %s"%rs[1]

def test_cs_loss_package(percent):
    start_iops = get_cluster_iops()
    chunkserver_list = config.chunkserver_list
    chunkserver_host = random.choice(chunkserver_list)
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    dev = get_hostip_dev(ssh,chunkserver_host)
    logger.info("|------begin test host %s dev %s loss package------|"%(chunkserver_host,dev))
    try:
        package_loss_all(ssh, dev, percent)
        show_tc_inject(ssh,dev)
        check_nbd_iops(1)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.1:
            raise Exception("client io slow op more than 5s")
    except Exception as e:
        raise       
    finally:
        time.sleep(60)
        cancel_tc_inject(ssh,dev)

def test_mds_loss_package(percent):
    start_iops = get_cluster_iops()
    mds_list = config.mds_list
    mds_host = random.choice(mds_list)
    ssh = shell_operator.create_ssh_connect(mds_host, 1046, config.abnormal_user)
    dev = get_hostip_dev(ssh,mds_host)
    logger.info("|------begin test host %s dev %s loss package------|"%(mds_host,dev))
    try:
        package_loss_all(ssh, dev, percent)
        show_tc_inject(ssh,dev)
        check_nbd_iops(1)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.1:
            raise Exception("client io slow op more than 5s")
    except Exception as e:
        raise
    finally:
        time.sleep(60)
        cancel_tc_inject(ssh,dev)

def test_cs_delay_package(ms):
    start_iops = get_cluster_iops()
    chunkserver_list = config.chunkserver_list
    chunkserver_host = random.choice(chunkserver_list)
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    dev = get_hostip_dev(ssh,chunkserver_host)
    logger.info("|------begin test host %s dev %s delay package------|"%(chunkserver_host,dev))
    try:
        package_delay_all(ssh, dev, ms)
        show_tc_inject(ssh,dev)
        check_nbd_iops(1)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.1:
            raise Exception("client io slow op more than 5s")
    except Exception as e:
        raise
    finally:
        time.sleep(60)
        cancel_tc_inject(ssh,dev)

def test_mds_delay_package(ms):
    start_iops = get_cluster_iops()
    mds_list = config.mds_list
    mds_host = random.choice(mds_list)
    ssh = shell_operator.create_ssh_connect(mds_host, 1046, config.abnormal_user)
    dev = get_hostip_dev(ssh,mds_host)
    logger.info("|------begin test host %s dev %s delay package------|"%(mds_host,dev))
    try:
        package_delay_all(ssh, dev, ms)
        show_tc_inject(ssh,dev)
#        check_nbd_iops(1)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.1:
            raise Exception("client io slow op more than 5s")
    except Exception as e:
        raise
    finally:
        time.sleep(60)
        cancel_tc_inject(ssh,dev)

def test_chunkserver_cpu_stress(stress=50):
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test chunkserver cpu stress,host %s------|"%(chunkserver_host))
    cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 robot/Resources/keywords/cpu_stress.py \
     %s:~/"%(config.pravie_key_path,chunkserver_host)
    shell_operator.run_exec2(cmd)
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    inject_cpu_stress(ssh,stress)
    return ssh
 
def test_mds_cpu_stress(stress=50):
    mds_host = random.choice(config.mds_list)
    logger.info("|------begin test mds cpu stress,host %s------|"%(mds_host))
    cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 robot/Resources/keywords/cpu_stress.py \
     %s:~/"%(config.pravie_key_path,mds_host)
    shell_operator.run_exec2(cmd)
    ssh = shell_operator.create_ssh_connect(mds_host, 1046, config.abnormal_user)
    inject_cpu_stress(ssh,stress)
    return ssh

def test_client_cpu_stress(stress=50):
#    client_host = random.choice(config.client_list)
    client_host = config.client_list[0]
    logger.info("|------begin test client cpu stress,host %s------|"%(client_host))
    cmd = "scp -i %s -o StrictHostKeyChecking=no -P 1046 robot/Resources/keywords/cpu_stress.py \
     %s:~/"%(config.pravie_key_path,client_host)
    shell_operator.run_exec2(cmd)
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    inject_cpu_stress(ssh,stress)
    return ssh

def test_chunkserver_mem_stress(stress=50):
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test chunkserver mem stress,host %s------|"%(chunkserver_host))
    cmd = "free -g |grep Mem|awk \'{print $2}\'"
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    all_mem = int("".join(rs[1]).strip())
    stress = all_mem * stress / 100
    inject_mem_stress(ssh,stress)
    return ssh

def test_mds_mem_stress(stress=50):
    mds_host = random.choice(config.mds_list)
    logger.info("|------begin test mds mem stress,host %s------|"%(mds_host))
    cmd = "free -g |grep Mem|awk \'{print $2}\'"
    ssh = shell_operator.create_ssh_connect(mds_host, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    all_mem = int("".join(rs[1]).strip())
    stress = all_mem * stress / 100
    inject_mem_stress(ssh,stress)
    return ssh

def test_client_mem_stress(stress=50):
    client_host = config.client_list[0]
    logger.info("|------begin test client mem stress,host %s------|"%(client_host))
    cmd = "free -g |grep Mem|awk \'{print $2}\'"
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    all_mem = int("".join(rs[1]).strip())
    stress = all_mem * stress / 100
    inject_mem_stress(ssh,stress)
    return ssh

def test_chunkserver_network_stress():
    chunkserver_host = random.choice(config.chunkserver_list)
    logger.info("|------begin test chunkserver network stress,host %s------|"%(chunkserver_host))
    t1 = mythread.runThread(listen_network_stress, chunkserver_host)
    t2 = mythread.runThread(inject_network_stress, chunkserver_host)
    t1.start()
    time.sleep(3)
    t2.start()
    return chunkserver_host

def test_mds_network_stress():
    mds_host = random.choice(config.mds_list)
    logger.info("|------begin test mds network stress,host %s------|"%(mds_host))
    t1 = mythread.runThread(listen_network_stress, mds_host)
    t2 = mythread.runThread(inject_network_stress, mds_host)
    t1.start()
    time.sleep(3)
    t2.start()
    return mds_host

def test_client_network_stress():
    client_host = config.client_list[0]
    logger.info("|------begin test client network stress,host %s------|"%(client_host))
    t1 = mythread.runThread(listen_network_stress, client_host)
    t2 = mythread.runThread(inject_network_stress, client_host)
    t1.start()
    time.sleep(3)
    t2.start()
    return client_host

def test_chunkserver_clock_offset(offset):
    chunkserver_host = random.choice(config.chunkserver_list)
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    inject_clock_offset(ssh,offset)
    return ssh

def test_mds_clock_offset(offset):
    mds_host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(mds_host, 1046, config.abnormal_user)
    inject_clock_offset(ssh,offset)
    return ssh

#使用cycle会从掉电到上电有１秒钟的间隔
def test_ipmitool_restart_chunkserver():
    chunkserver_host = random.choice(config.chunkserver_reset_list)
    logger.info("|------begin test chunkserver ipmitool cycle,host %s------|"%(chunkserver_host))
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    ipmitool_cycle_restart_host(ssh)
    time.sleep(60)
    starttime = time.time()
    i = 0
    while time.time() - starttime < 600:
        status = check_host_connect(chunkserver_host)
        if status == True:
            break
        else:
            logger.debug("wait host up")
            time.sleep(5)
    assert status,"restart host %s fail"%chunkserver_host
    start_host_cs_process(chunkserver_host)

def test_ipmitool_restart_client():
    client_host = config.client_list[1]
    logger.info("|------begin test client ipmitool cycle,host %s------|"%(client_host))
    ssh = shell_operator.create_ssh_connect(client_host, 1046, config.abnormal_user)
    ipmitool_cycle_restart_host(ssh)
    time.sleep(60)
    starttime = time.time()
    i = 0
    while time.time() - starttime < 600:
        status = check_host_connect(client_host)
        if status == True:
            break
        else:
            logger.debug("wait host up")
            time.sleep(5)
    assert status,"restart host %s fail"%client_host

#使用reset从掉电到上电没有间隔
def test_ipmitool_reset_chunkserver():
    chunkserver_host = random.choice(config.chunkserver_reset_list)
    logger.info("|------begin test chunkserver ipmitool reset,host %s------|"%(chunkserver_host))
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    ipmitool_reset_restart_host(ssh)
    time.sleep(60)
    starttime = time.time()
    i = 0
    while time.time() - starttime < 600:
        status = check_host_connect(chunkserver_host)
        if status == True:
            break
        else:
            logger.debug("wait host up")
            time.sleep(5)
    assert status,"restart host %s fail"%chunkserver_host
    start_host_cs_process(chunkserver_host)

def test_ipmitool_restart_mds():
    mds_host = random.choice(config.mds_reset_list)
    logger.info("|------begin test mds ipmitool cycle,host %s------|"%(mds_host))
    ssh = shell_operator.create_ssh_connect(mds_host, 1046, config.abnormal_user)
    ipmitool_cycle_restart_host(ssh)
    time.sleep(60)
    starttime = time.time()
    i = 0
    while time.time() - starttime < 600:
        status = check_host_connect(mds_host)
        if status == True:
            break
        else:
            logger.debug("wait host up")
            time.sleep(5)
    assert status,"restart host %s fail"%mds_host
    start_mds_process(mds_host)
    start_etcd_process(mds_host)
    start_host_cs_process(mds_host)

def clean_last_data():
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    ori_cmd = "rm /root/perf/test-ssd/fiodata/* && rm /root/perf/test-ssd/cfg/*"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    #assert rs[3] == 0,"rm fail"
    ori_cmd = "rm /root/perf/fiodata -rf"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)

def analysis_data(ssh):
    ori_cmd = "cd /root/perf/ && python gen_randrw_data.py"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"gen randrw data fail,error is %s"%rs[1]
    ori_cmd = "cat /root/perf/test.csv"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"get data fail,error is %s"%rs[1]
    for line in rs[1]:
        if 'randread,4k' in line:
            randr_4k_iops = line.split(',')[4]
        elif 'randwrite,4k' in line:
            randw_4k_iops = line.split(',')[8]
        elif 'write,512k' in line: 
            write_512k_iops = line.split(',')[8]
        elif 'read,512k' in line:
            read_512k_iops = line.split(',')[4]
    randr_4k_iops = float(randr_4k_iops)*1000
    randw_4k_iops = float(randw_4k_iops)*1000
    read_512k_BW = float(read_512k_iops)*1000/2
    write_512k_BW = float(write_512k_iops)*1000/2
    logger.info("get one volume Basic data:-------------------------------")
    logger.info("4k rand read iops is %d/s"%int(randr_4k_iops))
    logger.info("4k rand write iops is %d/s"%int(randw_4k_iops))
    logger.info("512k read BW is %d MB/s"%int(read_512k_BW))
    logger.info("512k write BW is %d MB/s"%int(write_512k_BW))
    filename = "onevolume_perf.txt"
    with open(filename,'w') as f:
        f.write("4k randwrite %d/s 56000\n"%int(randw_4k_iops))
        f.write("4k randread %d/s 75000\n"%int(randr_4k_iops))
        f.write("512k  write %dMB/s 135\n"%int(write_512k_BW))
        f.write("512k  read %dMB/s 450\n"%int(read_512k_BW))
    if randr_4k_iops < 75000:
        assert float(75000 - randr_4k_iops)/75000 < 0.02,"4k_randr_iops did not meet expectations,expect more than 75000"
    if randw_4k_iops < 56000:
        assert float(56000 - randw_4k_iops)/56000 < 0.02,"4k_randw_iops did not meet expectations,expect more than 56000"    
    if read_512k_BW < 450:
        assert float(450 - read_512k_BW)/450 < 0.02,"512k_read_bw did not meet expectations,expect more than 450"
    if write_512k_BW < 135:
        assert float(135 - write_512k_BW)/135 < 0.02,"512k_write_bw did not meet expectations,expect more than 135"

def perf_test():
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    ori_cmd = "supervisorctl stop all"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    time.sleep(5)
    clean_last_data()
    init_io = "fio -name=/dev/vdc -direct=1 -iodepth=128 -rw=randrw  -ioengine=libaio \
        -bs=4k -size=200G  -runtime=300 -numjobs=1 -time_based"
    shell_operator.ssh_exec(ssh, init_io)
    start_test = "cd /root/perf && nohup python /root/perf/io_test.py &"
    shell_operator.ssh_background_exec2(ssh,start_test)
    time.sleep(60)
    final = 0
    starttime = time.time()
    while time.time() - starttime < 3600:
        ori_cmd = "ps -ef|grep -v grep |grep io_test.py"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] == []:
            final = 1
            break
        else:
            logger.debug("wait io test finally")
            time.sleep(60)
    assert final == 1,"io test have not finall"
    ori_cmd = "cp -r /root/perf/test-ssd/fiodata /root/perf"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"cp fiodata fail,error is %s"%rs[1]
    analysis_data(ssh)

def add_data_disk():
    ori_cmd = "bash attach_thrash.sh"
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"attach thrash vol fail,rs is %s"%rs[1]
    ori_cmd = "cat thrash_vm"
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("rs is %s"%rs[1])
    vm_list = []
    for i in rs[1]:
       logger.info("uuid is %s"%i)
       vm_list.append(i.strip())
    vm_ip_list = []
    for vm in vm_list:
        ori_cmd = "source OPENRC && nova list|grep %s"%vm
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        ret = "".join(rs[1]).strip()
        ip = re.findall(r'\d+\.\d+\.\d+\.\d+',ret)
        logger.info("get vm %s ip %s"%(vm,ip))
        vm_ip_list.append(ip[0])
    ssh.close()
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    for ip in vm_ip_list:
        ori_cmd = "ssh %s -o StrictHostKeyChecking=no "%ip + "\"" + " supervisorctl reload && supervisorctl start all " + "\""
        logger.info("exec cmd %s" % ori_cmd)
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        assert rs[3] == 0,"start supervisor fail,rs is %s"%rs[1]
    ssh.close()


def create_vm_image(vm_name):
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%(vm_name)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("vm uuid is %s" % rs[1])
    thrash_vm_uuid = "".join(rs[1]).strip()
    ori_cmd = "source OPENRC && nova image-create %s image-%s"%(thrash_vm_uuid,vm_name)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"create vm %s image fail"%(thrash_vm_uuid)
    starttime = time.time()
    ori_cmd = "source OPENRC && nova image-list|grep image-%s|awk '{print $6}'"%vm_name
    while time.time() - starttime < 600:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if "".join(rs[1]).strip() == "ACTIVE":
            break
        elif "".join(rs[1]).strip() == "ERROR":
            assert False,"create vm image image-%s fail"%(vm_name)
        else:
            time.sleep(10)
    if "".join(rs[1]).strip() != "ACTIVE":
        assert False,"wait image create image-%s fail"%(vm_name)
    ori_cmd = "source OPENRC && nova image-list|grep image-%s|awk '{print $2}'"%vm_name
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    return "".join(rs[1]).strip()

def get_all_curvevm_active_num(num):
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    starttime = time.time()
    while time.time() - starttime < 600:
        ori_cmd = "source OPENRC && nova list |grep %s | grep ACTIVE | wc -l"%config.vm_prefix
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        assert rs[3] == 0,"get vm status fail"
        if int("".join(rs[1]).strip()) == num:
            break
        else:
            time.sleep(10)
    active_num = "".join(rs[1]).strip()
    ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%config.vm_prefix
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"get vm uuid fail"
    for uuid in rs[1]:
        uuid = uuid.strip()
        status = "up"
        cmd = "source OPENRC && nova show %s |grep os-server-status |awk \'{print $4}\'" % uuid
        st = shell_operator.ssh_exec(ssh, cmd)
        status = "".join(st[1]).strip()
        assert status == "up","get vm status fail,not up.is %s,current vm id is %s"%(status,uuid)
    return active_num

def init_create_curve_vm(num):
    image_id = config.image_id
    salt = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    logger.info("vm name is thrash-%s"%salt)
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    ori_cmd = "source OPENRC && nova boot --flavor 400 --image %s --vnc-password 000000  --availability-zone %s \
            --key-name  cyh  --nic vpc-net=ff89c80a-585d-4b19-992a-462f4d2ddd27:77a410be-1cf4-4992-8894-0c0bc67f5e48 \
            --meta use-vpc=True --meta instance_image_type=curve thrash-%s"%(config.image_id,config.avail_zone,salt)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"create vm fail,return is %s"%rs[1]
    vm_name = "thrash-%s"%salt
    starttime = time.time()
    ori_cmd = "source OPENRC && nova list|grep %s|awk '{print $6}'"%vm_name
    while time.time() - starttime < 600:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if "".join(rs[1]).strip() == "ACTIVE":
            break
        elif "".join(rs[1]).strip() == "ERROR":
            assert False,"create vm %s fail"%(vm_name)
        else:
            time.sleep(10)
    if "".join(rs[1]).strip() != "ACTIVE":
        assert False,"wait vm ok %s fail"%(vm_name)
    new_image_id = create_vm_image(vm_name)
    config.vm_prefix = vm_name
    for i in range(1,num):
        ori_cmd = "source OPENRC && nova boot --flavor 400 --image %s --vnc-password 000000  --availability-zone %s \
            --key-name  cyh  --nic vpc-net=ff89c80a-585d-4b19-992a-462f4d2ddd27:77a410be-1cf4-4992-8894-0c0bc67f5e48 \
            --meta use-vpc=True --meta instance_image_type=curve thrash-%s-%d"%(new_image_id,config.avail_zone,salt,i)
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        assert rs[3] == 0,"create vm fail,return is %s"%rs[1]
    starttime = time.time()
    while time.time() - starttime < 300:
        active_num = int(get_all_curvevm_active_num(num))
        if active_num == num:
            logger.info("all vm is active")
            break
        else:
            time.sleep(10)
    assert active_num == num,"some vm are abnormal,%d is acitve"%active_num

def reboot_curve_vm():
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    ori_cmd = "vm=`source OPENRC && nova list |grep %s |awk '{print $2}'`;source OPENRC;for i in $vm;do nova reboot $i;done "%config.vm_prefix
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"reboot curve vm fail"

def clean_curve_data():
    ori_cmd = "bash detach_thrash.sh"
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"detach thrash vol fail,rs is %s"%rs[1]
    ori_cmd = "vm=`source OPENRC && nova list|grep %s | awk '{print $2}'`;source OPENRC;for i in $vm;do nova delete $i;done"%config.vm_prefix
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"delete vm fail,rs is %s"%rs[1]
    ori_cmd = "source OPENRC && nova image-list |grep image-%s | awk '{print $2}'"%config.vm_prefix
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    image_id = "".join(rs[1]).strip()
    ori_cmd = "source OPENRC && nova image-delete %s"%(image_id)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    assert rs[3] == 0,"delete image fail,rs is %s"%rs
    time.sleep(30)
    ori_cmd = "curve_ops_tool list -fileName=/nova |grep Total"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if "".join(rs[1]).strip() == "Total file number: 0":
        return True
    else:
        ori_cmd = "curve_ops_tool list -fileName=/nova"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        logger.error("No deleted files: %s"%rs[1])
        assert  False,"vm or image not be deleted"

def do_thrasher(action):
    #start level1
    if type(action) is types.StringType:
        logger.debug("开始启动故障XXXXXXXXXXXXXXXXXXX %s XXXXXXXXXXXXXXXXXXXXXXXXX"%action)
        globals()[action]()
    else:
        logger.debug("开始启动故障XXXXXXXXXXXXXXXXXXX %s,%s XXXXXXXXXXXXXXXXXXXXXX"%(action[0],str(action[1])))
        globals()[action[0]](action[1])

def start_retired_and_down_chunkservers():
    for host in config.chunkserver_list:
        ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
        try:
           cs_status = get_chunkserver_status(host)
           down_cs = cs_status["down"]
           if down_cs == []:
               continue
           logger.debug("down_cs is %s"%down_cs)
           for cs in down_cs:
               if  get_cs_copyset_num(host,cs) == 0:
                   ori_cmd = "sudo rm -rf /data/chunkserver%d/chunkserver.dat"%(cs)
                   rs = shell_operator.ssh_exec(ssh, ori_cmd)
                   assert rs[3] == 0,"rm chunkserver%d chunkserver.dat fail"%cs
               ori_cmd = "sudo /home/nbs/chunkserver_ctl.sh start %d"%cs
               logger.debug("exec %s"%ori_cmd)
               rs = shell_operator.ssh_exec(ssh,ori_cmd)
               assert rs[3] == 0,"start chunkserver fail,error is %s"%rs[1]
               time.sleep(2)
               ori_cmd = "ps -ef|grep -v grep | grep -w chunkserver%d | awk '{print $2}' && \
               ps -ef|grep -v grep | grep -w /etc/curve/chunkserver.conf.%d |grep -v sudo | awk '{print $2}'" % (cs, cs)
               rs = shell_operator.ssh_exec(ssh,ori_cmd)
               if rs[1] == []:
                   assert False,"up chunkserver fail"
        except:
            raise
        ssh.close()

def get_level_list(level):
    if level == "level1":
        return config.level1
    elif level == "level2":
        return config.level2
    elif level == "level3":
        return config.level3
