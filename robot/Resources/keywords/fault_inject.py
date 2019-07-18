#!/usr/bin/env python
# -*- coding: utf8 -*-

import subprocess
from config import config
from logger import logger
from lib import shell_operator
import random
import time
from lib import db_operator
import threading
import random
import time
import mythread

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
    assert rs[3] == 0
    # rc = shell_operator.run_exec(cmd)

def package_delay_all(ssh, dev,ms):
    ori_cmd = "sudo tc qdisc add dev %s root netem delay %dms" % (dev, ms)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0
    # rc = shell_operator.run_exec(cmd)

def cancel_tc_inject(ssh,dev):
    ori_cmd = "sudo tc qdisc del dev %s root" % dev
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0
    # rc = shell_operator.run_exec(cmd)

def show_tc_inject(ssh,dev):
    ori_cmd = "sudo tc qdisc show dev %s " % dev
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0
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

def get_hostip_dev(ssh,hostip):
    ori_cmd = "ip a|grep %s | awk '{print $7}'"%hostip
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0
    return "".join(rs[1]).strip()

def remove_vm_key():
    cmd = "ssh-keygen -f ~/.ssh/known_hosts -R %s"%config.vm_host
    shell_operator.run_exec(cmd)
    print cmd

def attach_new_vol(fio_size,vdbench_size):
    ori_cmd = "bash curve_test.sh create %d %d"%(int(fio_size),int(vdbench_size))
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"attach vol fail,return is %s"%rs[1]
    logger.info("exec cmd %s"%ori_cmd)
    get_vol_uuid()
    ssh.close()

def detach_vol():
    stop_rwio()
    ori_cmd = "bash curve_test.sh delete"
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"retcode is %d"%rs[3]
    logger.info("exec cmd %s"%ori_cmd)
    ssh.close()

def get_vol_uuid():
    ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%config.vm_host
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    ori_cmd = "source OPENRC && cinder list |grep %s |grep thrash-fio | awk '{print $2}'"%("".join(rs[1]).strip())
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    vol_uuid = "".join(rs[1]).strip() 
    assert vol_uuid != "","get vol uuid fail"
    config.vol_uuid = vol_uuid
    ssh.close()

def stop_rwio():
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    ori_cmd = "supervisorctl stop all"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"stop rwio fail"
    ori_cmd = "ps -ef|grep -v grep | grep -w /root/vdbench50406/profile | awk '{print $2}'| xargs kill -9"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    time.sleep(3)
    ssh.close()

def run_rwio():
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    ori_cmd =  "lsblk |grep vdc | awk '{print $1}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    output = "".join(rs[1]).strip()
    if output != "vdc":
        logger.error("attach is error")
        assert  False,"output is %s"%output
    ori_cmd =  "lsblk |grep vdd | awk '{print $1}'"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    output = "".join(rs[1]).strip()
    if output != "vdd":
        logger.error("attach is error")
        assert  False,"output is %s"%output
    ori_cmd = "supervisorctl stop all && supervisorctl reload"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    ori_cmd = "nohup /root/vdbench50406/vdbench -jn -f /root/vdbench50406/profile &"
    rs = shell_operator.ssh_background_exec2(ssh, ori_cmd)
    #write 60s io
    time.sleep(60)
#    assert rs[3] == 0,"start rwio fail"
    ssh.close()

def write_full_disk(fio_size):
    ori_cmd = "fio -name=/dev/vdc -direct=1 -iodepth=32 -rw=write -ioengine=libaio -bs=1024k -size=%dG -numjobs=1 -time_based"%int(fio_size)
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"write fio fail"
   
def get_chunkserver_id(host,cs_id):
    conn = db_operator.conn_db(config.abnormal_db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
    sql = R"select * from curve_chunkserver where `internalHostIP` like '%s' and `mountPoint` like 'local:///data/chunkserver%d/' and `rwstatus` like 0;;"%(host,cs_id)
    chunkserver = db_operator.query_db(conn, sql)
    if chunkserver["rowcount"] == 1:
        chunkserver_id = chunkserver["data"][0]["chunkServerID"]
        logger.info("operator chunkserver id is %d"%chunkserver_id)
    else:
#            assert False,"get chunkserver id fail,retun is %s"%(chunkserver)
        return -1

    return int(chunkserver_id)

def get_cs_copyset_num(chunkserver_id):
    conn = db_operator.conn_db(config.abnormal_db_host, config.db_port, config.db_user, config.db_pass, config.mds_db_name)
    try:
        sql = R"select * from curve_copyset where chunkServerIDList REGEXP '\n\t%d,|,\n\t%d,|,\n\t%d\n';"\
                %(chunkserver_id,chunkserver_id,chunkserver_id)
        cs_copyset_info = db_operator.query_db(conn, sql)
#        logger.debug("get table row is %s"%cs_copyset_info["rowcount"])
#        logger.debug("get table %s" %(cs_copyset_info))
    except Exception:
        logger.error("get db fail.")
        raise
    return int(cs_copyset_info["rowcount"])


def stop_vm(ssh,uuid):
    stop_cmd = "source OPENRC && nova stop %s"%uuid
    rs = shell_operator.ssh_exec(ssh, stop_cmd)
    assert rs[3] == 0,"stop vm fail"
    time.sleep(5)

def start_vm(ssh,uuid):
    start_cmd = "source OPENRC && nova start %s"%uuid
    rs = shell_operator.ssh_exec(ssh, start_cmd)
    assert rs[3] == 0,"start vm fail"

def restart_vm(ssh,uuid):
    restart_cmd = "source OPENRC && nova reboot %s"%uuid
    rs = shell_operator.ssh_exec(ssh, restart_cmd)
    assert rs[3] == 0,"reboot vm fail"

def check_vm_status(ssh,uuid):
    ori_cmd = "source OPENRC && nova list|grep %s|awk '{print $6}'"%uuid
    i = 0
    while i < 180:
       rs = shell_operator.ssh_exec(ssh, ori_cmd)
       if "".join(rs[1]).strip() == "ACTIVE":
           return True
       else:
           time.sleep(5)
           i = i + 5
    assert False,"start vm fail"

def get_chunkserver_status(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
#    grep_cmd = "ls -l /etc/curve/chunkserver.conf.* | grep -v example | awk '{print $9}'"
    grep_cmd = "bash /home/nbs/chunkserver_status.sh"
    rs = shell_operator.ssh_exec(ssh,grep_cmd)
    chunkserver_lines = rs[1]
    #......only test usi
    logger.debug("get lines is %s"%chunkserver_lines)
    up_cs = [int(i.split()[0][11:]) for i in filter(lambda x: "active" in x, chunkserver_lines)]
    down_cs = [int(i.split()[0][11:]) for i in filter(lambda x: "down" in x, chunkserver_lines)]
#    chunkserver_lines = []
 #  do not use new chunkserver
#    grep_cmd = "ls -l /etc/curve/chunkserver.conf.* | grep -v example | awk '{print $9}'"
#    rs = shell_operator.ssh_exec(ssh, grep_cmd)
#    chunkserver_lines = rs[1]
#    for i in range(0,9):
#        chunkserver_lines.append("/etc/curve/chunkserver.conf.%d\n"%i)
    #........
#    logger.debug("set lines is %s"%chunkserver_lines)
 #   grep_cmd = "ps -ef | grep curve-chunkserver | grep -v grep  |grep -v sudo | awk '{print $12}' | cut -d = -f 2"
#    rs = shell_operator.ssh_exec(ssh,grep_cmd)
#    cs_up = rs[1]
#    logger.debug("cs_up is %s"%cs_up)
#    up_cs = [int(i[28:])for i in filter(lambda x: x in cs_up,chunkserver_lines)]
#    down_cs = [int(i[28:])for i in filter(lambda x: x not in cs_up,chunkserver_lines)]
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
 #       ori_cmd = "ps -ef|grep -v grep | grep -w /etc/curve/chunkserver.conf.%d |grep -v sudo | awk '{print $2}'"%cs
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
        id = get_chunkserver_id(host,cs)
        if id == -1 and get_cs_copyset_num(id) == 0:
            ori_cmd = "sudo rm -rf /data/chunkserver%d/chunkserver.dat;sudo rm -rf /data/chunkserver%d/copysets;\
             sudo rm -rf /data/chunkserver%d/recycler"%(cs,cs,cs)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0
 #       ori_cmd = "sudo nohup curve-chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_sync=true\
 #               -conf=/etc/curve/chunkserver.conf.%d 2>/data/log/chunkserver%d/chunkserver.err &"%(cs,cs)
        ori_cmd = "sudo /home/nbs/chunkserver_start.sh %d %s 8200  &"%(cs,host)
        logger.debug("exec %s"%ori_cmd)
        shell_operator.ssh_background_exec2(ssh,ori_cmd)
        #logger.info("test up host %s chunkserver %s,retun is %s"%(host,cs,rs[1]))
     #   assert rs[3] == 0,"exec %s fail,return is %d"%(ori_cmd,rs[3])
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
            id = get_chunkserver_id(host,cs)
            if id == -1 and get_cs_copyset_num(id) == 0:
                ori_cmd = "sudo rm -rf /data/chunkserver%d/chunkserver.dat;sudo rm -rf /data/chunkserver%d/copysets;\
                sudo rm -rf /data/chunkserver%d/recycler"%(cs,cs,cs)
                rs = shell_operator.ssh_exec(ssh, ori_cmd)
                assert rs[3] == 0
            ori_cmd = "sudo /home/nbs/chunkserver_start.sh %d %s 8200 > /dev/null 2>&1 &"%(cs,host)
            logger.debug("exec %s"%ori_cmd)
            shell_operator.ssh_background_exec(ssh,ori_cmd)
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
        ori_cmd = "sudo nohup /home/nbs/chunkserver_start.sh all %s 8200 &"%host
    else:
        id = get_chunkserver_id(host,csid)
        if id == -1 and get_cs_copyset_num(id) == 0:
            ori_cmd = "sudo rm -rf /data/chunkserver%d/chunkserver.dat;sudo rm -rf /data/chunkserver%d/copysets;\
             sudo rm -rf /data/chunkserver%d/recycler"%(cs,cs,cs)
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0
        ori_cmd = "sudo nohup /home/nbs/chunkserver_start.sh %d %s 8200 &"%(csid,host)
    print "test up host %s chunkserver %s"%(host, down_cs)
    shell_operator.ssh_background_exec2(ssh,ori_cmd)
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
        ori_cmd = "sudo /home/nbs/chunkserver_start.sh %d %s 8200 > /dev/null 2>&1 &" % (cs, host)
        shell_operator.ssh_background_exec(ssh, ori_cmd)
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
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    if rs[1] == []:
        logger.debug("mds not up")
        return
    pid = "".join(rs[1]).strip()
    kill_cmd = "sudo kill -9 %s"%pid
    rs = shell_operator.ssh_exec(ssh,kill_cmd)
    logger.debug("exec %s,stdout is %s"%(kill_cmd,"".join(rs[1])))
    assert rs[3] == 0,"kill mds fail"

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

def get_cluster_iops():
    return 100

def exec_deleteforce():
    mds_list = config.mds_list
    host = random.choice(mds_list)
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
        for cs in cs_status["up"]:
            ori_cmd = "ls /data/chunkserver%d/chunkfilepool/ |wc -l"%cs
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            assert rs[3] == 0
            num = num + int("".join(rs[1]).strip())
        logger.info("now num is %d"%(num)) 
    return num


def check_vm_iops(limit_iops=2000):
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    ori_cmd = "iostat -d vdc 1 2 |grep vdc | awk 'END {print $6}'"
    time.sleep(5)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    kb_wrtn = "".join(rs[1]).strip()
    iops = int(kb_wrtn) / int(config.vm_iosize)
    logger.info("now vm vdc iops is %d with 4k randrw"%iops)
    assert iops >= limit_iops,"vm iops not ok,is %d"%iops

def wait_iops_ok(limit_iops=8000):
    ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
    i = 0
    while i < 300:
        ori_cmd = "iostat -d vdc 1 2 |grep vdc | awk 'END {print $6}'"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        kb_wrtn = "".join(rs[1]).strip()
        iops = int(kb_wrtn) / int(config.vm_iosize)
        if iops >= limit_iops:
            break
        i = i + 2
        time.sleep(2)
    assert iops >= limit_iops,"vm iops not ok in 300s"

def check_copies_consistency():
    host = random.choice(config.mds_list)
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    if config.vol_uuid == "":
        assert False,"not get vol uuid"
    filename = "volume-" + config.vol_uuid
    ori_cmdpri = "checkConsistecny  -config_path=/etc/curve/client.conf -filename=/cinder/%s \
            -chunksize=16777216 -filesize=10737418240 -segmentsize=1073741824 -username=cinder -check_hash="%(filename)
    check_hash = "false"
    ori_cmd = ori_cmdpri + check_hash
    i = 0
    try:
        stop_rwio()
        while i < 600:
            rs = shell_operator.ssh_exec(ssh, ori_cmd)
            if rs[1] == []:
                break
            logger.info("check_hash false return is %s,return code is %d"%(rs[1],rs[3]))
            time.sleep(3)
            i = i + 3
        if rs[1] != []:
            assert False,"exec check_hash false fail,return is %s"%rs[1]
        check_hash = "true"
        ori_cmd = ori_cmdpri + check_hash
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        logger.debug("exec %s,stdout is %s"%(ori_cmd,"".join(rs[1])))
        assert rs[1] == [],"checkconsistecny fail,error is %s"%("".join(rs[1]).strip())
#        check_data_consistency()
    except:
        logger.error("check consistency error")
        run_rwio()
        raise
    run_rwio()

def check_data_consistency():
    try:
        #wait run 60s io
        time.sleep(60)
        ssh = shell_operator.create_ssh_connect(config.vm_host, 22, config.vm_user)
        ori_cmd = "grep \"Data Validation error\" /root/output/ -R  && \
                grep \"Data Validation error\" /root/nohup.out"
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        if rs[1] != []:
            t = time.time()
            ori_cmd = "mv /root/output /root/vdbench-output/output-%d && mv /root/nohup.out /root/nohup-%d"%(int(t),int(t))
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
    try:
#    check_chunkserver_status(chunkserver_host)
        kill_mult_cs_process(chunkserver_host,num)
        time.sleep(5)
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
    try:
        start_mult_cs_process(chunkserver_host,num)
        time.sleep(5)
        end_iops = get_cluster_iops()
        if float(end_iops)/float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise 

def test_outcs_recover_copyset():
    chunkserver_host = random.choice(config.chunkserver_list)
    try:
        cs_list = kill_mult_cs_process(chunkserver_host,1)
        chunkserver_id = get_chunkserver_id(chunkserver_host,cs_list[0])
        assert chunkserver_id != -1
        begin_num = get_cs_copyset_num(chunkserver_id)
        #time.sleep(config.recover_time)
        i = 0
        time.sleep(5)
        while i < config.recover_time:
            i = i + 60
            num = get_cs_copyset_num(chunkserver_id)
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
    try:
        cs_list = start_mult_cs_process(chunkserver_host,1)
        time.sleep(65)
        chunkserver_id = get_chunkserver_id(chunkserver_host,cs_list[0])
        assert chunkserver_id != -1,"mysql can not find chunkserver"
        #time.sleep(config.recover_time)
        i = 0
        while i < config.recover_time:
            i = i + 60
            time.sleep(60)
            num = get_cs_copyset_num(chunkserver_id)
            logger.info("cs copyset num is %d"%num)
            if abs(num - copyset_num) <= 10:
                break
        if abs(num - copyset_num) > 10:
            raise Exception(
                "host %s chunkserver %d not recover to %d in %d,now is %d" % \
            (chunkserver_host, cs_list[0],copyset_num,config.recover_time,num))
        logger.error("get host %s chunkserver %d copyset num is %d"%(chunkserver_host,cs_list[0],num))
    except Exception as e:
        logger.error("error is :%s"%e)
        raise 
    return chunkserver_host

def stop_all_cs_not_recover():
    chunkserver_host = random.choice(config.chunkserver_list)
    try:
        stop_host_cs_process(chunkserver_host)
        list = get_chunkserver_status(chunkserver_host)
        down_list = list["down"]
        dict = {}
        for cs in down_list:
            chunkserver_id = get_chunkserver_id(chunkserver_host,cs)
            assert chunkserver_id != -1
            num = get_cs_copyset_num(chunkserver_id)
            dict[chunkserver_id] = num
        time.sleep(config.offline_timeout + 10)
        for cs in dict:
            num = get_cs_copyset_num(cs)
            if num != dict[cs]:
            #    assert num != 0
                raise Exception("stop all cs not recover fail,cs id %d" % (cs))
    except Exception as e:
        #        raise AssertionError()
        logger.error("error is %s" % e)
        cs_list = start_host_cs_process(chunkserver_host)
        raise
    start_host_cs_process(chunkserver_host)

def test_suspend_recover_copyset():
    chunkserver_host = random.choice(config.chunkserver_list)
    try:
        cs_list = kill_mult_cs_process(chunkserver_host,1)
        chunkserver_id = get_chunkserver_id(chunkserver_host,cs_list[0])
        assert chunkserver_id != -1
        begin_num = get_cs_copyset_num(chunkserver_id)
        #time.sleep(config.recover_time)
        i = 0
        time.sleep(5)
        while i < config.recover_time:
            i = i + 5
            num = get_cs_copyset_num(chunkserver_id)
            time.sleep(5)
            logger.info("now cs copyset num is %d,begin_num is %d"%(num,begin_num))
            if num > 0 and num != begin_num :
                break
        start_host_cs_process(chunkserver_host,cs_list[0])
        i = 0
        while i < config.recover_time:
            i = i + 60
            num = get_cs_copyset_num(chunkserver_id)
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

def test_kill_mds():
    start_iops = get_cluster_iops()
    mds_host = random.choice(config.mds_list)
    try:
        kill_mds_process(mds_host)
#    start_mds_process(mds_host)
        time.sleep(5)
        end_iops = get_cluster_iops()
        if float(end_iops)/float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        start_mds_process(mds_host)
        raise 
    return mds_host

def test_start_mds():
    start_iops = get_cluster_iops()
    mds_host = random.choice(config.mds_list)
    try:
        kill_mds_process(mds_host)
        time.sleep(30)
        start_mds_process(mds_host)
        time.sleep(5)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise 

def test_stop_chunkserver_host():
    start_iops = get_cluster_iops()
    chunkserver_host = random.choice(config.chunkserver_list)
    try:
        stop_host_cs_process(chunkserver_host)
        time.sleep(5)
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
        time.sleep(5)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise e

def test_restart_chunkserver_num(num):
    start_iops = get_cluster_iops()
    chunkserver_host = random.choice(config.chunkserver_list)
    try:
        restart_mult_cs_process(chunkserver_host,num)
        time.sleep(5)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow, = %d more than 5s" % (end_iops))
    except Exception as e:
        raise e


def test_kill_diff_host_chunkserver():
    start_iops = get_cluster_iops()
    chunkserver_list = config.chunkserver_list
    chunkserver_host1 = random.choice(chunkserver_list)
    chunkserver_list.remove(chunkserver_host1)
    chunkserver_host2 = random.choice(chunkserver_list)
    try:
        kill_mult_cs_process(chunkserver_host1, 1)
        kill_mult_cs_process(chunkserver_host2, 1)
        time.sleep(5)
    # io hang ....

        end_iops = get_cluster_iops()
        check_vm_iops(0)
     #   logger.error("kill diff host chunkserver,end iops is %d"%(end_iops))
     #   if float(end_iops) / float(start_iops) < 0.9:
     #   raise Exception("client io is slow, = %d more than 5s" % (end_iops))
     #   assert False
    except Exception as e:
        raise e
    finally:
        start_mult_cs_process(chunkserver_host1, 1)
        start_mult_cs_process(chunkserver_host2, 1)

def test_start_vm():
    start_iops = get_cluster_iops()
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    ori_cmd = "source OPENRC && nova list|grep %s | awk '{print $2}'"%config.vm_host
    try:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        logger.debug("exec %s" % ori_cmd)
        uuid = "".join(rs[1]).strip()
        stop_vm(ssh,uuid)
        time.sleep(30)
        start_vm(ssh,uuid)
        check_vm_status(ssh,uuid)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow = %d"%(end_iops))
    except Exception as e:
        raise
    
def test_restart_vm():
    start_iops = get_cluster_iops()
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    ori_cmd = "source OPENRC && nova list|grep %s | awk '{print $2}'"%config.vm_host
    try:
        rs = shell_operator.ssh_exec(ssh, ori_cmd)
        logger.debug("exec %s" % ori_cmd)
        uuid = "".join(rs[1]).strip()
        restart_vm(ssh,uuid)
        time.sleep(60)
        check_vm_status(ssh,uuid)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.9:
            raise Exception("client io is slow = %d"%(end_iops))
    except Exception as e:
         raise

def test_cs_loss_package(percent):
    start_iops = get_cluster_iops()
    chunkserver_list = config.chunkserver_list
    chunkserver_host = random.choice(chunkserver_list)
    ssh = shell_operator.create_ssh_connect(chunkserver_host, 1046, config.abnormal_user)
    dev = get_hostip_dev(ssh,chunkserver_host)
    try:
        package_loss_all(ssh, dev, percent)
        show_tc_inject(ssh,dev)
        time.sleep(5)
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
    try:
        package_loss_all(ssh, dev, percent)
        show_tc_inject(ssh,dev)
        time.sleep(5)
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
    try:
        package_delay_all(ssh, dev, ms)
        show_tc_inject(ssh,dev)
        time.sleep(5)
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
    try:
        package_delay_all(ssh, dev, ms)
        show_tc_inject(ssh,dev)
        time.sleep(5)
        end_iops = get_cluster_iops()
        if float(end_iops) / float(start_iops) < 0.1:
            raise Exception("client io slow op more than 5s")
    except Exception as e:
        raise
    finally:
        time.sleep(60)
        cancel_tc_inject(ssh,dev)

def thrasher_abnormal_cluster():
    actions = []
    actions.append((test_kill_chunkserver_num,1.0,))
    actions.append((test_outcs_recover_copyset,0,))
    actions.append((stop_all_cs_not_recover,1.0,))
    actions.append((test_suspend_recover_copyset,1.0,))
    actions.append((test_kill_mds,1.0,))

