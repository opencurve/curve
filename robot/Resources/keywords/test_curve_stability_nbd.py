# -*- coding: utf8 -*-
#/usr/bin/env python


import os, time, random, re
from multiprocessing import Process, Pool
import datetime
from config import config
from logger.logger import *
from lib import shell_operator
import mythread
import requests
#import base_operate
import json
import hashlib
import curvefs
import fault_inject

size_list = [100, 200, 400, 1000]

class NbdThrash:
    def __init__(self,ssh,name):
        self.name = name
        self.ssh = ssh
        self.user = "test"
        self.dev = ""
        self.write_full_vol = ""

    def nbd_create(self,vol_size):
        stripeUnit = [524288,1048576,2097152,4194304]
        stripeCount = [4,8,16]
        unit = random.choice(stripeUnit)
        count = random.choice(stripeCount)
        cmd = "curve bs create file --path /%s --user test --size %s --stripeunit %d \
                --stripecount %d"%(self.name,vol_size,unit,count)
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        assert rs[3] == 0,"create vol fail：%s"%rs[1]

    def nbd_map(self,dev=None):
        cmd = "/home/nbs/.curveadm/bin/curveadm map test:/%s --host %s -c /home/nbs/bs-client.yaml"%(self.name,config.client_list[0])
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        assert rs[3] == 0,"map vol fail：%s"%rs[1]

    def nbd_getdev(self):
        cmd = "/home/nbs/.curveadm/bin/curveadm client status | grep %s | awk '{print $4}'"%self.name
        rs = shell_operator.ssh_exec(self.ssh,cmd)
        assert rs[3] == 0,"get client docker id fail"
        docker_id = "".join(rs[1]).strip()
        cmd = "sudo docker exec -i %s curve-nbd list-mapped |grep %s | awk '{print $3}'"%(docker_id,self.name)
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        self.dev = "".join(rs[1]).strip()
        logger3.info("map %s to %s"%(self.name,self.dev))
        assert rs[3] == 0,"map fail：%s"%rs[1]

    def nbd_unmap(self):
        if self.dev != "":
            cmd = "/home/nbs/.curveadm/bin/curveadm unmap test:/%s --host %s"%(self.name,config.client_list[0])
            rs = shell_operator.ssh_exec(self.ssh, cmd)
            time.sleep(3)
            assert rs[3] == 0,"unmap fail,error is %s"%rs[1]
        else:
            assert False,"can not find nbd"
    
    def nbd_delete(self):
        cmd = "curve bs delete file --path /%s --user test"%self.name
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        assert rs[3] == 0,"delete /%s fail：%s"%(self.name,rs[1])

    def write_data(self,size):
        ori_cmd = "sudo fio -name=%s -direct=1 -iodepth=8 -rw=randwrite -ioengine=libaio -bs=64k -size=%dM -numjobs=1 -time_based"%(self.dev,size)
        rs = shell_operator.ssh_exec(self.ssh, ori_cmd)
        assert rs[3] == 0,"write fio fail"

    def write_data_full(self,size):
        ori_cmd = "sudo fio -name=%s -direct=1 -iodepth=8 -rw=write -ioengine=libaio -bs=1024k -size=%dM -numjobs=1 -time_based"%(self.dev,size)
        rs = shell_operator.ssh_exec(self.ssh, ori_cmd)
        assert rs[3] == 0,"write fio fail"

def nbd_all(name):
    thrash_time = 0
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    thrash = NbdThrash(ssh,name)
    vol_size = random.choice(size_list)
    thrash.nbd_create(vol_size)
    while config.thrash_map == True:
        try:
            thrash.nbd_map()
            time.sleep(5)
            thrash.nbd_getdev()
            thrash.nbd_unmap()
            time.sleep(15)
            thrash_time = thrash_time + 1
            logger3.info("thrash_map_unmap time is %d,filename is %s"%(thrash_time,name))
        except:
            thrash.nbd_delete()
            logger3.error("map/unmap nbd %s fail"%name)
            config.thrash_map = False
            raise
    else:
        thrash.nbd_delete()
        return thrash_time


def create_vol_snapshot(vol_name,snapshot_name='snap1'):
    cmd = "curve bs create snapshot --snappath /%s@%s --user test"%(vol_name,snapshot_name)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    logger3.info("create return is:%s"%rs[1])
    assert rs[3] == 0,"create snapshot %s fail"%vol_name
    ssh.close()

def delete_vol_snapshot(vol_name,snapshot_name):
    cmd = "curve bs delete snapshot --snappath /%s@%s --user test"%(vol_name,snapshot_name)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    logger3.info("delete return is:%s"%rs[1])
    assert rs[3] == 0,"delete snapshot %s fail"%vol_name
    ssh.close()

def list_vol_snapshot(vol_name,snapshot_name=""):
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    if snapshot_name:
        cmd = "curve bs list snapshot --path /%s --user test |grep %s"%(vol_name,snapshot_name)
        rs = shell_operator.ssh_exec(ssh, cmd)
    else:
        cmd = "curve bs list snapshot --path /%s --user test | grep %s"%(vol_name,vol_name)
        rs = shell_operator.ssh_exec(ssh, cmd)
    ssh.close()
    if rs[1] == []:
        return False
    else:
        return rs[1]

def clone_vol_snapshot(vol_name,snapshot_name,clone_name):
    cmd = "curve bs clone --snappath /%s@%s --dstpath /%s --user test"%(vol_name,snapshot_name,clone_name)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    logger3.info("clone return is:%s"%rs[1])
    assert rs[3] == 0,"clone vol %s fail"%clone_name
    ssh.close()

def flatten_clone_vol(clone_name):
    cmd = "curve bs flatten --path /%s --user test"%(clone_name)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    logger3.info("flatten clone return is:%s"%rs[1])
    assert rs[3] == 0,"flatten clone vol %s fail"%clone_name
    ssh.close()

def protect_snapshot(vol_name,snapshot_name):
    cmd = "curve bs protect --snappath /%s@%s --user test"%(vol_name,snapshot_name)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    logger3.info("protect snapshot return is:%s"%rs[1])
    assert rs[3] == 0,"protect vol %s fail"%vol_name
    ssh.close()

def unprotect_snapshot(vol_name,snapshot_name):
    cmd = "curve bs unprotect --snappath /%s@%s --user test"%(vol_name,snapshot_name)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh, cmd)
    logger3.info("unprotect snapshot return is:%s"%rs[1])
    assert rs[3] == 0,"unprotect vol %s fail"%vol_name
    ssh.close()

def list_clone_vol(vol_name,clone_name=''):
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    if clone_name:
        cmd = "curve bs children --path /%s --user test | grep %s"%(vol_name,clone_name)
        rs = shell_operator.ssh_exec(ssh, cmd)
    else:
        cmd = "curve bs children --path /%s --user test |grep %s"%(vol_name,vol_name)
        rs = shell_operator.ssh_exec(ssh, cmd)
    if rs[1] == []:
        return False
    return rs[1]
    ssh.close()

def delete_vol(vol):
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    cmd = "curve bs delete file --path /%s --user test"%vol
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"delete /%s fail：%s"%(vol,rs[1])
    ssh.close()

def get_vol_md5(vol_uuid):
    cbd = curvefs.CBDClient()
    rc = cbd.Init("./client.conf")
    if rc != 0:
        raise AssertionError
    filename = "/" + vol_uuid
    user = curvefs.UserInfo_t()
    user.owner = "test"
#    user.password = ""
    logger2.info("file name is %s,type is %s"%(filename,type(filename)))
    logger2.info("user is %s,type is %s"%(user,type(user)))
    fd = cbd.Open(str(filename), user)
    buf = ''
    md5_obj = hashlib.md5()
    for i in range(1,2560):
        j = i - 1
        context = cbd.Read(fd, buf, 4096*1024*j,4096*1024)
        md5_obj.update(context)
    hash_code = md5_obj.hexdigest()
    md5 = str(hash_code).lower()
    cbd.Close(fd)
    cbd.UnInit()
    logger2.info("md5 is %s"%md5)
    return md5

def check_clone_vol_exist(clonevol_uuid):
    cbd = curvefs.CBDClient()
    rc = cbd.Init("./client.conf")
    if rc != 0:
        raise AssertionError
    filename = clonevol_uuid
    user = curvefs.UserInfo_t()
    user.owner = "root"
    user.password = "root_password"
    dirs = cbd.Listdir("/", user)
    for d in dirs:
        logger2.info("dir is %s,filename is %s"%(d,filename))
        if d == filename:
            cbd.UnInit()
            return True
    cbd.UnInit()
    return False

def diff_vol_consistency(vol_uuid,clone_uuid):
    context1 = get_vol_md5(vol_uuid)
    context2 = get_vol_md5(clone_uuid)
    if context1 != context2:
        logger2.error("check md5 consistency fail ,vol is %s,clone vol is %s"%(vol_uuid,clone_uuid))
#    if config.snapshot_thrash.check_md5 == True:
#   have added clean chunk feature 
        assert context1 == context2,"vol and clone vol not same"
    else:
        logger2.info("md5 is consistent")
        return True

def init_nbd_vol(write_full_vol=True):
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    try:
        name = "volume-snapshot" + "-" + str(write_full_vol)
        thrash = NbdThrash(ssh,name)
        if write_full_vol == True:
            vol_size = 10 #GB
        else:
            vol_size = 20
        thrash.nbd_create(vol_size)
#        nbd_dev = "nbd3"
        time.sleep(3)
        thrash.nbd_map()
        time.sleep(5)
        thrash.nbd_getdev()
        init_data = 10 * 1024
        thrash.write_data_full(init_data)
        time.sleep(60)
        config.snapshot_volid = name
        config.snapshot_thrash = thrash
        thrash.write_full_vol = write_full_vol
    except:
        logger2.error("create snapshot nbd fail")
        raise

def check_snapshot_delete(vol_id,snapshot_id):
    starttime = time.time()
    final = False
    while time.time() - starttime < 90:
        rc = list_vol_snapshot(vol_id,snapshot_id)
        logger2.info("rc is %s"%rc)
        if rc == False:
            final = True
            break
        else:
           time.sleep(10)
    if final == True:
        return True
    else:
        assert False,"delete snapshot fail in 30s,rc is %s"%rc

def check_clone_delete(clone_id):
    starttime = time.time()
    final = False
    while time.time() - starttime < 120:
        rc = list_clone_vol(clone_id)
        logger2.info("rc is %s"%rc)
        if rc == False:
            final = True
            break
        else:
           time.sleep(10)
    if final == True:
        return True
    else:
        assert False,"clean clone fail in 120s,rc is %s"%rc


def test_clone_iovol_consistency():
    logger2.info("------------begin test clone iovol consistency----------")
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    vol_id = config.snapshot_volid
    snapshot_name = "snap1"
    try:
        create_vol_snapshot(vol_id)
        time.sleep(2)
        rc = list_vol_snapshot(vol_id,snapshot_name)
        assert rc != False,"creat snapshot %s fail"%snapshot_name
        protect_snapshot(vol_id,snapshot_name)
        clone_vol = vol_id + '-' + snapshot_name
        time.sleep(2)
        clone_vol_snapshot(vol_id,snapshot_name,clone_vol)
        rc = list_clone_vol(vol_id,clone_vol)
        assert rc != False,"creat clone vol %s fail"%clone_vol
        time.sleep(2)
        diff_vol_consistency(vol_id,clone_vol)
        thrash = NbdThrash(ssh,clone_vol)
        thrash.nbd_map()
        thrash.nbd_getdev()
        data_size = 2000 #MB
        thrash.write_data(data_size)
        flatten_clone_vol(clone_vol)
        unprotect_snapshot(vol_id,snapshot_name)
        time.sleep(2)
        thrash.write_data(data_size)
        time.sleep(2)
        delete_vol_snapshot(vol_id,snapshot_name)
        time.sleep(2)
        thrash.write_data(data_size)
        time.sleep(2)
        thrash.nbd_unmap()
        time.sleep(2)
        thrash.nbd_delete()
    except:
        raise
    finally:
        rc = check_snapshot_delete(vol_id,snapshot_name)
        assert rc == True,"delete snapshot %s fail"%snapshot_name
        rc = check_clone_delete(vol_id)
        assert rc == True,"delete clone vol %s fail"%clone_vol

def test_clone_chain_consistency():
    logger2.info("------------begin test clone iovol consistency----------")
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    vol_id = config.snapshot_volid
    try:
        src_vol = vol_id
        snapshot_name = "snap"
        for i in range(0,5):
            create_vol_snapshot(src_vol,snapshot_name)
            time.sleep(2)
            rc = list_vol_snapshot(src_vol,snapshot_name)
            assert rc != False,"creat snapshot %s fail"%snapshot_name
            protect_snapshot(src_vol,snapshot_name)
            clone_vol = vol_id + '-' + str(i)
            time.sleep(2)
            clone_vol_snapshot(src_vol,snapshot_name,clone_vol)
            rc = list_clone_vol(src_vol,clone_vol)
            assert rc != False,"creat clone vol %s fail"%clone_vol
            time.sleep(2)
            src_vol = clone_vol
        diff_vol_consistency(vol_id,clone_vol)
        thrash = NbdThrash(ssh,clone_vol)
        thrash.nbd_map()
        thrash.nbd_getdev()
        data_size = 2000
        thrash.write_data(data_size)
        flatten_clone_vol(clone_vol)
        thrash.nbd_unmap()
        time.sleep(2)
        thrash.nbd_delete()
        # 删除克隆卷和快照
        for i in range(3, -1, -1):
            src_vol = vol_id + '-' + str(i)
            unprotect_snapshot(src_vol, snapshot_name)
            delete_vol_snapshot(src_vol, snapshot_name)
            time.sleep(2)
            check_snapshot_delete(src_vol,snapshot_name)
            delete_vol(src_vol)
            check_clone_delete(src_vol)
        unprotect_snapshot(vol_id, snapshot_name)
        delete_vol_snapshot(vol_id, snapshot_name)
    except:
        raise
    finally:
        rc = check_snapshot_delete(vol_id,snapshot_name)
        assert rc == True,"delete snapshot %s fail"%snapshot_name
        rc = check_clone_delete(vol_id)
        assert rc == True,"delete clone vol %s fail"%src_vol


#测试原卷读写过程中创建大量快照
def test_create_many_snapshot():
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    vol_id = 'vdbenchfile'
    snapshot_name = "vdbenchsnap-"
    try:
        for i in range(1,100):
            new_snapshot_name = snapshot_name + str(i)
            create_vol_snapshot(vol_id,new_snapshot_name)
            time.sleep(2)
            rc = list_vol_snapshot(vol_id,new_snapshot_name)
            assert rc != False,"creat snapshot %s fail"%new_snapshot_name
            protect_snapshot(vol_id,new_snapshot_name)
        for i in range(1,100):
            new_snapshot_name = snapshot_name + str(i)
            unprotect_snapshot(vol_id,new_snapshot_name)
            time.sleep(2)
            delete_vol_snapshot(vol_id,new_snapshot_name)
        time.sleep(120)
        rc = list_vol_snapshot(vol_id)
        assert rc == False,"delete snapshot %s fail"%snapshot_name
        fault_inject.check_data_consistency()
    except:
        raise

def test_clone_vol_from_file():
    logger2.info("------------begin test clone vol from file ----------")
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    vol_id = config.snapshot_volid
    destination = vol_id + "-" + lazy
    clone_vol_uuid = clone_vol_from_file(vol_id,lazy)
    time.sleep(1)
    if lazy == "true":
        rc = check_clone_vol_exist(destination)
        assert rc,"clone vol volume-%s not create ok in 2s"%destination
    starttime = time.time()
    status = 0
    final = False
    if lazy == "true":
        status = 7
    while time.time() - starttime < config.snapshot_timeout:
        rc = get_clone_status(clone_vol_uuid)
        if rc["TaskStatus"] == status and rc["TaskType"] == 0:
            final = True
            break
        else:
           time.sleep(60)
    if final == True:
        time.sleep(3)
        try:
           diff_vol_consistency(vol_id,destination)
           if lazy == "true":
               flatten_clone_vol(clone_vol_uuid)
               starttime = time.time()
               final = False
               while time.time() - starttime < config.snapshot_timeout:
                   rc = get_clone_status(clone_vol_uuid)
                   if rc["TaskStatus"] == 0 and rc["TaskType"] == 0:
                       final = True
                       break
                   else:
                      time.sleep(60)
        except:
           raise
        finally:
           clean_vol_clone(clone_vol_uuid)
           check_clone_clean(clone_vol_uuid)
           unlink_clone_vol(destination)
    else:
       assert False,"clone vol fail,status is %s"%rc

def clone_file_and_check(vol_id,destination,lazy):
    logger2.info("------------begin clone vol and check lazy=%s----------"%lazy)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    clone_vol_uuid = clone_file(vol_id,destination,lazy)
    time.sleep(1)
    if lazy == "true":
        rc = check_clone_vol_exist(destination)
        assert rc,"clone vol volume-%s not create ok in 2s"%destination
    starttime = time.time()
    status = 0
    final = False
    if lazy == "true":
        status = 7
    while time.time() - starttime < config.snapshot_timeout:
        rc = get_clone_status(clone_vol_uuid)
        if rc["TaskStatus"] == status and rc["TaskType"] == 0:
            final = True
            break
        else:
           time.sleep(60)
    if final == True:
        time.sleep(3)
        try:
           diff_vol_consistency(vol_id,destination)
           if lazy == "true":
               flatten_clone_vol(clone_vol_uuid)
               starttime = time.time()
               final = False
               while time.time() - starttime < config.snapshot_timeout:
                   rc = get_clone_status(clone_vol_uuid)
                   if rc["TaskStatus"] == 0 and rc["TaskType"] == 0:
                       final = True
                       break
                   else:
                      time.sleep(60)
        except:
           raise
    else:
       assert False,"clone vol fail,status is %s"%rc

def test_clone_vol_same_uuid(lazy):
    file_name = config.snapshot_volid
    destination1 = file_name + "clone1"
    #test create->delete->create
    clone_file_and_check(file_name,destination1,lazy)
    unlink_clone_vol(destination1)
    clone_file_and_check(file_name,destination1,lazy)
    #different source 
    destination2 = file_name + "clone2"
    clone_file_and_check(file_name,destination2,lazy)
    unlink_clone_vol(destination2)
    clone_file_and_check(destination1,destination2,lazy)
    unlink_clone_vol(destination2)
    unlink_clone_vol(destination1)
    clone_file_and_check(file_name,destination2,lazy)
    unlink_clone_vol(destination2)

def test_cancel_snapshot():
    logger2.info("------------begin test cancel snapshot----------")
    config.snapshot_thrash.write_data(1000)
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    vol_id = config.snapshot_volid
    snapshot_uuid = create_vol_snapshot(vol_id)
    starttime = time.time()
    time.sleep(20)
    cancel_vol_snapshot(vol_id,snapshot_uuid)
    starttime = time.time()
    final = False
    time.sleep(5)
    check_snapshot_delete(vol_id,snapshot_uuid)

def test_recover_snapshot(lazy="true"):
    logger2.info("------------begin test recover snapshot----------")
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    vol_id = config.snapshot_volid
    snapshot_uuid = create_vol_snapshot(vol_id)
    starttime = time.time()
    final = False
    time.sleep(5)
    while time.time() - starttime < config.snapshot_timeout:
        rc = get_snapshot_status(vol_id,snapshot_uuid)
        if rc["Progress"] == 100 and rc["Status"] == 0 :
            final = True
            break
        else:
           time.sleep(60)
    if final == True:
        try:
            first_md5 = get_vol_md5(vol_id)
            config.snapshot_thrash.write_data(8000)
            config.snapshot_thrash.nbd_unmap()
            recover_task = recover_snapshot(vol_id,snapshot_uuid,lazy)
            final_recover = False
            time.sleep(5)
            starttime = time.time()
            status = 0
            if lazy == "true":
               second_md5 = get_vol_md5(vol_id)
               assert first_md5 == second_md5,"vol md5 not same after lazy recover,fisrt is %s,recovered is %s"(first_md5,second_md5)
               flatten_clone_vol(recover_task)
               starttime = time.time()
               final = False
               while time.time() - starttime < config.snapshot_timeout:
                   rc = get_clone_status(recover_task)
                   if rc["TaskStatus"] == 0 and rc["TaskType"] == 1:
                       final_recover = True
                       break
                   else:
                      time.sleep(60)
            else:
               final_recover = False
               starttime = time.time()
               while time.time() - starttime < config.snapshot_timeout:
                   rc = get_clone_status(recover_task)
                   if rc["TaskStatus"] == 0 and rc["TaskType"] == 1:
                      final_recover = True
                      break
                   else:
                      time.sleep(60)
            if final_recover == True:
                second_md5 = get_vol_md5(vol_id)
            else:
                assert False,"recover vol %s fail in %d s"%(vol_id,config.snapshot_timeout)
            if config.snapshot_thrash.check_md5 == True:
                assert first_md5 == second_md5,"vol md5 not same after recover,fisrt is %s,recovered is %s"(first_md5,second_md5)
            config.snapshot_thrash.nbd_map()
            config.snapshot_thrash.nbd_getdev()
        except:
            raise
    delete_vol_snapshot(vol_id,snapshot_uuid)
    check_snapshot_delete(vol_id,snapshot_uuid)

def kill_snapshotclone_server(host):
    ssh = shell_operator.create_ssh_connect(host, 1046, config.abnormal_user)
    ori_cmd = "ps -ef|grep -v grep |grep -v sudo | grep snapshotcloneserver | awk '{print $2}' | sudo xargs kill -9"
    rs = shell_operator.ssh_exec(ssh, ori_cmd)

def random_kill_snapshot():
    snap_server = random.choice(config.snap_server_list)
    logger2.info("begin to kill snapshotserver %s"%(snap_server))
    kill_snapshotclone_server(snap_server)
    time.sleep(10)
    ssh = shell_operator.create_ssh_connect(snap_server, 1046, config.abnormal_user)
    ori_cmd = "cd snapshot/temp && sudo nohup curve-snapshotcloneserver -conf=/etc/curve/snapshot_clone_server.conf &"
    shell_operator.ssh_background_exec2(ssh, ori_cmd)

#test bug: https://ep.netease.com/v2/my_workbench/bugdetail/Bug-3835
def test_lazy_clone_flatten_snapshot_fail():
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    try:
        lazy = "true"
        name = "volume-flatten"
        thrash = NbdThrash(ssh,name)
        vol_size = 10
        thrash.nbd_create(vol_size)
        thrash.nbd_map()
        time.sleep(5)
        thrash.nbd_getdev()
        write_size = 500
        thrash.write_data(write_size)
        time.sleep(60)
        vol_id = name
        destination = vol_id + "-" + lazy
        clone_vol_uuid = clone_vol_from_file(vol_id,lazy)
        time.sleep(1)
        if lazy == "true":
            rc = check_clone_vol_exist(destination)
            assert rc,"clone vol volume-%s not create ok in 2s"%destination
        starttime = time.time()
        status = 0
        final = False
        if lazy == "true":
            status = 7
        while time.time() - starttime < config.snapshot_timeout:
            rc = get_clone_status(clone_vol_uuid)
            if rc["TaskStatus"] == status and rc["TaskType"] == 0:
                final = True
                break
            else:
               time.sleep(60)
        if final == True:
            try:
               if lazy == "true":
                   flatten_clone_vol(clone_vol_uuid)
                   starttime = time.time()
                   final = False
                   while time.time() - starttime < config.snapshot_timeout:
                       rc = get_clone_status(clone_vol_uuid)
                       if rc["TaskStatus"] == 0 and rc["TaskType"] == 0:
                           final = True
                           break
                       else:
                           time.sleep(60)
            except:
               logger2.error("faltten clone file fail")
               raise
        thrash_clone = NbdThrash(ssh,destination)
        thrash_clone.nbd_map()
        time.sleep(5)
        thrash_clone.nbd_getdev()
        thrash_clone.write_data(2500)
        snapshot_uuid = create_vol_snapshot(destination)
        starttime = time.time()
        final = False
        time.sleep(5)
        while time.time() - starttime < config.snapshot_timeout:
            rc = get_snapshot_status(destination,snapshot_uuid)
            if rc["Progress"] == 100 and rc["Status"] == 0 :
                final = True
                break
            else:
                time.sleep(60)
        if final == True:
            logger.info("test case bug-3835 pass!!!")
        else:
            assert False,"create lazy clone file snapshot fail"
    except:
        logger2.error("test case bug-3835 fail")
        raise
    delete_vol_snapshot(destination,snapshot_uuid)
    check_snapshot_delete(destination,snapshot_uuid)
    thrash.nbd_unmap()
    thrash_clone.nbd_unmap()
    unlink_clone_vol(destination)
    unlink_clone_vol(vol_id)

def test_snapshot_all(vol_uuid=config.snapshot_volid):
    write_full_vol = [True,False]
    for write_vol in write_full_vol: 
        init_nbd_vol(write_vol)
#        test_clone_vol_same_uuid(lazy)
        test_clone_iovol_consistency()
        test_clone_chain_consistency()
    test_create_many_snapshot()
    return "finally"

def begin_snapshot_test():
    t = mythread.runThread(test_snapshot_all,config.snapshot_volid)
    config.snapshot_thread = t
    t.start()

def loop_kill_snapshotserver():
    thread = config.snapshot_thread
    while thread.isAlive():
        random_kill_snapshot()
        time.sleep(120)

def begin_kill_snapshotserver():
    t = mythread.runThread(loop_kill_snapshotserver)
    t.start()

def stop_snapshot_test():
    try:
        if config.snapshot_thread == []:
            assert False,"snapshot thread not up"
        thread = config.snapshot_thread
        assert thread.exitcode == 0,"snapshot thread error"
        result = thread.get_result()
        assert result == "finally","snapshot test fail,result is %s"%result
    except:
        raise



