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
import base_operate
import json
import hashlib
from curvefs_python import curvefs

size_list = [100, 200, 400, 1000]

class NbdThrash:
    def __init__(self,ssh,name):
        self.name = name
        self.ssh = ssh
        self.user = "test"
        self.dev = ""
        self.check_md5 = ""

    def nbd_create(self,vol_size):
        cmd = "curve create --filename /%s --length %s --user test"%(self.name,vol_size)
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        assert rs[3] == 0,"create nbd fail：%s"%rs[1]

    def nbd_map(self):
        cmd = "sudo curve-nbd map cbd:pool1//%s_%s_ >/dev/null 2>&1"%(self.name,self.user)
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        assert rs[3] == 0,"map nbd fail：%s"%rs[1]
    
    def nbd_getdev(self):
        cmd = "sudo curve-nbd list-mapped |grep %s_ | awk '{print $3}'"%(self.name)
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        self.dev = "".join(rs[1]).strip()
        logger3.info("map %s to %s"%(self.name,self.dev))
        assert rs[3] == 0,"map fail：%s"%rs[1]

    def nbd_unmap(self):
        if self.dev != "":
            cmd = "sudo curve-nbd unmap %s"%(self.dev)
            rs = shell_operator.ssh_exec(self.ssh, cmd)
            assert rs[3] == 0,"unmap fail：%s"%rs[1]
            logger3.info("unmap dev %s"%self.dev)
            cmd = "sudo  curve-nbd list-mapped |grep %s_ | awk '{print $3}'"%(self.name)
            rs = shell_operator.ssh_exec(self.ssh, cmd)
            assert "".join(rs[1]).strip() == "","unmap fail,map dev is %s"%rs[1]
        else:
            assert False,"can not find nbd"
    
    def nbd_delete(self):
        cmd = "sudo curve delete --filename /%s --user %s"%(self.name,self.user)
        rs = shell_operator.ssh_exec(self.ssh, cmd)
        assert rs[3] == 0,"delete fail：%s"%rs[1]

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


def create_vol_snapshot(vol_uuid):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'CreateSnapshot'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['File'] = R'/' + vol_uuid
    payload['Name'] = 'test'
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService'%snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http,params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200,"create snapshot fail,return code is %d,return msg is %s"%(r.status_code,r.text)
    logger2.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    snapshot_uuid = ref["UUID"]
    return snapshot_uuid

def delete_vol_snapshot(voluuid,snapshot_uuid):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'DeleteSnapshot'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['File'] = R'/' + voluuid
    payload['UUID'] = snapshot_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    logger2.info("requests ret is %s"%r.text)
    assert r.status_code == 200, "delete snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)

def clone_vol_snapshot(snapshot_uuid,lazy="true"):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'Clone'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['Source'] = snapshot_uuid
    payload['Destination'] = R'/%s'%snapshot_uuid
    payload['Lazy']  = lazy
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200, "clone snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger2.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    clone_vol_uuid = ref["UUID"]
    return clone_vol_uuid

def clone_vol_from_file(file_name,lazy="true"):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'Clone'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['Source'] = "/" + file_name
    payload['Destination'] = R'/%s-%s'%(file_name,lazy)
    payload['Lazy']  = lazy
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200, "clone snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger2.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    clone_vol_uuid = ref["UUID"]
    return clone_vol_uuid

def clone_file(file_name,destination,lazy="true"):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'Clone'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['Source'] = "/" + file_name
    payload['Destination'] = R'/%s'%(destination)
    payload['Lazy']  = lazy
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200, "clone snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger2.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    clone_vol_uuid = ref["UUID"]
    return clone_vol_uuid

def flatten_clone_vol(clone_uuid):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'Flatten'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['UUID'] = clone_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200, "flatten clone vol fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger2.info("requests ret is %s"%r.text)

def clean_vol_clone(clone_uuid):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'CleanCloneTask'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['UUID'] = clone_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200, "clean clone vol fail,return code is %d,return msg is %s" % (r.status_code, r.text)

def unlink_clone_vol(vol_uuid):
    cbd = curvefs.CBDClient()
    rc = cbd.Init("./client.conf")
    if rc != 0:
        raise AssertionError
    filename = "/" + vol_uuid
    user = curvefs.UserInfo_t()
    user.owner = "test"
    logger2.debug("filename is %s"%filename)
    cbd.Unlink(str(filename), user)
    cbd.UnInit()

def cancel_vol_snapshot(voluuid,snapshot_uuid):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'CancelSnapshot'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['File'] = R'/' + voluuid
    payload['UUID'] = snapshot_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    logger2.info("requests ret is %s"%r.text)
    assert r.status_code == 200, "cancel snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)

def recover_snapshot(vol_uuid,snapshot_uuid,lazy='true'):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'Recover'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['Source'] = snapshot_uuid
    payload['Destination'] = R'/%s'%vol_uuid
    payload['Lazy']  = lazy
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200, "recover snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger2.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    recover_vol_uuid = ref["UUID"]
    return recover_vol_uuid

def detach_snapshot_vol(vol_uuid):
    ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%config.vm_host
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd, logger2)
    vm_uuid = "".join(rs[1]).strip()
    ori_cmd = "source OPENRC &&nova volume-detach  %s %s"%((vm_uuid,vol_uuid))
    rs = shell_operator.ssh_exec(ssh,ori_cmd, logger2)
    logger2.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"detach vol fail,return is %s"%rs[2]
    logger2.info("exec cmd %s"%ori_cmd)
    ssh.close()

def attach_snapshot_vol(vol_uuid):
    ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%config.vm_host
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd,logger2)
    vm_uuid = "".join(rs[1]).strip()
    ori_cmd = "source OPENRC &&nova volume-attach  %s %s"%((vm_uuid,vol_uuid))
    rs = shell_operator.ssh_exec(ssh,ori_cmd,logger2)
    logger2.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"detach vol fail,return is %s"%rs[2]
    logger2.info("exec cmd %s"%ori_cmd)
    ssh.close()

def get_snapshot_status(voluuid,snapshot_uuid):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'GetFileSnapshotInfo'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['File'] = '/' + voluuid
    payload['UUID'] = snapshot_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = 'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
#    assert r.status_code == 200, "get snapshot info fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger2.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    if r.status_code != 200:
        logger2.error("get snapshot %s fail"%snapshot_uuid)
        return ref["Code"]
    snapshots_info = ref["Snapshots"]
    for snapshot in snapshots_info:
        if snapshot["UUID"] == snapshot_uuid:
            return snapshot
    logger2.info("snap %s status is %s,progress is %s"%(snapshot_uuid,snapshot["Status"],snapshot["Progress"]))
    return False

def get_clone_status(clone_vol_uuid):
    snap_server = config.snapshot_vip
    payload = {}
    payload['Action'] = 'GetCloneTasks'
    payload['Version'] = config.snap_version
    payload['User'] = 'test'
    payload['UUID'] = clone_vol_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = 'http://%s:5555/SnapshotCloneService' % snap_server
    logger2.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger2.info("exec requests url:%s"%(r.url))
#    assert r.status_code == 200, "get clone vol info fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger2.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    if r.status_code != 200:
        logger2.error("get clone vol %s fail"%clone_vol_uuid)
        return ref["Code"]
    clones_info = ref["TaskInfos"]
    if clones_info == None:
         return None
    for clone in clones_info:
        if clone["UUID"] == clone_vol_uuid:
            return clone
#            if clone["TaskStatus"] == "0":
#                return True
    logger2.info("clone vol %s status is %s"%(clone_vol_uuid,clone["TaskStatus"]))
    return False

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
    if config.snapshot_thrash.check_md5 == True:
        assert context1 == context2,"vol and clone vol not same"

def init_nbd_vol(check_md5=True,lazy="True"):
    ssh = shell_operator.create_ssh_connect(config.client_list[0], 1046, config.abnormal_user)
    try:
        name = "volume-snapshot" + "-" + str(check_md5) + "-" + lazy
        thrash = NbdThrash(ssh,name)
        vol_size = 10 #GB
        thrash.nbd_create(vol_size)
        thrash.nbd_map()
        time.sleep(5)
        thrash.nbd_getdev()
        if check_md5 == True:
            init_data = vol_size * 1024
            thrash.write_data_full(init_data)
        else:
            init_data = 5000 #MB
            thrash.write_data(init_data)
        time.sleep(60)
        config.snapshot_volid = name
        config.snapshot_thrash = thrash
        thrash.check_md5 = check_md5
    except:
        logger2.error("create snapshot nbd fail")
        raise

def check_snapshot_delete(vol_id,snapshot_id):
    starttime = time.time()
    final = False
    while time.time() - starttime < 120:
        rc = get_snapshot_status(vol_id,snapshot_id)
        logger2.info("rc is %s"%rc)
        if rc == "-8":
            final = True
            break
        else:
           time.sleep(10)
    if final == True:
        return True
    else:
        assert False,"delete snapshot fail in 120s,rc is %s"%rc

def check_clone_clean(clone_id):
    starttime = time.time()
    final = False
    while time.time() - starttime < 120:
        rc = get_clone_status(clone_id)
        logger2.info("rc is %s"%rc)
        if rc == "-8":
            final = True
            break
        else:
           time.sleep(10)
    if final == True:
        return True
    else:
        assert False,"clean clone fail in 120s,rc is %s"%rc


def test_clone_iovol_consistency(lazy):
    logger2.info("------------begin test clone iovol consistency lazy=%s----------"%lazy)
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
        clone_vol_uuid = clone_vol_snapshot(snapshot_uuid,lazy)
        time.sleep(1)
        if lazy == "true":
            rc = check_clone_vol_exist(snapshot_uuid)
            assert rc,"clone vol volume-%s not create ok in 2s"%snapshot_uuid
    else:
        assert False,"create snapshot vol fail,status is %s"%rc
    final = False
    time.sleep(5)
    starttime = time.time()
    status = 0
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
           diff_vol_consistency(vol_id,snapshot_uuid)
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
           delete_vol_snapshot(vol_id,snapshot_uuid)
           check_snapshot_delete(vol_id,snapshot_uuid)
           clean_vol_clone(clone_vol_uuid)
           check_clone_clean(clone_vol_uuid)
           unlink_clone_vol(snapshot_uuid)
    else:
       assert False,"clone vol fail,status is %s"%rc

def test_clone_vol_from_file(lazy):
    logger2.info("------------begin test clone vol from file lazy=%s----------"%lazy)
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

def test_snapshot_all(vol_uuid):
    check_md5_all = [True]
    lazy_all = ["true","false"]
    for check_md5 in check_md5_all: 
        for lazy in lazy_all:
            init_nbd_vol(check_md5,lazy)
            test_clone_vol_from_file(lazy)
            test_clone_vol_same_uuid(lazy)
            test_clone_iovol_consistency(lazy)
            test_recover_snapshot(lazy)
            test_cancel_snapshot()
            config.snapshot_thrash.nbd_unmap()
            config.snapshot_thrash.nbd_delete()
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
