#/usr/bin/env python
# -*- coding: utf8 -*-

import os, time, random, re
from multiprocessing import Process, Pool
import datetime
from config import config
from logger import logger
from lib import shell_operator
from lib import db_operator
import mythread
import requests
import base_operate
import json
import hashlib
from curvefs_python import curvefs

size_list = [100, 200, 400, 1000]

def cinder_vol_create(ssh, size):
    name = "volume" + str(int(time.time()))
    cmd = "source OPENRC && cinder create --volume-type curve %d  --availability-zone dongguan1 --disable_host  \
         pubbeta2-curve2.dg.163.org@curve --display-name %s " %(size,name)
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"create vol fail,error is %s"%rs
    id_pt = re.compile(r'\S+\s+id\s+\S\s+(\S+)')
    id_mt = id_pt.search("".join(rs[1]))
#    logger.error("id_mt = %s"%id_mt)
    if id_mt:
        id = id_mt.group(1)
    return id

def nova_vol_attach(ssh,vm_id, vol_id):
    cmd = 'source OPENRC && nova volume-attach %s %s' %(vm_id,vol_id)
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0,"attach vol %s fail,error is %s"%(vol_id,rs)

def nova_vol_detach(ssh,vm_id, vol_id):
    cmd = 'source OPENRC && nova volume-detach %s %s' %(vm_id,vol_id)
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0, "detach vol %s fail,error is %s" % (vol_id,rs)

def show_vol(ssh,id):
    status = 'none'
    cmd = "source OPENRC && cinder show %s" % id
    rs = shell_operator.ssh_exec(ssh, cmd,log=False)
    assert rs[3] == 0, "show vol %s fail" % vol_id
    status_pt = re.compile(r'\S+\s+status\s+\S\s+(\S+)')
    status_mt = status_pt.search("".join(rs[1]))
    if status_mt:
        status = status_mt.group(1)
        #print status
    return status

def wait_vol_status(ssh,id, status):
    time_expire = 350
    time_all = 0
    status_now = show_vol(ssh,id)
    while status_now != status:
        if time_all < time_expire:
            time.sleep(4)
            time_all = time_all + 4
            status_now = show_vol(ssh,id)
            if status_now == "error":
                logger.error("vol %serror"%id)
                assert False,"vol %s error"%id
                break
        else:
            print "time expired %s %s" %(id, status)
            break

def vol_deleted(ssh,vol_id):
    cmd = 'source OPENRC && cinder delete %s' % (vol_id)
    rs = shell_operator.ssh_exec(ssh, cmd)
    assert rs[3] == 0, "delete vol %s fail,error is %s" % (vol_id,rs)


def vol_all(vm_id):
    thrash_time = 0
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    while config.thrash_attach == True:
        try:
            vol_size = random.choice(size_list)
            vol_id = cinder_vol_create(ssh,vol_size)
            logger.debug("vol id is %s"%vol_id)
            wait_vol_status(ssh,vol_id, 'available')
            nova_vol_attach(ssh,vm_id, vol_id)
            wait_vol_status(ssh,vol_id, 'in-use')
            nova_vol_detach(ssh,vm_id, vol_id)
            wait_vol_status(ssh,vol_id, 'available')
            vol_deleted(ssh,vol_id)
            thrash_time = thrash_time + 1
            logger.info("thrash_attach_detach time is %d,vol_size is %d"%(thrash_time,vol_size))
        except:
            logger.error("attach/detach vol %s fail"%vol_id)
            config.thrash_attach = False
            raise
    else:
        return thrash_time

def vol_write_data():
    ssh = shell_operator.create_ssh_connect(config.vm_stability_host, 22, config.vm_user)
    ori_cmd = "lsblk |grep %dG | awk '{print $1}'"%config.snapshot_size
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    vd = "".join(rs[1]).strip()
    ori_cmd = "fio -name=/dev/%s -direct=1 -iodepth=8 -rw=write -ioengine=libaio -bs=64k -size=%dG -numjobs=1 -time_based  -runtime=120"%(vd,config.snapshot_size)
    rs = shell_operator.ssh_exec(ssh, ori_cmd)
    assert rs[3] == 0,"write fio fail"

def creat_vol_snapshot(vol_uuid):
    snap_server = random.choice(config.snap_server_list)
    payload = {}
    payload['Action'] = 'CreateSnapshot'
    payload['Version'] = config.snap_version
    payload['User'] = 'cinder'
    payload['File'] = R'/cinder/volume-' + vol_uuid
    payload['Name'] = 'test'
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService'%snap_server
    logger.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http,params=payload_str)
    logger.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200,"create snapshot fail,return code is %d,return msg is %s"%(r.status_code,r.text)
    logger.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    snapshot_uuid = ref["UUID"]
    return snapshot_uuid

def delete_vol_snapshot(voluuid,snapshot_uuid):
    snap_server = random.choice(config.snap_server_list)
    payload = {}
    payload['Action'] = 'DeleteSnapshot'
    payload['Version'] = config.snap_version
    payload['User'] = 'cinder'
    payload['File'] = R'/cinder/volume-' + voluuid
    payload['UUID'] = snapshot_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger.info("exec requests url:%s"%(r.url))
    logger.info("requests ret is %s"%r.text)
    assert r.status_code == 200, "delete snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)

def clone_vol_snapshot(snapshot_uuid,lazy="true"):
    snap_server = random.choice(config.snap_server_list)
    payload = {}
    payload['Action'] = 'Clone'
    payload['Version'] = config.snap_version
    payload['User'] = 'cinder'
    payload['Source'] = snapshot_uuid
    payload['Destination'] = R'/cinder/volume-%s'%snapshot_uuid
    payload['Lazy']  = lazy
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger.info("exec requests url:%s"%(r.url))
    assert r.status_code == 200, "clone snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    clone_vol_uuid = ref["UUID"]
    return clone_vol_uuid

def cancel_vol_snapshot(voluuid,snapshot_uuid):
    snap_server = random.choice(config.snap_server_list)
    payload = {}
    payload['Action'] = 'CancelSnapshot'
    payload['Version'] = config.snap_version
    payload['User'] = 'cinder'
    payload['File'] = R'/cinder/volume-' + voluuid
    payload['UUID'] = snapshot_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = R'http://%s:5555/SnapshotCloneService' % snap_server
    logger.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger.info("exec requests url:%s"%(r.url))
    logger.info("requests ret is %s"%r.text)
    assert r.status_code == 200, "cancel snapshot fail,return code is %d,return msg is %s" % (r.status_code, r.text)

def detach_snapshot_vol(vol_uuid):
    ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%config.vm_host
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    vm_uuid = "".join(rs[1]).strip() 
    ori_cmd = "source OPENRC &&nova volume-detach  %s %s"%((vm_uuid,vol_uuid))
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"detach vol fail,return is %s"%rs[2]
    logger.info("exec cmd %s"%ori_cmd)
    ssh.close()

def attach_snapshot_vol(vol_uuid):
    ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%config.vm_host
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    vm_uuid = "".join(rs[1]).strip()  
    ori_cmd = "source OPENRC &&nova volume-attach  %s %s"%((vm_uuid,vol_uuid))
    rs = shell_operator.ssh_exec(ssh,ori_cmd)
    logger.info("exec cmd %s" % ori_cmd)
    assert rs[3] == 0,"detach vol fail,return is %s"%rs[2]
    logger.info("exec cmd %s"%ori_cmd)
    ssh.close()
  
def get_snapshot_status(voluuid,snapshot_uuid):
    snap_server = random.choice(config.snap_server_list)
    payload = {}
    payload['Action'] = 'GetFileSnapshotInfo'
    payload['Version'] = config.snap_version
    payload['User'] = 'cinder'
    payload['File'] = '/cinder/volume-' + voluuid
    payload['UUID'] = snapshot_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = 'http://%s:5555/SnapshotCloneService' % snap_server
    logger.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger.info("exec requests url:%s"%(r.url))
#    assert r.status_code == 200, "get snapshot info fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger.info("requests ret is %s"%r.text)    
    ref = json.loads(r.text)
    if r.status_code != 200:
        logger.error("get snapshot %s fail"%snapshot_uuid)
        return ref["Code"]
    snapshots_info = ref["Snapshots"]
    for snapshot in snapshots_info:
        if snapshot["UUID"] == snapshot_uuid:
            return snapshot
    logger.info("snap %s status is %s,progress is %s"%(snapshot_uuid,snapshot["Status"],snapshot["Progress"]))
    return False

def get_clone_status(clone_vol_uuid):
    snap_server = random.choice(config.snap_server_list)
    payload = {}
    payload['Action'] = 'GetCloneTasks'
    payload['Version'] = config.snap_version
    payload['User'] = 'cinder'
    payload['UUID'] = clone_vol_uuid
    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())
    http = 'http://%s:5555/SnapshotCloneService' % snap_server
    logger.info("exec requests:%s %s"%(http,payload))
    r = requests.get(http, params=payload_str)
    logger.info("exec requests url:%s"%(r.url))
#    assert r.status_code == 200, "get clone vol info fail,return code is %d,return msg is %s" % (r.status_code, r.text)
    logger.info("requests ret is %s"%r.text)
    ref = json.loads(r.text)
    if r.status_code != 200:
        logger.error("get clone vol %s fail"%sclone_vol_uuid)
        return ref["Code"]
    clones_info = ref["TaskInfos"]
    for clone in clones_info:
        if clone["UUID"] == clone_vol_uuid:
            return clone
#            if clone["TaskStatus"] == "0":
#                return True
    logger.info("clone vol %s status is %s"%(clone_vol_uuid,clone["TaskStatus"]))
    return False

def get_vol_md5(vol_uuid):
    curvefs.UnInit()
    rc = curvefs.Init("./client.conf") 
    if rc != 0:
        raise AssertionError
    filename = "/cinder/volume-" + vol_uuid
    user = curvefs.UserInfo_t()
    user.owner = "cinder"
#    user.password = ""
    logger.info("file name is %s,type is %s"%(filename,type(filename)))
    logger.info("user is %s,type is %s"%(user,type(user)))
    fd = curvefs.Open(str(filename), user)
    buf = ''
    md5_obj = hashlib.md5()
    for i in range(1,2560):
        j = i - 1
        context = curvefs.Read(fd, buf, 4096*j,4096*i)
        md5_obj.update(context)
    hash_code = md5_obj.hexdigest()
    md5 = str(hash_code).lower()
    curvefs.Close(fd)
    logger.info("md5 is %s"%md5)
    return md5

def diff_vol_consistency(vol_uuid,clone_uuid):
    context1 = get_vol_md5(vol_uuid)
    context2 = get_vol_md5(clone_uuid)
    assert context1 == context2,"vol and clone vol not same"

def init_vol():
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    try:
        ori_cmd = "source OPENRC && nova list |grep %s | awk '{print $2}'"%config.vm_stability_host
        rs = shell_operator.ssh_exec(ssh,ori_cmd)
        vm_id = "".join(rs[1]).strip()
        vol_size = config.snapshot_size
        vol_id = cinder_vol_create(ssh,vol_size)
        logger.debug("snapshot test vol id is %s"%vol_id)
        wait_vol_status(ssh,vol_id, 'available')
        nova_vol_attach(ssh,vm_id, vol_id)
        wait_vol_status(ssh,vol_id, 'in-use')
        vol_write_data()
    except:
        logger.error("create vol fail")
        raise
    ssh.close()
    config.snapshot_vmid = vm_id
    config.snapshot_volid = vol_id

def check_snapshot_delete(vol_id,snapshot_id):
    starttime = time.time()
    final = False
    while time.time() - starttime < 120:
        rc = get_snapshot_status(vol_id,snapshot_id)
        if rc["Code"] == "-8":
            final = True
            break
        else:
           time.sleep(10)
    if final == True:
        return True
    else:
        assert False,"delete snapshot fail in 120s,rc is %s"%rc

def test_clone_iovol_consistency():
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    vol_id = config.snapshot_volid
    vm_id = config.snapshot_vmid 
    snapshot_uuid = creat_vol_snapshot(vol_id)
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
        clone_vol_uuid = clone_vol_snapshot(snapshot_uuid)
    else:
        assert False,"create snapshot vol fail,status is %s"%rc
    final = False
    time.sleep(5)
    starttime = time.time()
    while time.time() - starttime < config.snapshot_timeout:
        rc = get_clone_status(clone_vol_uuid)
        if rc["TaskStatus"] == 0 and rc["TaskType"] == 0:
            final = True
            break
        else:
           time.sleep(60)
    if final == True:
        time.sleep(3)
        try:
           nova_vol_detach(ssh,vm_id, vol_id)
           wait_vol_status(ssh,vol_id, 'available')
           diff_vol_consistency(vol_id,snapshot_uuid)
        except:
           raise
        finally:
           nova_vol_attach(ssh,vm_id, vol_id)
    else:
       assert False,"clone vol fail,status is %s"%rc

def test_cancel_snapshot():
    vol_write_data()
    ssh = shell_operator.create_ssh_connect(config.nova_host, 1046, config.nova_user)
    vol_id = config.snapshot_volid
    vm_id = config.snapshot_vmid
    snapshot_uuid = creat_vol_snapshot(vol_id)
    starttime = time.time()
    time.sleep(60)
    cancel_vol_snapshot(vol_id,snapshot_uuid)
    starttime = time.time()
    final = False
    time.sleep(5)
    check_snapshot_delete(vol_id,snapshot_uuid)

#def test_delete_snapshot():

#def test_delete_clone():    

def test_snapshot_all(vol_uuid):
    test_clone_iovol_consistency()
    test_cancel_snapshot()
    return "finally"

def begin_snapshot_test():
    t = mythread.runThread(test_snapshot_all,config.snapshot_volid)
    config.snapshot_thread = t
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
