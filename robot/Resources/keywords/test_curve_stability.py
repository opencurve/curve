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
