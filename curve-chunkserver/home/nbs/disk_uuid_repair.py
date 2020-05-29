#!/usr/bin/env python
# -*- coding: utf8 -*-
# 检测磁盘上disk.meta中记录的uuid与当前磁盘的实际uuid是否相符合
# 如果不符合, 更新为当前的uuid

import os
import hashlib
import sys
import subprocess

def __get_umount_disk_list():
    # 获取需要挂载的设备
    cmd = "lsblk -O|grep ATA|awk '{print $1}'"
    out_msg = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    devlist = out_msg.splitlines()

    # 查看当前设备的挂载状况
    umount = []
    for dev in devlist:
        cmd = "lsblk|grep " + dev + "|awk '{print $7}'"
        out_msg = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)

        if len(out_msg.replace('\n', '')) == 0:
            umount.append(dev)
    return umount

def __uninit():
    try:
        cmd = "grep curvefs /etc/fstab"
        subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        return False
    except subprocess.CalledProcessError:
        return True

def __analyse_uuid(kv):
    uuid = ""
    uuidkv = kv[0].split("=")
    if len(uuidkv) != 2:
        print("uuid[%s] record format error." % kv[0])
        return ""
    else:
        uuid = uuidkv[1].replace("\n", "")

    uuidmd5kv = kv[1].split("=")
    if len(uuidmd5kv) != 2:
        print("uuidmd5[%s] record format error." % kv[1])
        return ""
    else:
        uuidmd5 = uuidmd5kv[1].replace("\n", "")
        # 校验
        if (hashlib.md5(uuid).hexdigest() != uuidmd5):
            print("uuid[%s] not match uuidmd5[%s]" % (uuid, uuidmd5))
            return ""
    return uuid

def __get_recorduuid(disk):
    uuid = ""
    # 将磁盘挂载到临时目录
    cmd = "mkdir -p /data/tmp; mount " + disk + " /data/tmp"
    retCode = subprocess.call(cmd, shell=True)
    if retCode != 0:
        print("Get record uuid in %s fail." % disk)
        return False, uuid

    # 挂载成功，获取记录的uuid
    try:
        cmd = "cat /data/tmp/disk.meta"
        out_msg = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)

        kv = out_msg.splitlines()
        if len(kv) != 2:
            print("File disk.meta in %s error." % disk)
        else:
            uuid = __analyse_uuid(kv)
            if len(uuid) == 0:
                print("Get uuid from disk.meta in %s fail." % disk)
    except subprocess.CalledProcessError as e:
        print("Get file disk.meta from %s fail, reason: %s." % (disk, e))

    # 卸载磁盘
    cmd = "umount " + disk + "; rm -fr /data/tmp"
    retCode = subprocess.call(cmd, shell=True)
    if retCode != 0:
        print("call [%s] occurr error." % cmd)
        return False, uuid

    return True, uuid

def __get_actualuuid(disk):
    uuid = ""
    try:
        cmd = "ls -l /dev/disk/by-uuid/|grep " + disk + "|awk '{print $9}'"
        uuid = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print("Get actual uuid of %s fail, reason: %s." % (disk, e))

    return uuid

def __cmp_recorduuid_with_actual(umountDisk):
    recordList = {}
    actualList = {}
    for disk in umountDisk:
       # 获取当前disk上记录的uuid
       diskFullName = "/dev/" + disk
       opRes, recorduuid = __get_recorduuid(diskFullName)
       if opRes != True or len(recorduuid) == 0:
           return False, recordList, actualList

       # 获取disk的实际uuid
       actualuuid = __get_actualuuid(disk).replace("\n", "")

       # 比较记录的和实际的是否相同
       if actualuuid != recorduuid:
           recordList[disk] = recorduuid
           actualList[disk] = actualuuid
       else:
            return False, recordList, actualList

    return True, recordList, actualList

def __mount_with_atual_uuid(diskPath, record, actual):
    print("%s uuid change from [%s] to [%s]." % (diskPath, record, actual))
    # 从/etc/fstab中获取对应的挂载目录
    mntdir = ""
    try:
        cmd = "grep " + record + " /etc/fstab | awk -F \" \" '{print $2}'"
        mntdir = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).replace("\n", "")
    except subprocess.CalledProcessError as e:
        print("Get mount dir for %s fail. error: %s." % (diskPath, e))
        return False

    # 将actual挂载到相应的目录下
    cmd = "mount " + diskPath + " " + mntdir
    retCode = subprocess.call(cmd, shell=True)
    if retCode !=0:
        print("exec [%s] fail." % cmd)
        return False
    print("mount %s to %s success." % (diskPath, mntdir))

    replaceCmd = "sed -i \"s/" + record + "/" + actual + "/g\""
    # 将新的uuid写入到fstab
    cmd = "cp /etc/fstab /etc/fstab.bak;" + replaceCmd + " /etc/fstab > /dev/null"
    retCode = subprocess.call(cmd, shell=True)
    if retCode !=0:
        print("exec [%s] fail." % cmd)
        return False
    print("modify actual uuid to /etc/fstab for disk %s success." % diskPath)

    # 将新的uuid写入到diskmeta
    fileFullName = mntdir + "/disk.meta"
    filebakName = fileFullName + ".bak"
    cpcmd = "cp " + fileFullName + " " + filebakName
    uuidcmd = "echo uuid=" + actual + " > " + fileFullName
    uuidmd5cmd = "echo uuidmd5=" + hashlib.md5(actual).hexdigest() + " >> " + fileFullName
    cmd = cpcmd + ";" + uuidcmd + ";" + uuidmd5cmd
    retCode = subprocess.call(cmd, shell=True)
    if retCode !=0:
        print("exec [%s] fail." % cmd)
        return False
    print("modify actual uuid to %s success." % fileFullName)

    return True


def __handle_inconsistent(umountDisk, record, actual):
    for disk in umountDisk:
        if disk not in record:
            print("record uuid and actual uuid of %s is same, please check other reason" % disk)
            continue
        # 按照actual uuid做挂载
        res = __mount_with_atual_uuid("/dev/" + disk, record[disk], actual[disk])
        if res:
            continue
        else:
            return False
    return True

if __name__ == "__main__":
    # 查看未挂载成功的磁盘设备列表
    umountDisk = __get_umount_disk_list()
    if len(umountDisk) == 0:
        print("All disk mount success.")
        exit(0)

    # 查看是否之前已经挂载过
    if __uninit():
        print("Please init env with chunkserver_ctl.sh first.")
        exit(0)

    # 查看当前未挂载成功的磁盘设备记录的uuid和实际uuid
    cmpRes, record, actual = __cmp_recorduuid_with_actual(umountDisk)
    if cmpRes == False:
        print("Compare record uuid with actual uuid fail.")
        exit(-1)
    elif len(record) == 0:
        print("Record uuid with actual uuid all consistent.")
        exit(0)

    # 将不一致的磁盘按照当前的uuid重新挂载
    if __handle_inconsistent(umountDisk, record, actual):
        print("fix uuid-changed disk[%s] success." % umountDisk)
        exit(0)
    else:
        print("fxi uuid-changed disk[%s] fail." % umountDisk)
        exit(-1)



