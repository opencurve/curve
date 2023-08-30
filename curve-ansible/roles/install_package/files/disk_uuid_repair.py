#!/usr/bin/env python
# -*- coding: utf8 -*-

#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

#Check if the uuid recorded in disk. meta on the disk matches the actual uuid of the current disk
#If not, update to the current uuid

import os
import hashlib
import sys
import subprocess

def __get_umount_disk_list():
    #Obtain devices that need to be mounted
    cmd = "lsblk -O|grep ATA|awk '{print $1}'"
    out_msg = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    devlist = out_msg.splitlines()

    #View the mounting status of the current device
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
        #Verification
        if (hashlib.md5(uuid).hexdigest() != uuidmd5):
            print("uuid[%s] not match uuidmd5[%s]" % (uuid, uuidmd5))
            return ""
    return uuid

def __get_recorduuid(disk):
    uuid = ""
    #Mount the disk to a temporary directory
    cmd = "mkdir -p /data/tmp; mount " + disk + " /data/tmp"
    retCode = subprocess.call(cmd, shell=True)
    if retCode != 0:
        print("Get record uuid in %s fail." % disk)
        return False, uuid

    #Successfully mounted, obtaining the recorded uuid
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

    #Unmount Disk
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
       #Obtain the uuid recorded on the current disk
       diskFullName = "/dev/" + disk
       opRes, recorduuid = __get_recorduuid(diskFullName)
       if opRes != True or len(recorduuid) == 0:
           return False, recordList, actualList

       #Obtain the actual uuid of the disk
       actualuuid = __get_actualuuid(disk).replace("\n", "")

       #Compare whether the recorded and actual values are the same
       if actualuuid != recorduuid:
           recordList[disk] = recorduuid
           actualList[disk] = actualuuid
       else:
            return False, recordList, actualList

    return True, recordList, actualList

def __mount_with_atual_uuid(diskPath, record, actual):
    print("%s uuid change from [%s] to [%s]." % (diskPath, record, actual))
    #Obtain the corresponding mount directory from/etc/fstab
    mntdir = ""
    try:
        cmd = "grep " + record + " /etc/fstab | awk -F \" \" '{print $2}'"
        mntdir = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).replace("\n", "")
    except subprocess.CalledProcessError as e:
        print("Get mount dir for %s fail. error: %s." % (diskPath, e))
        return False

    #Mount the actual to the corresponding directory
    cmd = "mount " + diskPath + " " + mntdir
    retCode = subprocess.call(cmd, shell=True)
    if retCode !=0:
        print("exec [%s] fail." % cmd)
        return False
    print("mount %s to %s success." % (diskPath, mntdir))

    replaceCmd = "sed -i \"s/" + record + "/" + actual + "/g\""
    #Write the new uuid to fstab
    cmd = "cp /etc/fstab /etc/fstab.bak;" + replaceCmd + " /etc/fstab > /dev/null"
    retCode = subprocess.call(cmd, shell=True)
    if retCode !=0:
        print("exec [%s] fail." % cmd)
        return False
    print("modify actual uuid to /etc/fstab for disk %s success." % diskPath)

    #Write the new uuid to diskmeta
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
        #Mount according to the actual uuid
        res = __mount_with_atual_uuid("/dev/" + disk, record[disk], actual[disk])
        if res:
            continue
        else:
            return False
    return True

if __name__ == "__main__":
    #View the list of disk devices that were not successfully mounted
    umountDisk = __get_umount_disk_list()
    if len(umountDisk) == 0:
        print("All disk mount success.")
        exit(0)

    #Check if it has been previously mounted
    if __uninit():
        print("Please init env with chunkserver_ctl.sh first.")
        exit(0)

    #View the uuid and actual uuid of disk devices that have not been successfully mounted currently
    cmpRes, record, actual = __cmp_recorduuid_with_actual(umountDisk)
    if cmpRes == False:
        print("Compare record uuid with actual uuid fail.")
        exit(-1)
    elif len(record) == 0:
        print("Record uuid with actual uuid all consistent.")
        exit(0)

    #Remount inconsistent disks according to the current uuid
    if __handle_inconsistent(umountDisk, record, actual):
        print("fix uuid-changed disk[%s] success." % umountDisk)
        exit(0)
    else:
        print("fxi uuid-changed disk[%s] fail." % umountDisk)
        exit(-1)



