#!/usr/bin/env python
# coding=utf-8

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

import curvefs
import parser
import time

fileType = ["INODE_DIRECTORY", "INODE_PAGEFILE", "INODE_APPENDFILE",
            "INODE_APPENDECFILE", "INODE_SNAPSHOT_PAGEFILE"]
fileStatus = ["Created", "Deleting", "Cloning",
              "CloneMetaInstalled", "Cloned", "BeingCloned"]
kGB = 1024 * 1024 * 1024
kUnitializedFileID = 0

# Refer to curve/include/client/libcurve.h
retCode = {0: "OK",
           1: "EXISTS",
           2: "FAILED",
           3: "DISABLEIO",
           4: "AUTHFAIL",
           5: "DELETING",
           6: "NOTEXIST",
           7: "UNDER_SNAPSHOT",
           8: "NOT_UNDERSNAPSHOT",
           9: "DELETE_ERROR",
           10: "NOT_ALLOCATE",
           11: "NOT_SUPPORT",
           12: "NOT_EMPTY",
           13: "NO_SHRINK_BIGGER_FILE",
           14: "SESSION_NOTEXISTS",
           15: "FILE_OCCUPIED",
           16: "PARAM_ERROR",
           17: "INTERNAL_ERROR",
           18: "CRC_ERROR",
           19: "INVALID_REQUEST",
           20: "DISK_FAIL",
           21: "NO_SPACE",
           22: "NOT_ALIGNED",
           23: "BAD_FD",
           24: "LENGTH_NOT_SUPPORT",
           25: "SESSION_NOT_EXIST",
           26: "STATUS_NOT_MATCH",
           27: "DELETE_BEING_CLONED",
           28: "CLIENT_NOT_SUPPORT_SNAPSHOT",
           29: "SNAPSTHO_FROZEN",
           100: "UNKNOWN"}


def getRetCodeMsg(ret):
    if retCode.has_key(-ret):
        return retCode[-ret]
    return "Unknown Error Code"


if __name__ == '__main__':
    # Parameter parsing
    args = parser.get_parser().parse_args()

    # Initialize client
    cbd = curvefs.CBDClient()
    ret = cbd.Init(args.confpath)
    if ret != 0:
        print "init fail"
        exit(1)

    # Obtain file user information
    user = curvefs.UserInfo_t()
    user.owner = args.user
    if args.password:
        user.password = args.password

    fileId = kUnitializedFileID

    if args.optype == "create":
        if args.stripeUnit or args.stripeCount:
            ret = cbd.Create2(args.filename, user, args.length *
                              kGB, args.stripeUnit, args.stripeCount)
        else:
            ret = cbd.Create(args.filename, user, args.length * kGB)
    elif args.optype == "delete":
        ret = cbd.Unlink(args.filename, user)
    elif args.optype == "recover":
        if args.id:
            fileId = args.id
        ret = cbd.Recover(args.filename, user, fileId)
    elif args.optype == "extend":
        ret = cbd.Extend(args.filename, user, args.length * kGB)
    elif args.optype == "stat":
        finfo = curvefs.FileInfo_t()
        ret = cbd.StatFile(args.filename, user, finfo)
        if ret == 0:
            print "id: " + str(finfo.id)
            print "parentid: " + str(finfo.parentid)
            print "filetype: " + fileType[finfo.filetype]
            print "length(GB): " + str(finfo.length/1024/1024/1024)
            print "createtime: " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(finfo.ctime/1000000))
            print "user: " + finfo.owner
            print "filename: " + finfo.filename
            print "fileStatus: " + fileStatus[finfo.fileStatus]
            print "stripeUnit: " + str(finfo.stripeUnit)
            print "stripeCount: " + str(finfo.stripeCount)
    elif args.optype == "rename":
        ret = cbd.Rename(user, args.filename, args.newname)
    elif args.optype == "mkdir":
        ret = cbd.Mkdir(args.dirname, user)
    elif args.optype == "rmdir":
        ret = cbd.Rmdir(args.dirname, user)
    elif args.optype == "list":
        dir = cbd.Listdir(args.dirname, user)
        for i in dir:
            print i
    if ret != 0:
        print args.optype + " fail, ret = " + str(ret) + ", " + getRetCodeMsg(ret)
        exit(1)
    curvefs.UnInit()
