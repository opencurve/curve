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

fileType = ["INODE_DIRECTORY", "INODE_PAGEFILE", "INODE_APPENDFILE", "INODE_APPENDECFILE", "INODE_SNAPSHOT_PAGEFILE"]
fileStatus = ["Created", "Deleting", "Cloning", "CloneMetaInstalled", "Cloned", "BeingCloned"]
kGB = 1024 * 1024 * 1024
retCode = { 0 : "CURVE_ERROR_OK",
            1 : "CURVE_ERROR_EXISTS",
            2 : "CURVE_ERROR_FAILED",
            3 : "CURVE_ERROR_DISABLEIO",
            4 : "CURVE_ERROR_AUTHFAIL",
            5 : "CURVE_ERROR_DELETING",
            6 : "CURVE_ERROR_NOTEXIST",
            7 : "CURVE_ERROR_UNDER_SNAPSHOT",
            8 : "CURVE_ERROR_NOT_UNDERSNAPSHOT",
            9 : "CURVE_ERROR_DELETE_ERROR",
            10 : "CURVE_ERROR_NOT_ALLOCATE",
            11 : "CURVE_ERROR_NOT_SUPPORT",
            12 : "CURVE_ERROR_NOT_EMPTY",
            13 : "CURVE_ERROR_NO_SHRINK_BIGGER_FILE",
            14 : "CURVE_ERROR_SESSION_NOTEXISTS",
            15 : "CURVE_ERROR_FILE_OCCUPIED",
            16 : "CURVE_ERROR_PARAM_ERROR",
            17 : "CURVE_ERROR_INTERNAL_ERROR",
            18 : "CURVE_ERROR_CRC_ERROR",
            19 : "CURVE_ERROR_INVALID_REQUEST",
            20 : "CURVE_ERROR_DISK_FAIL",
            21 : "CURVE_ERROR_NO_SPACE",
            22 : "CURVE_ERROR_NOT_ALIGNED",
            23 : "CURVE_ERROR_BAD_FD",
            24 : "CURVE_ERROR_LENGTH_NOT_SUPPORT",
            100 : "CURVE_ERROR_UNKNOWN"}

def getRetCodeMsg(ret):
    if retCode.has_key(-ret) :
        return retCode[-ret]
    return "Unknown Error Code"

if __name__ == '__main__':
    # 参数解析
    args = parser.get_parser().parse_args()

    # 初始化client
    cbd = curvefs.CBDClient()
    ret = cbd.Init(args.confpath)
    if ret != 0:
        print "init fail"
        exit(1)

    # 获取文件user信息
    user = curvefs.UserInfo_t()
    user.owner = args.user
    if args.password:
        user.password = args.password

    if args.optype == "create":
        ret = cbd.Create(args.filename, user, args.length * kGB)
    elif args.optype == "delete":
        ret = cbd.Unlink(args.filename, user)
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
    elif args.optype == "rename":
        ret = cbd.Rename(user, args.filename, args.newname)
    elif args.optype == "mkdir":
        ret = cbd.Mkdir(args.dirname, user)
    elif args.optype == "rmdir":
        ret = cbd.Rmdir(args.dirname, user)
    elif args.optype == "list" :
        dir = cbd.Listdir(args.dirname, user)
        for i in dir:
            print i
    if ret != 0:
        print args.optype + " fail, ret = " + str(ret) + ", " + getRetCodeMsg(ret)
        exit(1)
    curvefs.UnInit()
