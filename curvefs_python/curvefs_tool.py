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
        print args.optype + " fail, ret = " + str(ret)
        exit(1)
    curvefs.UnInit()
