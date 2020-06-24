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

import os

def exec_cmd(cmd):
    ret = os.system(cmd)
    if ret == 0:
        print cmd + " exec success"
    else :
        print cmd + " exec fail, ret = " + str(ret)


cmd = "curve mkdir --user k8s --dirname /k8s"
exec_cmd(cmd)
cmd = "curve create --user k8s --filename /k8s/volume1 --length 10"
exec_cmd(cmd)
cmd = "curve extend --user k8s --filename /k8s/volume1 --length 11"
exec_cmd(cmd)
cmd = "curve rename --user k8s --filename /k8s/volume1 --newname /k8s/volume2"
exec_cmd(cmd)
cmd = "curve list --user k8s --dirname /k8s"
exec_cmd(cmd)
# only the root user can list the root directory
cmd = "curve list --user k8s --dirname /"
exec_cmd(cmd)
# user is root, but no password is provided
cmd = "curve list --user root --dirname /"
exec_cmd(cmd)
cmd = "curve list --user root --dirname / --password root_password"
exec_cmd(cmd)
cmd = "curve stat --user k8s --filename /k8s/volume2"
exec_cmd(cmd)
cmd = "curve delete --user k8s --filename /k8s/volume2"
exec_cmd(cmd)
cmd = "curve rmdir --user k8s --dirname /k8s"
exec_cmd(cmd)
