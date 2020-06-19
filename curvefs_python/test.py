#!/usr/bin/env python
# coding=utf-8
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
cmd = "curve stat --user k8s --filename /k8s/volume2"
exec_cmd(cmd)
cmd = "curve delete --user k8s --filename /k8s/volume2"
exec_cmd(cmd)
cmd = "curve rmdir --user k8s --dirname /k8s"
exec_cmd(cmd)
