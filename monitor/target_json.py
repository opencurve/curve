#!/usr/bin/env python
# coding=utf-8

#
#     Copyright (c) 2020 NetEase Inc.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#

import os
import time
import json
import ConfigParser
import commands

targetPath=None
etcd_exporter=None
mds_exporter=None
snapclone_exporter=None

def loadConf():
    global targetPath,etcd_exporter,mds_exporter,snapclone_exporter
    conf=ConfigParser.ConfigParser()
    conf.read("target.ini")
    targetPath=conf.get("path", "target_path")

def refresh():
    targets = []

    # Get chunkserver's ip and port
    chunkserverip = []
    chunkserverport= []
    curve_ops_tool_res = commands.getstatusoutput("curve_ops_tool chunkserver-list -checkHealth=false")
    if curve_ops_tool_res[0] != 0:
        print "curve_ops_tool list chunkserver fail!"
    else:
        chunkserver_infos = curve_ops_tool_res[1].split("\n")
        for line in chunkserver_infos:
            if not line.startswith("chunkServerID"):
                continue
            ip = line.split(", ")[2].split(" = ")[1]
            chunkserverip.append(ip)
            port = line.split(", ")[3].split(" = ")[1]
            chunkserverport.append(port)

    # Get chunkserver's hostname
    ip2hostname_dict = {}
    curve_ops_tool_res = commands.getstatusoutput("curve_ops_tool server-list")
    if curve_ops_tool_res[0] != 0:
        print "curve_ops_tool list server fail!"
    else:
        server_infos = curve_ops_tool_res[1].split("\n")
        for line in server_infos:
            if not line.startswith("serverID"):
                continue
            ip = line.split(", ")[2].split(" = ")[1]
            hostname = line.split(", ")[1].split(" = ")[1]
            ip2hostname_dict[ip] = hostname

    # add chunkserver targets
    for i in range(len(chunkserverip)):
        hostname=ip2hostname_dict[chunkserverip[i]]
        targets.append({
            'labels': {'job': "chunkserver", 'hostname': hostname},
            'targets': [chunkserverip[i]+':'+chunkserverport[i]],
        })

    # add node_exporter targets
    targets.append({
        'labels': {'job': "node_exporter"},
        'targets': [t+':9100' for t in chunkserverip],
    })

    # get client's ip and port
    curve_ops_tool_res = commands.getstatusoutput("curve_ops_tool client-list -listClientInRepo=true")
    if curve_ops_tool_res[0] != 0:
        print "curve_ops_tool list client fail!"
    else:
        # add client targets
        targets.append({
            'labels': {'job': "client"},
            'targets': curve_ops_tool_res[1].split("\n"),
        })

    # get etcd ip and port
    curve_ops_tool_res = commands.getstatusoutput("curve_ops_tool etcd-status | grep online")
    if curve_ops_tool_res[0] != 0:
        print "curve_ops_tool get etcd-status fail!"
    else:
        etcd_addrs = curve_ops_tool_res[1].split(": ")[1]
        # add etcd targets
        targets.append({
            'labels': {'job': "etcd"},
            'targets': etcd_addrs.split(", "),
        })

    # get mds ip and port
    curve_ops_tool_res = commands.getstatusoutput("curve_ops_tool mds-status | grep current")
    if curve_ops_tool_res[0] != 0:
        print "curve_ops_tool get mds-status fail!"
    else:
        mds_addrs = curve_ops_tool_res[1].split(": ")[1]
        # add mds targets
        targets.append({
            'labels': {'job': "mds"},
            'targets': mds_addrs.split(", "),
        })

    # get snapclone ip and port
    curve_ops_tool_res = commands.getstatusoutput("curve_ops_tool snapshot-clone-status | grep current")
    if curve_ops_tool_res[0] != 0:
        print "curve_ops_tool get snapshot-clone-status fail!"
    else:
        snapclone_addrs = curve_ops_tool_res[1].split(": ")[1]
        # add snapclone targets
        targets.append({
            'labels': {'job': "snapclone"},
            'targets': snapclone_addrs.split(", "),
        })

    with open(targetPath+'.new', 'w', 0777) as fd:
        json.dump(targets, fd)
        fd.flush()
        os.fsync(fd.fileno())

    os.rename(targetPath+'.new', targetPath)
    os.chmod(targetPath, 0777)

if __name__ == '__main__':
    while True:
        loadConf()
        refresh()
        # refresh every 30s
        time.sleep(30)
