#!/usr/bin/env python3
# coding=utf-8

#
#     Copyright (c) 2022 NetEase Inc.
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

from cProfile import label
import os
import time
import json
import configparser
import subprocess
import re

CURVEFS_TOOL = "curvefs_tool"
JSON_PATH = "/tmp/topology.json"
HOSTNAME_PORT_REGEX = r"[^\"\ ]\S*:\d+"
IP_PORT_REGEX = r"[0-9]+(?:\.[0-9]+){3}:\d+"

targetPath=None

def loadConf():
    global targetPath
    conf=configparser.ConfigParser()
    conf.read("target.ini")
    targetPath=conf.get("path", "target_path")

def runCurvefsToolCommand(command):
    cmd = [CURVEFS_TOOL]+command
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=5)
    except subprocess.TimeoutExpired as e:
        return -1, output
    except subprocess.CalledProcessError as e:
        return 0, e.output
    return 0, output
    

def loadServer():
    ret, _ = runCurvefsToolCommand(["list-topology", "-jsonType=tree", "-jsonPath=%s"%JSON_PATH])
    data = None
    if ret == 0:
        with open(JSON_PATH) as load_f:
            data = json.load(load_f)
    servers = []
    if data is not None:
        for pool in data["poollist"]:
            for zone in pool["zonelist"]:
                for server in zone["serverlist"]:
                    servers.append(server)
    return servers

def loadClient():
    ret, output = runCurvefsToolCommand(["list-fs"])
    clients = []
    if ret == 0 :
        data = json.loads(output.decode())
        for fsinfo in data["fsInfo"]:
            for mountpoint in fsinfo["mountpoints"]:
                clients.append(mountpoint["hostname"] + ":" + str(mountpoint["port"]))
    label = lablesValue(None, "client")
    return unitValue(label, clients)

def loadType(hostType):
    ret, output = runCurvefsToolCommand(["status-%s"%hostType])
    targets = []
    if ret == 0:
        targets = re.findall(IP_PORT_REGEX, str(output))
    labels = lablesValue(None, hostType)
    return unitValue(labels, targets)

def ipPort2Addr(ip, port):
    return str(ip) + ":" + str(port)

def server2Target(server):
    labels = lablesValue(server["hostname"], "metaserver")
    serverAddr = []
    serverAddr.append(ipPort2Addr(server["internalip"], server["internalport"]))
    targets = list(set(serverAddr))
    return unitValue(labels, targets)

def lablesValue(hostname, job):
    labels = {}
    if hostname is not None:
        labels["hostname"] = hostname
    if job is not None:
        labels["job"] = job
    return labels

def unitValue(lables, targets):
    unit = {}
    if lables is not None:
        unit["labels"] = lables
    if targets is not None:
        unit["targets"] = targets
    return unit


def refresh():
    targets = []
    # load metaserver
    servers = loadServer()
    for server in servers:
        targets.append(server2Target(server))
    # load etcd
    etcd = loadType("etcd")
    targets.append(etcd)
    # load mds
    mds = loadType("mds")
    targets.append(mds)
    # load client 
    client = loadClient()
    targets.append(client)
    
    with open(targetPath+'.new', 'w', 0o777) as fd:
        json.dump(targets, fd, indent=4)
        fd.flush()
        os.fsync(fd.fileno())

    os.rename(targetPath+'.new', targetPath)
    os.chmod(targetPath, 0o777)


if __name__ == '__main__':
    while True:
        loadConf()
        refresh()
        # refresh every 30s
        time.sleep(30)
