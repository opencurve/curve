#!/usr/bin/env python3
# coding=utf-8

from cProfile import label
import os
import time
import json
import configparser
import subprocess
import re

CURVEFS_TOOL = "curvefs_tool"
JSON_PATH = "/tmp/topology.json"
PLUGIN_PATH = "plugin"
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
    metaservers = []
    if data is not None:
        for pool in data["poollist"]:
            for zone in pool["zonelist"]:
                for server in zone["serverlist"]:
                    for metaserver in server["metaserverlist"]:
                        metaservers.append(metaserver)
    return metaservers

def loadClient():
    ret, output = runCurvefsToolCommand(["list-fs"])
    clients = []
    label = lablesValue(None, "client")
    if ret == 0 :
        try:
            data = json.loads(output.decode())
        except json.decoder.JSONDecodeError:
            return unitValue(label, clients)
        for fsinfo in data["fsInfo"]:
            for mountpoint in fsinfo["mountpoints"]:
                clients.append(mountpoint["hostname"] + ":" + str(mountpoint["port"]))
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
    hostname = server["hostname"] + "." + str(server["metaserverid"])
    labels = lablesValue(hostname, "metaserver")
    serverAddr = []
    serverAddr.append(ipPort2Addr(server["externalip"], server["externalport"]))
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

def loadPlugin():
    # load *.json file in plugin dir
    # merge to one json
    data = []
    if os.path.isdir(PLUGIN_PATH):
        for filename in os.listdir(PLUGIN_PATH):
            if filename.endswith('.json'):
                with open(os.path.join(PLUGIN_PATH, filename)) as f:
                    plugin_data = json.load(f)
                    if len(plugin_data) == 0:
                        continue
                    data.append(unitValue(plugin_data["labels"], plugin_data["targets"]))
    return data

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
    plugin = loadPlugin()
    targets += plugin

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
