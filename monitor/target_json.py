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
import MySQLdb
import commands

from itertools import groupby
from MySQLdb import connect
from MySQLdb import cursors

host=None
port=None
username=None
password=None
database='curve_mds'
targetPath=None
etcd_exporter=None
mds_exporter=None
mysql_exporter=None
snapclone_exporter=None

def loadConf():
    global host,port,username,password,targetPath,etcd_exporter,mds_exporter,mysql_exporter,snapclone_exporter
    conf=ConfigParser.ConfigParser()
    conf.read("target.ini")
    host=conf.get("mysql", "ip")
    port=conf.getint("mysql", "port")
    username=conf.get("mysql", "user")
    password=conf.get("mysql", "pwd")
    targetPath=conf.get("path", "target_path")
    etcd_exporter=conf.get("exporter", "etcd")
    mds_exporter=conf.get("exporter", "mds")
    mysql_exporter=conf.get("exporter", "mysql")
    snapclone_exporter=conf.get("exporter", "snapclone")

def refresh(cur):
    targets = []

    # 获取chunkserver的ip和port
    cur.execute("SELECT internalHostIP,port FROM curve_chunkserver")
    result=cur.fetchall()

    cur.execute("SELECT internalHostIP,hostName FROM curve_server")
    hostnames=cur.fetchall()

    # 添加chunkserver targets
    for t in result:
        count = cur.execute("SELECT hostName FROM curve_server where internalHostIP ='%s'" % t[0])
        if count > 0:
            server=cur.fetchone()
            hostname=server[0]
        else:
            hostname=t[0]
        targets.append({
            'labels': {'job': "chunkserver", 'hostname': hostname},
            'targets': [t[0]+':'+str(t[1])],
        })

    # 添加node_exporter targets
    chunkserverip=set([t[0] for t in result])
    targets.append({
        'labels': {'job': "node_exporter"},
        'targets': [t+':9100' for t in chunkserverip],
    })

    # 获取client的ip和port
    result = commands.getstatusoutput("curve_ops_tool client-list -listClientInRepo=true")
    if result[0] != 0:
        print "curve_ops_tool list client fail!"
        return

    # 添加client targets
    targets.append({
        'labels': {'job': "client"},
        'targets': result[1].split("\n"),
    })

    # 添加mysql exporter targets
    targets.append({
        'labels': {'job': "mysqld"},
        'targets': [mysql_exporter],
    })

    # 添加etcd targets
    targets.append({
        'labels': {'job': "etcd"},
        'targets': etcd_exporter.split(','),
    })

    # 添加mds targets
    targets.append({
        'labels': {'job': "mds"},
        'targets': mds_exporter.split(','),
    })

    # 添加snapclone targets
    targets.append({
        'labels': {'job': "snapclone"},
        'targets': snapclone_exporter.split(','),
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
        db = None
        try:
            db = connect(host=host, user=username, passwd=password, db=database, port=port)
            cur = db.cursor()
            refresh(cur)
        except MySQLdb.Error, e:
            print "MySQL Error:%s" % str(e)
        finally:
            if db:
                cur.close()
                db.close()
        # 每隔30s刷新一次
        time.sleep(30)
