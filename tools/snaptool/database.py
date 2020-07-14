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

#!/usr/bin/env python
# coding=utf-8

import os
import sys
import time
import MySQLdb
import config

def connectDB():
    dbhost = config.get('metastore.db_host')
    dbport = config.get('metastore.db_port')
    dbuser = config.get('metastore.db_user')
    dbpass = config.get('metastore.db_passwd')
    dbname = config.get('metastore.db_name')
    db = MySQLdb.connect(host=dbhost, port=int(dbport), user=dbuser, passwd=dbpass, db=dbname)
    return db

def queryDB(db, sql):
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = []
    for row in cursor.fetchall():
        rows.append(row)
    cursor.close()
    return rows

def updateDB(db, sql, params):
    cursor = db.cursor()
    n = cursor.execute(sql,params)
    cursor.close()
    db.commit()
    return n


