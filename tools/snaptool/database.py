#!/usr/bin/env python
# coding=utf-8

import os
import sys
import time
import MySQLdb
import config

def connectDB():
    dbhost = config.get('metastore.db_address')
    dbuser = config.get('metastore.db_user')
    dbpass = config.get('metastore.db_passwd')
    dbname = config.get('metastore.db_name')
    db = MySQLdb.connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
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


