#!/usr/bin/env python
# -*- coding: utf-8 -*-


import MySQLdb

from config import config
from logger.logger import *


def conn_db(db_host, db_port, db_user, db_pass, db_name):
    conn = MySQLdb.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        passwd=db_pass,
        db=db_name)
    return conn

def query_db(conn, sql):
    #  print "Run SQL: %s" % sql
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    #  print "Rows selected:", cursor.rowcount

    data = []
    rowcount = cursor.rowcount
    for row in cursor.fetchall():
        data.append(row)
    cursor.close()
    return_data = {"data": data, "rowcount": rowcount}
    return return_data

def exec_sql(conn, sql):
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
    except:
        logger.debug("conn %s cmd %s failed."%(conn, sql))
        conn.rollback()
    conn.close()

def exec_sql_file(conn, filepath):
    cursor = conn.cursor()
    with open(filepath) as f:
        # 读取整个sql文件，以分号切割。[:-1]删除最后一个元素，也就是空字符串
        sql_list = f.read().split(';')[:-1]
        for x in sql_list:
            # 判断包含空行的
            if '\n' in x:
                # 替换空行为1个空格
                x = x.replace('\n', ' ')

            # sql语句添加分号结尾
            sql_item = x+';'
            # print(sql_item)
            cursor.execute(sql_item)
            conn.commit()
    conn.close()


def get_db_info(table, *select_key_info, **condition_info):
    '''
    检查数据库信息
    '''
    conn = conn_db(
        config.db_host,
        config.db_port,
        config.db_user,
        config.db_pass,
        config.db_name)

    select_key = ""
    select_key_count = 0
    for t_key in select_key_info:
        if select_key_count < len(select_key_info) - 1:
            select_key = select_key + t_key + ","
        else:
            select_key = select_key + t_key

    condition = ""
    condition_count = 0
    for t_condition in condition_info:
        if condition_count < len(condition_info) - 1:
            condition = condition + "%s = %s" % (t_condition, condition_info[t_condition]) + "and"
        else:
            condition = condition + "%s = %s" % (t_condition, condition_info[t_condition])
        condition_count

    sql = "select %s from %s where %s" % (select_key, table, condition)
    data = query_db(conn, sql)
    conn.close()
    print data
    return data


