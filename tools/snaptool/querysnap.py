#!/usr/bin/env python
# coding=utf-8

import curltool
import common
import time

status = ['done', 'in-progress', 'deleting', 'errorDeleting', 'canceling', 'error']

def __get_status(args):
    if args.status:
        return status.index(args.status)
    return None

def query_snapshot(args):
    totalCount, records = curltool.get_snapshot_list_all(args.user, args.filename, args.uuid, __get_status(args))
    if records == None or len(records) == 0:
        print "no record found"
        return

    for record in records:
        code = record['Status']
        record['Status'] = status[code]
        time_temp = record['Time']
        record['Time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_temp/1000000))
        record['FileLength(GB)'] = record['FileLength'] / 1024 / 1024 / 1024;

    notes = {}
    heads = ['UUID', 'User', 'File', 'SeqNum', 'FileLength(GB)', 'Time', 'Status', 'Name', 'Progress']
    common.printTable(heads, records, notes)

def snapshot_status(args):
    totalCount, records = curltool.get_snapshot_list_all(args.user, args.filename)
    if records == None or len(records) == 0:
        print "no record found"
        return
    statistics = {}
    for i in status:
        statistics[i] = 0
    for record in records:
        code = record['Status']
        status_name = status[code]
        statistics[status_name] =  statistics[status_name] + 1

    for k,v in statistics.items():
        print("%s : %d" % (k, v))

