#!/usr/bin/env python
# coding=utf-8

import database
import curltool
import common

status = ['done', 'in-progress', 'deleting', 'errorDeleting', 'canceling', 'error']

def __get_sql(args):
    sql = "select * from snapshot where 1=1"
    if args.uuid:
        sql += " and uuid=\"%s\"" % args.uuid

    if args.user:
        sql += " and user=\"%s\"" % args.user

    if args.filename:
        sql += " and filename=\"%s\"" % args.filename

    if args.failed:
        code = status.index('error')
        sql += " and status=%d" % code

    if args.inprogress:
        code = status.index('in-progress')
        sql += " and status=%d" % code

    if args.done:
        code = status.index('done')
        sql += " and status=%d" % code
    return sql

def query_snapshot_from_db(args):
    sql = __get_sql(args)

    if sql is None:
        return None

    db = database.connectDB()
    records = database.queryDB(db, sql)
    return records


def query_snapshot(args):
    records = query_snapshot_from_db(args)
    #挑选出未完成的快照，去snapshotcloneserver查询进度信息
    for record in records:
        code = record['status']
        record['status'] = status[code]
        if record['status'] == 'done':
            record['progress'] = 100
        elif record['status'] == 'error':
            record['progress'] = 0
        else :
            progress = curltool.query_snapshot_progress(record['user'], record['filename'], record['uuid'])
            record['progress'] = progress

    notes = {}
    heads = ['uuid', 'user', 'filename', 'seqnum', 'filelength', 'time', 'status', 'snapdesc', 'progress']
    common.printTable(heads, records, notes)

def snapshot_status(args):
    records = query_snapshot_from_db(args)
    statistics = {}
    for record in records:
        code = record['status']
        status_name = status[code]
        if statistics.has_key(status_name):
            statistics[status_name].append(record['uuid'])
        else:
            statistics[status_name] = [record['uuid']]

    for k,v in statistics.items():
        print("%s : %d" % (k, len(v)))
        if args.detail:
            print("%s" %"\n".join(v)) 
