#!/usr/bin/env python
# coding=utf-8

import database
import curltool
import common

status = ['done', 'in-progress', 'recovering', 'cleaning', 'errorCleaning', 'error']
filetype = ['file', 'snapshot']
clonestep = ['createCloneFile', 'createCloneMeta', 'createCloneChunk', 'completeCloneMeta',
                'recoverChunk', 'changeOwner', 'renameCloneFile', 'completeCloneFile', 'end']
tasktype = ["clone", "recover"]

def __get_sql(args):
    sql = "select * from clone where 1=1"
    if args.taskid:
        sql += " and taskid=\"%s\"" % args.taskid

    if args.user:
        sql += " and user=\"%s\"" % args.user

    if args.src:
        sql += " and src=\"%s\"" % args.src

    if args.dest:
        sql += " and dest=\"%s\"" % args.dest

    if args.failed:
        code = status.index('error')
        sql += " and status=%d" % code

    if args.inprogress:
        code = status.index('in-progress')
        sql += " and status=%d" % code

    if args.done:
        code = status.index('done')
        sql += " and status=%d" % code

    if args.clone:
        code = tasktype.index("clone")
        sql += " and tasktype=%d" % code

    if args.recover:
        code = tasktype.index("recover")
        sql += " and tasktype=%d" % code
    return sql

def query_clone_from_db(args):
    sql = __get_sql(args)

    if sql is None:
        return None

    db = database.connectDB()
    records = database.queryDB(db, sql)
    return records

def query_clone_recover(args):
    records = query_clone_from_db(args)
    #挑选出未完成的快照，去snapshotcloneserver查询进度信息
    for record in records:
        code = record['status']
        record['status'] = status[code]
        code = record['tasktype']
        record['tasktype'] = tasktype[code]
        code = record['filetype']
        record['filetype'] = filetype[code]
        code = record['nextstep']
        record['nextstep'] = clonestep[code]


    notes = {}
    heads = ['taskid', 'user', 'tasktype', 'src', 'dest', 'originID', 'destID',
                    'time', 'filetype', 'isLazy', 'nextstep', 'status']
    common.printTable(heads, records, notes)

def clone_recover_status(args):
    records = query_clone_from_db(args)
    statistics = {}
    for record in records:
        code = record['status']
        status_name = status[code]
        if statistics.has_key(status_name):
            statistics[status_name].append(record['taskid'])
        else:
            statistics[status_name] = [record['taskid']]

    for k,v in statistics.items():
        print("%s : %d" % (k, len(v)))
        if args.detail:
            print("%s" %"\n".join(v))
