#!/usr/bin/env python
# coding=utf-8

import curltool
import common
import time

status = ['done', 'cloning', 'recovering', 'cleaning',
          'errorCleaning', 'error', 'retrying', 'metaInstalled']
filetype = ['file', 'snapshot']
clonestep = ['createCloneFile', 'createCloneMeta', 'createCloneChunk', 'completeCloneMeta',
             'recoverChunk', 'changeOwner', 'renameCloneFile', 'completeCloneFile', 'end']
tasktype = ["clone", "recover"]
islazy = ["notlazy", "lazy"]


def __get_status(args):
    if args.status:
        return status.index(args.status)
    return None


def __get_type(args):
    if args.clone:
        return tasktype.index("clone")
    if args.recover:
        return tasktype.index("recover")
    return None


def query_clone_recover(args):
    totalCount, records = curltool.get_clone_list_all(
        args.user, args.src, args.dest, args.taskid, __get_type(args), __get_status(args))
    if totalCount == 0:
        print "no record found"
        return
    # Improving Print Readability
    for record in records:
        code = record['TaskStatus']
        record['TaskStatus'] = status[code]
        code = record['TaskType']
        record['TaskType'] = tasktype[code]
        code = record['FileType']
        record['FileType'] = filetype[code]
        code = record['NextStep']
        record['NextStep'] = clonestep[code]
        code = record['IsLazy']
        record['IsLazy'] = islazy[code]
        time_temp = record['Time']
        record['Time'] = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(time_temp/1000000))

    notes = {}
    heads = ['UUID', 'User', 'TaskType', 'Src', 'File',
             'Time', 'FileType', 'IsLazy', 'NextStep', 'TaskStatus', 'Progress']
    common.printTable(heads, records, notes)


def clone_recover_status(args):
    totalCount, records = curltool.get_clone_list_all(
        args.user, args.src, args.dest, None, __get_type(args))
    if totalCount == 0:
        print "no record found"
        return
    clone_statistics = {}
    recover_statistics = {}
    for record in records:
        code = record['TaskStatus']
        status_name = status[code]
        if record['TaskType'] == tasktype.index("clone"):
            if clone_statistics.has_key(status_name):
                clone_statistics[status_name].append(record['UUID'])
            else:
                clone_statistics[status_name] = [record['UUID']]
        else:
            if recover_statistics.has_key(status_name):
                recover_statistics[status_name].append(record['UUID'])
            else:
                recover_statistics[status_name] = [record['UUID']]
    if clone_statistics:
        print "clone status:"
        for k, v in clone_statistics.items():
            print("%s : %d" % (k, len(v)))

    if recover_statistics:
        print "recover status:"
        for k, v in recover_statistics.items():
            print("%s : %d" % (k, len(v)))
