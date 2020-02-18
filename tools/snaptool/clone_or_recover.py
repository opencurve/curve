#!/usr/bin/env python
# coding=utf-8

import curltool
import database

CLEANCLONE = 'CleanCloneTask'

def __check_ok():
    askRes = raw_input('are you sure to clean(y/n):')
    if askRes == 'y' or askRes == 'yes':
        return True
    elif askRes == 'n' or askRes == 'no':
        return False
    print('Please enter yes/y or no/n')
    return False

def __print_records(records):
    print('%d tasks list to be clean:' % len(records))
    for record in records:
        print('taskid=%s, user=%s' % (record['taskid'], record['user']))

def __query_clone_or_recover_by(args, tasktype):
    sql = 'select taskid,user from clone where tasktype=%d' % tasktype
    if args.taskid:
        sql +=  ' and taskid="%s"' % args.taskid
    elif args.dest:
        sql += ' and dest="%s"' % args.dest
    elif args.src:
        sql += 'and src="%s"' % args.src
    elif args.user:
        sql += ' and user="%s"' % args.user

    if args.failed:
        sql += " and status=5"

    db = database.connectDB()
    records = database.queryDB(db, sql)
    if len(records) != 0:
        __print_records(records)
    return records

def __clean_clone_or_recover_batch(batchlist):
    for item in batchlist:
        curltool.clean_clone_or_recover(item['taskid'], item['user'])
        print('clean finished')

def __empty_records_print(records):
    if len(records) == 0:
        print('no task to clean')

def __send_request(records):
    if len(records) == 0:
        return
    if __check_ok() == False:
        return
    __clean_clone_or_recover_batch(records)

def clean_clone_or_recover(tasktype, args):
    records = __query_clone_or_recover_by(args, tasktype)
    __empty_records_print(records)
    __send_request(records)




