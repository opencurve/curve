#!/usr/bin/env python
# coding=utf-8

import curltool
import database

DELETESNAPSHOT = 'DeleteSnapshot'
CANCELSNAPSHOT = 'CancelSnapshot'

def __check_ok():
    askRes = raw_input('are you sure to delete or cancel(y/n):')
    if askRes == 'y' or askRes == 'yes':
        return True
    elif askRes == 'n' or askRes == 'no':
        return False
    print('Please enter yes/y or no/n')
    return False

def __print_records(records):
    print('%d tasks list to be clean:' % len(records))
    for record in records:
        print('uuid=%s, user=%s' % (record['uuid'], record['user']))

def __del_query(args):
    sql = 'select uuid,user,filename from snapshot where 1=1'
    if args.uuid:
        sql += ' and uuid="%s"' % args.uuid
    elif args.user:
        sql += ' and user="%s"' % args.user
    elif args.filename:
        sql += ' and filename="%s"' % args.filename

    if args.failed:
        sql += ' and status=5'

    db = database.connectDB()
    records = database.queryDB(db, sql)
    if len(records) != 0:
        __print_records(records)
    return records

def __cancel_query(args):
    sql = 'select uuid,user,filename from snapshot where 1=1'
    if args.uuid:
        sql += ' and uuid="%s"' % args.uuid
    elif args.filename:
        sql += ' and filename="%s"' % args.filename
    elif args.user:
        sql += ' and user="%s"' % args.user

    db = database.connectDB()
    records = database.queryDB(db, sql)
    if len(records) != 0:
        __print_records(records)
    return records


def _delete_or_cancel_snapshot_batch(method, records):
    for item in records:
        curltool.delete_or_cancel_snapshot(method, item['uuid'], item['user'], item['filename'])
        print('%s finished' % method)

def __send_request(method, records):
    if len(records) == 0:
        return
    if __check_ok() == False:
        return
    _delete_or_cancel_snapshot_batch(method, records)

def __empty_records_print(records):
    if len(records) == 0:
        print('no eligible snapshot')

def delete_or_cancel_snapshot(method, args):
    records = []
    if method == DELETESNAPSHOT:
        records = __del_query(args)
    else:
        records = __cancel_query(args)

    __empty_records_print(records)
    __send_request(method, records)


