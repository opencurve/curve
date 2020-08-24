#!/usr/bin/env python
# coding=utf-8

import curltool

DELETESNAPSHOT = 'DeleteSnapshot'
CANCELSNAPSHOT = 'CancelSnapshot'

def __check_ok():
    askRes = raw_input('are you sure to delete or cancel(yes/no):')
    if askRes == 'yes':
        return True
    elif askRes == 'no':
        return False
    print('Please enter yes or no')
    return False

def __print_records(records):
    print('%d tasks list to be clean:' % len(records))
    for record in records:
        print('uuid=%s, user=%s, file=%s, status=%s' % (record['UUID'], record['User'], record['File'], record['Status']))

def __del_query(args):
    status = None
    if args.failed:
        status = 5
    totalCount, records = curltool.get_snapshot_list_all(args.user, args.filename, args.uuid, status)
    if totalCount != 0:
        __print_records(records)
    return records

def __cancel_query(args):
    totalCount, records = curltool.get_snapshot_list_all(args.user, args.filename, args.uuid)
    if totalCount != 0:
        __print_records(records)
    return records


def _delete_or_cancel_snapshot_batch(method, records):
    for item in records:
        curltool.delete_or_cancel_snapshot(method, item['UUID'], item['User'], item['File'])
        print('%s finished' % method)

def __send_request(method, records):
    if (records is None) or len(records) == 0:
        return
    if __check_ok() == False:
        return
    _delete_or_cancel_snapshot_batch(method, records)

def __empty_records_print(records):
    if (records is None) or len(records) == 0:
        print('no eligible snapshot')

def delete_or_cancel_snapshot(method, args):
    records = []
    if method == DELETESNAPSHOT:
        records = __del_query(args)
    else:
        records = __cancel_query(args)

    __empty_records_print(records)
    __send_request(method, records)


