#!/usr/bin/env python
# coding=utf-8

import curltool

CLEANCLONE = 'CleanCloneTask'

def __check_ok():
    askRes = raw_input('are you sure to clean(yes/no):')
    if askRes == 'yes':
        return True
    elif askRes == 'no':
        return False
    print('Please enter yes or no')
    return False

def __print_records(records):
    print('%d tasks list to be clean:' % len(records))
    for record in records:
        print('UUID=%s, User=%s, Src=%s, Dest=%s' % (record['UUID'], record['User'], record['Src'], record['File']))

def __query_clone_or_recover_by(args, tasktype):
    status = None
    if args.failed:
        status = 5
    totalCount, records = curltool.get_clone_list_all(args.user, args.src, args.dest, args.taskid, tasktype, status)

    if totalCount != 0:
        __print_records(records)
    return records

def __clean_clone_or_recover_batch(batchlist):
    for item in batchlist:
        curltool.clean_clone_or_recover(item['UUID'], item['User'])
        print('clean finished')

def __empty_records_print(records):
    if (records is None) or len(records) == 0:
        print('no task to clean')

def __send_request(records):
    if (records is None) or len(records) == 0:
        return
    if __check_ok() == False:
        return
    __clean_clone_or_recover_batch(records)

def clean_clone_or_recover(tasktype, args):
    records = __query_clone_or_recover_by(args, tasktype)
    __empty_records_print(records)
    __send_request(records)




