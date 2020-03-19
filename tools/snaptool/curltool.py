#!/usr/bin/env python
# coding=utf-8
import config
import requests
import json

errcodelist = {
    '0': 'Exec success',
    '-1': 'Internal error',
    '-2': 'Server init fail',
    '-3': 'Server start fail',
    '-4': 'Service is stop',
    '-5': 'BadRequest: Invalid request',
    '-6': 'Task already exist',
    '-7': 'Invalid user',
    '-8': 'File not exist',
    '-9': 'File status invalid',
    '-10': 'Chunk size not aligned',
    '-11': 'FileName not match',
    '-12': 'Cannot delete unfinished',
    '-13': 'Cannot create when has error',
    '-14': 'Cannot cancel finished',
    '-15': 'Invalid snapshot',
    '-16': 'Cannot delete when using',
    '-17': 'Cannot clean task unfinished',
    '-18': 'Snapshot count reach the limit',
    '-19': 'File exist',
    '-20': 'Task is full',
}

def encodeParam(params):
    url = ''
    for (key, value) in params.items():
        url += key
        url += '='
        url += value
        url += '&'
    url = url[:-1]
    return url

def query(params):
    serverhost = config.get('server.address')
    encoded_params = encodeParam(params)
    url = "http://%s/SnapshotCloneService?%s" % (serverhost, encoded_params)
    ret = requests.get(url)
    return ret.json()

def query_snapshot_progress(user, filename, uuid):
    params = {'Action':'GetFileSnapshotInfo', 'Version':'0.0.6', 'User':user, 'File':filename, 'UUID':uuid}
    jsonobj = query(params)
    if jsonobj['Code'] != '0' or jsonobj['TotalCount'] != 1:
        return -1
    snapInfo = jsonobj['Snapshots'][0]
    return snapInfo['Progress']

def delete_or_cancel_snapshot(method, uuid, user, file):
    params = None
    if user and uuid:
        params = {'Action':method, 'Version':'0.0.6', 'User':user, 'UUID':uuid, 'File':file}
    if params is None:
        return -1

    jsonobj = query(params)
    if jsonobj['Code'] != '0':
        print('%s uuid=%s, user=%s failed, ecode=%s, etext=%s' % (method, uuid, user, jsonobj['Code'], errcodelist[jsonobj['Code']]))
        return -1
    print('%s uuid=%s, user=%s success' % (method, uuid, user))
    return 0

def clean_clone_or_recover(taskid, user):
    params = None
    if taskid or user:
        params = {'Action':'CleanCloneTask', 'Version':'0.0.6', 'User':user, 'UUID':taskid}
    if params is None:
        return -1

    jsonobj = query(params)
    if jsonobj['Code'] != '0':
        print('clean taskid=%s, user=%s failed, ecode=%s, etext=%s' % (taskid, user, jsonobj['Code'], errcodelist[jsonobj['Code']]))
        return -1
    print('clean taskid=%s, user=%s success' % (taskid, user))
    return 0

def create_snapshot(user, filename, snapshotname):
    if user is None or filename is None or snapshotname is None:
        print('user, filename, snapshotname need')
        return

    params = {'Action':'CreateSnapshot', 'Version':'0.0.6', 'User':user, 'File':filename, 'Name':snapshotname}
    jsonobj = query(params)
    if jsonobj['Code'] != '0':
        print("create snapshot fail, ecode=%s, etext=%s" % (jsonobj['Code'], errcodelist[jsonobj['Code']]))
        return
    print("create snapshot success, UUID=%s" % jsonobj['UUID'])

def clone_or_recover(type, user, src, dest, lazy):
    if user is None or src is None or dest is None or lazy is None:
        print('user, src, dest, lazy need')
        return

    params = ''
    if type == "clone":
        params = {'Action':'Clone', 'Version':'0.0.6', 'User':user, 'Source':src, 'Destination':dest, 'Lazy':lazy}
    else:
        params = {'Action':'Recover', 'Version':'0.0.6', 'User':user, 'Source':src, 'Destination':dest, 'Lazy':lazy}

    jsonobj = query(params)
    if jsonobj['Code'] != '0':
        print("%s fail, ecode=%s, etest=%s" % (type, jsonobj['Code'], errcodelist[jsonobj['Code']]))
        return
    print("%s success. UUID=%s" % (type, jsonobj['UUID']))



