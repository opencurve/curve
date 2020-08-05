#!/usr/bin/env python
# coding=utf-8

import argparse

def get_parser():
    parser = argparse.ArgumentParser(
        prog='snaptool',
        description='snapshot tool\n\n',
        )
    parser.add_argument("--confpath", help='config file path of snaptool', type=str, default='/etc/curve/snapshot_tools.conf')
    subparsers = parser.add_subparsers(dest='optype')

    # query-snapshot option
    subparser = subparsers.add_parser("query-snapshot", help="query snapshot by condition")
    subparser.add_argument("--uuid", help='query snapshot by uuid', type=str)
    subparser.add_argument("--user", help='query snapshot by user', type=str)
    subparser.add_argument("--filename", help='query snapshot by filename', type=str)
    subparser.add_argument("--status", help='query snapshot by status', type=str, choices=['done', 'in-progress', 'deleting', 'errorDeleting', 'canceling', 'error'])

    # query-clone-recover option
    subparser = subparsers.add_parser("query-clone-recover", help="query clone and recover task by condition")
    group =subparser.add_mutually_exclusive_group(required=True)
    group.add_argument("--src", help='query clone and recover task by source', type=str)
    group.add_argument("--dest", help='query clone and recover task by destinition', type=str)
    group.add_argument("--user", help='query clone and recover task by user', type=str)
    group.add_argument("--taskid", help='query clone and recover task by taskid', type=str)
    group.add_argument("--status", help='query clone and recover task by status', type=str, choices=['done', 'cloning', 'recovering', 'cleaning', 'errorCleaning', 'error', 'retrying', 'metaInstalled'])
    group.add_argument("--all", help='query clone and recover task all', action='store_true')
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--clone", help='only query clone task', action='store_true')
    group.add_argument("--recover", help='only query recover task', action='store_true')

    # snapshot-status option
    subparser = subparsers.add_parser("snapshot-status", help="show the snapshot status")
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--user", help='show the snapshot status by user', type=str)
    group.add_argument("--filename", help='show the snapshot status by filename', type=str)

    # clone-recover-status option
    subparser = subparsers.add_parser("clone-recover-status", help="query clone and recover task by condition")
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--src", help='show the clone recover status by source', type=str)
    group.add_argument("--dest", help='show the clone recover status by destinition', type=str)
    group.add_argument("--user", help='show the clone recover status by user', type=str)
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--clone", help='only query clone task', action='store_true')
    group.add_argument("--recover", help='only query recover task', action='store_true')

    # delete-snapshot option
    subparser = subparsers.add_parser("delete-snapshot", help="delete snapshot by condition")
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--uuid", help='delete snapshot by uuid', type=str)
    group.add_argument("--user", help='delete snapshot by user', type=str)
    group.add_argument("--filename", help='delete snapshot by filename', type=str)
    group.add_argument("--all", help='delete  all snapshots', action='store_true')
    subparser.add_argument("--failed", help='delete failed snapshot', action='store_true')

    #  cancel-snapshot option
    subparser = subparsers.add_parser("cancel-snapshot", help="cancel snapshot by condition")
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--uuid", help='delete snapshot by uuid', type=str)
    group.add_argument("--user", help='delete snapshot by user', type=str)
    group.add_argument("--filename", help='delete snapshot by filename', type=str)
    group.add_argument("--all", help='delete  all snapshots', action='store_true')

    # clean-recover option
    subparser = subparsers.add_parser("clean-recover", help="clean recover by condition")
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--taskid", help='clean recover by taskid', type=str)
    group.add_argument("--user", help='clean recover by user', type=str)
    group.add_argument("--src", help='clean recover by src', type=str)
    group.add_argument("--dest", help='clean recover by dest', type=str)
    group.add_argument("--all", help='clean all recovers', action='store_true')
    subparser.add_argument("--failed", help='clean failed recover', action='store_true')

    # clean-clone option
    subparser = subparsers.add_parser("clean-clone", help="delete clone by condition")
    group =subparser.add_mutually_exclusive_group()
    group.add_argument("--taskid", help='clean clone by taskid', type=str)
    group.add_argument("--user", help='clean clone by user', type=str)
    group.add_argument("--src", help='clean clone by src', type=str)
    group.add_argument("--dest", help='clean clone by dest', type=str)
    group.add_argument("--all", help='clean all clones', action='store_true')
    subparser.add_argument("--failed", help='clean failed clone', action='store_true')

    # create-snapshot option
    subparser = subparsers.add_parser("create-snapshot", help="create a snapshot")
    subparser.add_argument("--user", help='user who need to create', type=str, required=True)
    subparser.add_argument("--filename", help='file witch need to do snap', type=str, required=True)
    subparser.add_argument("--snapshotname", help='snapshot name', type=str, required=True)

    # create-clone option
    subparser = subparsers.add_parser("clone", help="do clone")
    subparser.add_argument("--user", help='user who need to clone', type=str, required=True)
    subparser.add_argument("--src", help='source filename or  uuid', type=str, required=True)
    subparser.add_argument("--dest", help='dest file name', type=str, required=True)
    subparser.add_argument("--lazy", help='need lazy', type=str, choices=["true", "false"], required=True)

    # create-recover option
    subparser = subparsers.add_parser("recover", help="do recover")
    subparser.add_argument("--user", help='user who need to recover', type=str, required=True)
    subparser.add_argument("--src", help='source filename or  uuid', type=str, required=True)
    subparser.add_argument("--dest", help='dest file name', type=str, required=True)
    subparser.add_argument("--lazy", help='need lazy', type=str, choices=["true", "false"], required=True)

    # flatten option
    subparser =  subparsers.add_parser("flatten", help="do flatten lazy clone/recover")
    subparser.add_argument("--user", help='user', type=str, required=True)
    subparser.add_argument("--taskid", help='lazy clone/recover task id', type=str, required=True)

    return parser

def help():
    get_parser().print_usage()
