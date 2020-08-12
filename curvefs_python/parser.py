#!/usr/bin/env python
# coding=utf-8

#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import argparse

def get_parser():
    parser = argparse.ArgumentParser(
        prog='curve',
        description='curve tool\n\n',
        )
    parser.add_argument("--confpath", help='config file path of curve', type=str, default='/etc/curve/client.conf')
    subparsers = parser.add_subparsers(dest='optype')

    # create option
    subparser = subparsers.add_parser("create", help="create file")
    subparser.add_argument("--filename", help='name of file', type=str, required=True)
    subparser.add_argument("--length", help='length of file(GB)', type=long, required=True)
    subparser.add_argument("--user", help='user of file', type=str, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    # delete option
    subparser = subparsers.add_parser("delete", help="delete file")
    subparser.add_argument("--user", help='user of file', type=str, required=True)
    subparser.add_argument("--filename", help='name of file', type=str, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    # extend option
    subparser = subparsers.add_parser("extend", help="extend file")
    subparser.add_argument("--user", help='user of file', type=str, required=True)
    subparser.add_argument("--filename", help='name of file', type=str, required=True)
    subparser.add_argument("--length", help='length of file(GB)', type=long, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    # stat option
    subparser = subparsers.add_parser("stat", help="query file info")
    subparser.add_argument("--user", help='user of file', type=str, required=True)
    subparser.add_argument("--filename", help='name of file', type=str, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    # rename option
    subparser = subparsers.add_parser("rename", help="rename file")
    subparser.add_argument("--user", help='user of file', type=str, required=True)
    subparser.add_argument("--filename", help='old name of file', type=str, required=True)
    subparser.add_argument("--newname", help='new name of file', type=str, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    # mkdir option
    subparser = subparsers.add_parser("mkdir", help="create directory")
    subparser.add_argument("--user", help='user of dir', type=str, required=True)
    subparser.add_argument("--dirname", help='name of dir', type=str, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    # rmdir option
    subparser = subparsers.add_parser("rmdir", help="delete directory")
    subparser.add_argument("--user", help='user of dir', type=str, required=True)
    subparser.add_argument("--dirname", help='name of dir', type=str, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    # list option
    subparser = subparsers.add_parser("list", help="list file of dir")
    subparser.add_argument("--user", help='user of dir', type=str, required=True)
    subparser.add_argument("--dirname", help='name of dir', type=str, required=True)
    subparser.add_argument("--password", help='password of user', type=str)

    return parser

def help():
    get_parser().print_help()
