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

#!/usr/bin/env python
# coding=utf-8

import os
import sys
import time
import json
import database
import curltool
import help
import config
import delete_or_cancel_snap
import querysnap
import queryclone
import clone_or_recover

if __name__ == '__main__':
    args = help.get_parser().parse_args()
    config.load(args.confpath)

    if args.optype == "delete-snapshot":
        delete_or_cancel_snap.delete_or_cancel_snapshot(delete_or_cancel_snap.DELETESNAPSHOT, args)
    elif args.optype == "cancel-snapshot":
        delete_or_cancel_snap.delete_or_cancel_snapshot(delete_or_cancel_snap.CANCELSNAPSHOT, args)
    elif args.optype == "clean-recover":
        clone_or_recover.clean_clone_or_recover(1, args)
    elif args.optype == "clean-clone":
        clone_or_recover.clean_clone_or_recover(0, args)
    elif args.optype == "query-snapshot":
        querysnap.query_snapshot(args)
    elif args.optype == "snapshot-status":
        querysnap.snapshot_status(args)
    elif args.optype == "query-clone-recover":
        queryclone.query_clone_recover(args)
    elif args.optype == "clone-recover-status":
        queryclone.clone_recover_status(args)
    elif args.optype == "create-snapshot":
        curltool.create_snapshot(args.user, args.filename, args.snapshotname)
    elif args.optype == "clone" or args.optype == "recover":
        curltool.clone_or_recover(args.optype, args.user, args.src, args.dest, args.lazy)
    elif args.optype == "flatten" :
        curltool.flatten(args.user, args.taskid)


