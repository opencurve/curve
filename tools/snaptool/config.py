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

__loadedFile = False
__configData = {}

def load(fileName):
    global __configData

    try:
        f = open(fileName)
        for line in f:
            line = line.strip()

            if len(line) <1:
                continue
            if line[0]=="#":
                continue
            elif line[0]==";":
                 continue
            try:
               pos = line.index('=')
            except ValueError, e:
               print "Wrong config: " % line
               sys.exit(1)
            key = line[:pos].strip()
            value = line[pos+1:].strip()
            __configData[key] = value
    except Exception, e:
        print "Failed to open :%s" % fileName
        sys.exit(1)

def get(key):
    global __configData
    return __configData[key]


