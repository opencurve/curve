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

def printTable(heads, records, notes):
    seperatorLen = 1
    maxLens = {}
    for head in heads:
        maxLens[head] = len(str(head))
    for record in records:
        for head in heads:
            word = str(record[head])
            if notes and notes.has_key(head) and notes[head].has_key(word):
                word = notes[head][word]
            if len(word) > maxLens[head]:
                maxLens[head] = len(word)

    headline = ''
    for head in heads:
        word = str(head)
        while len(word) < maxLens[head] + seperatorLen:
            word = ' ' + word
        headline = headline + word

    linelen = len(headline)
    breakline = ''
    while len(breakline) < linelen:
        breakline = breakline + '-'
    print breakline
    print headline
    print breakline

    for record in records:
        recordline = ''
        for head in heads:
            word = str(record[head])
            if notes and notes.has_key(head) and notes[head].has_key(word):
                word = notes[head][word]
            while len(word) < maxLens[head] + seperatorLen:
                word = ' ' + word
            recordline = recordline + word
        print recordline
    print breakline
    print "Total: %d" % len(records)
