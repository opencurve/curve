#!/bin/bash

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

dataDir=
maxChunkId=0
maxSegmentId=0

function get_max_chunkid_inpool() {
    ret=`ls -lvr ${dataDir}/chunkserver$1/chunkfilepool`
    if [ ! -z "$ret" ]
    then
        maxChunkId=${ret[0]}
    fi
}

function get_max_segmentid_inpool() {
    if [ ! -d ${dataDir}/chunkserver$1/walfilepool ]
    then
        return
    fi
    ret=`ls -lvr ${dataDir}/chunkserver$1/walfilepool`
    if [ ! -z "$ret" ]
    then
        maxSegmentId=${ret[0]}
    fi
}

function recycle_copysets() {
    for copyset in $1
    do
        # recycle chunks
        chunks=`ls ${copyset}/data`
        for chunk in $chunks
        do
            maxChunkId=$(($maxChunkId+1))
            sudo mv ${copyset}/data/${chunk} ../chunkfilepool/${maxChunkId}
        done
        # recycle wal segments
        segments=`ls ${copyset}/log | grep curve_log`
        for segment in $segments
        do
            maxSegmentId=$(($maxSegmentId+1))
            sudo mv ${copyset}/log/${segment} ../chunkfilepool/${maxSegmentId}
        done
    done
}

function recycle() {
    # recycle chunk and segment under copysets
    if [ ! -d ${dataDir}/chunkserver$1/copysets ]
    then
        return
    fi
    cd ${dataDir}/chunkserver$1/copysets/
    copysets=`ls`
    recycle_copysets $copysets
    cd ..
    sudo rm -rf copysets
    # recycle chunk and segment under trash
    if [ ! -d ${dataDir}/chunkserver$1/recycler ]
    then
        return
    fi
    cd ${dataDir}/chunkserver$1/recycler
    copysets=`ls`
    recycle_copysets $copysets
    cd ..
    sudo rm -rf recycler
}

if [ $# -lt 2 ]
then
    echo "please give the dataDir"
    exit 1
fi
dataDir=$2

ret=`ls ${dataDir} |grep chunkserver| sed 's/[^0-9]//g'`
for i in $ret
do
    get_max_chunkid_inpool $i
    get_max_segmentid_inpool $i
    recycle $i
done
