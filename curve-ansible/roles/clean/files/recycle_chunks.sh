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


help="usage: sh recycle_chunks.sh -d /data -chunks chunkfilepool -wals walfilepool"
dataDir=
chunksRecycleTo=
walsRecycleTo=
maxChunkId=0
maxSegmentId=0

function chunkfilepoolinfo() {
    chunkfilepool=${dataDir}/chunkserver$1/${chunksRecycleTo}
    if [ ! -d $chunkfilepool ]
    then
        echo "no $chunkfilepool"
        return
    fi

    # chunkfilepool
    maxChunkId=`ls -vr $chunkfilepool | head -1`
    leftChunkNum=`ls $chunkfilepool | wc -l`

    # chunknum to be recycled
    recycleSum=0
    for copyset in `ls ${dataDir}/chunkserver$1/copysets`
    do
        chunkDir=${dataDir}/chunkserver$1/copysets/${copyset}/data
        one=`ls $chunkDir | wc -l`
        recycleSum=`expr $recycleSum + $one`
    done

    echo chunkserver$1, maxChunkId=$maxChunkId, leftChunkNum=$leftChunkNum, canRecycle=$recycleSum

}

function walpoolinfo() {
    walfilepool=${dataDir}/chunkserver$1/${walsRecycleTo}
    if [ ! -d $walfilepool ]
    then
        echo "no $walfilepool"
        return
    fi
    # walfilepool
    maxSegmentId=`ls -vr $walfilepool | head -1`
    leftSegmentNum=`ls $walfilepool | wc -l`

    # segmentnum to be recycled
    recycleSum=0
    for copyset in `ls ${dataDir}/chunkserver$1/copysets`
    do
        logDir=${dataDir}/chunkserver$1/copysets/${copyset}/log
        one=`ls $logDir | grep curve_log | wc -l`
        recycleSum=`expr $recycleSum + $one`
    done

    echo chunkserver$1, maxSegmentId=$maxSegmentId, leftSegmentNum=$leftSegmentNum, canRecycle=$recycleSum
}

function recycle_copysets() {
    rchunkSum=0
    rsegmentSum=0

    # recycle chunks
    maxChunkId=`ls -vr $chunkfilepool | head -1`
    for copyset in `ls $2`
    do
        copysetPath=${dataDir}/chunkserver$1/copysets/${copyset}
        chunkfilepool=${dataDir}/chunkserver$1/${chunksRecycleTo}

        chunks=`ls $copysetPath/data`
        for chunk in $chunks
        do
            maxChunkId=`expr $maxChunkId + 1`
            sudo mv $copysetPath/data/${chunk} $chunkfilepool/${maxChunkId}
            rchunkSum=`expr $rchunkSum + 1`
        done
    done
    echo chunkserver$1 actual recycle under data chunk=$rchunkSum

    # recycle wal segments
    maxSegmentId=`ls -vr $walfilepool | head -1`
    for copyset in `ls $2`
    do
        copysetPath=${dataDir}/chunkserver$1/copysets/${copyset}
        walfilepool=${dataDir}/chunkserver$1/${walsRecycleTo}

        segments=`ls $copysetPath/log | grep curve_log`
        for segment in $segments
        do
            maxSegmentId=`expr $maxSegmentId + 1`
            sudo mv $copysetPath/log/${segment} $walfilepool/${maxSegmentId}
            rsegmentSum=`expr $rsegmentSum + 1`
        done
    done
    echo chunkserver$1 actual recycle under log segment=$rsegmentSum

    # recycle chunks in raft_snapshot
    rchunkSum=0
    maxChunkId=`ls -vr $chunkfilepool | head -1`
    for copyset in `ls $2`
    do
        copysetPath=${dataDir}/chunkserver$1/copysets/${copyset}
        chunkfilepool=${dataDir}/chunkserver$1/${chunksRecycleTo}
        chunksize=`ls -l $chunkfilepool | tail -1 | awk -F" " '{print $5}'`

        chunks=`sudo ls -l $copysetPath/raft_snapshot | grep $chunksize | awk -F" " '{print $9}'`
        for chunk in $chunks
        do
            maxChunkId=`expr $maxChunkId + 1`
            sudo mv $copysetPath/raft_snapshot/${chunk} $chunkfilepool/${maxChunkId}
            rchunkSum=`expr $rchunkSum + 1`
        done

        if [ -d "$copysetPath/raft_snapshot/temp" ];then
            chunks=`sudo ls -l $copysetPath/raft_snapshot/temp | grep $chunksize | awk -F" " '{print $9}'`
            for chunk in $chunks
            do
                maxChunkId=`expr $maxChunkId + 1`
                sudo mv $copysetPath/raft_snapshot/temp/${chunk} $chunkfilepool/${maxChunkId}
                rchunkSum=`expr $rchunkSum + 1`
            done
        fi
    done
    echo chunkserver$1 actual recycle under raft_snapshot chunk=$rchunkSum
}

function recycle() {
    # recycle chunk/segment/snapshot under copysets
    if [ ! -d ${dataDir}/chunkserver$1/copysets ]
    then
        return
    fi
    copysets=${dataDir}/chunkserver$1/copysets
    recycle_copysets $1 $copysets
    sudo rm -rf ${dataDir}/chunkserver$1/copysets/

    # recycle chunk/segment/snapshot under trash
    if [ ! -d ${dataDir}/chunkserver$1/recycler ]
    then
        return
    fi
    recycler=${dataDir}/chunkserver$1/recycler
    recycle_copysets $1 $recycler
    sudo rm -rf ${dataDir}/chunkserver$1/recycler
}

function parse() {
    if [[ "$1" != "-d" || "$3" != "-chunks" || "$5" != "-wals" ]]
    then
        echo "$help"
        exit 0
    fi

    dataDir=$2
    chunksRecycleTo=$4
    walsRecycleTo=$6
}

main() {
    # check params
    if [[ "$1" = "-h" || $# -ne 6 ]]
    then
        echo "$help"
        exit 0
    fi

    # parse params
    parse $@

    # recycle all chunks and wals in chunkserver
    for i in `ls ${dataDir} |grep chunkserver| sed 's/[^0-9]//g'`
    do
        chunkfilepoolinfo $i
        walpoolinfo $i
        recycle $i &
    done

    wait
}

main $@