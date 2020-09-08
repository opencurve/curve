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

# default config path
confPath=/etc/curve/chunkserver.conf
DATA_DIR=/data
source ./chunkserver_deploy.sh

# 使用方式
function help() {
    echo "COMMANDS:"
    echo "    start   :  start chunkserver"
    echo "    stop    :  stop chunkserver"
    echo "    restart : restart chunkserver"
    echo "    status  : show the online status of chunkserver"
    echo "    deploy  : prepare the chunkserver running environment"
    echo "    deploy-wal-pool  : prepare the wal pool"
    echo "    format  : format the chunkfile pool"
    echo "USAGE:"
    echo "    start all chunkservers              : ./chunkserver_ctl.sh start all"
    echo "    start one chunkserver               : ./chunkserver_ctl.sh start {\$chunkserverId}"
    echo "    stop all chunkservers               : ./chunkserver_ctl.sh stop all"
    echo "    stop one chunkserver                : ./chunkserver_ctl.sh stop {\$chunkserverId}"
    echo "    restart all chunkservers            : ./chunkserver_ctl.sh restart all"
    echo "    restart one chunkserver             : ./chunkserver_ctl.sh restart {\$chunkserverId}"
    echo "    show the status of all chunkservers : ./chunkserver_ctl.sh status all"
    echo "    show the status of one chunkserver  : ./chunkserver_ctl.sh status {\$chunkserverId}"
    echo "    record uuid meta in all disks       : ./chunkserver_ctl.sh record-meta"
    echo "    deploy all disk                     : ./chunkserver_ctl.sh deploy all"
    echo "    deploy one disk                     : ./chunkserver_ctl.sh deploy /dev/sd{id} /data/chunkserver{id}"
    echo "    deploy all wal pool                 : ./chunkserver_ctl.sh deploy-wal-pool all"
    echo "    deploy one wal pool                 : ./chunkserver_ctl.sh deploy-wal-pool {\$chunkserverId}"
    echo "    deploy all chunk pool               : ./chunkserver_ctl.sh deploy-chunk-pool all"
    echo "    deploy one chunk pool               : ./chunkserver_ctl.sh deploy-chunk-pool {\$chunkserverId}"
    echo "    format by percent                   : ./chunkserver_ctl.sh format -allocatePercent=80 -fileSystemPath=/data/chunkserver{id} "
    echo "                                          -filePoolDir=/data/chunkserver{id}/filepool/"
    echo "                                          -filePoolMetaPath=/data/chunkserver{id}/filepool.meta"
    echo "    format by chunk numbers             : ./chunkserver_ctl.sh format -allocateByPercent=false -preAllocateNum=100"
    echo "                                          -fileSystemPath=/data/chunkserver{id} "
    echo "                                          -filePoolDir=/data/chunkserver{id}/filepool/"
    echo "                                          -filePoolMetaPath==/data/chunkserver{id}/filepool.meta"
    echo "OPSTIONS:"
    echo "    [-c|--confPath path]  chunkserver conf path need for start command, default:/etc/curve/chunkserver.conf"
}

# 从subnet获取ip
function get_ip_from_subnet() {
    subnet=$1
    prefix=`echo $subnet|awk -F/ '{print $1}'|awk -F. '{printf "%d", ($1*(2^24))+($2*(2^16))+($3*(2^8))+$4}'`
    mod=`echo $subnet|awk -F/ '{print $2}'`
    mask=$((2**32-2**(32-$mod)))
    # 对prefix再取一次模，为了支持10.182.26.50/22这种格式
    prefix=$(($prefix&$mask))
    ip=
    for i in `/sbin/ifconfig -a|grep inet|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
    do
        # 把ip转换成整数
        ip_int=`echo $i|awk -F. '{printf "%d\n", ($1*(2^24))+($2*(2^16))+($3*(2^8))+$4}'`
        if [ $(($ip_int&$mask)) -eq $prefix ]
        then
            ip=$i
            break
        fi
    done
    if [ -z "$ip" ]
    then
        echo "no ip matched!\n"
        exit 1
    fi
}

# 启动chunkserver
function start() {
    if [ $# -lt 1 ]
    then
        help
        return 1
    fi
    if [ $# -gt 2 ]
    then
	    confPath=$3
    fi
    # 检查配置文件
    if [ ! -f ${confPath} ]
    then
        echo "confPath $confPath not exist!"
        return 1
    fi
    # parse subnet mask from config
    internal_subnet=`cat $confPath|grep global.subnet|awk -F"=" '{print $2}'`
    port=`cat $confPath|grep global.port|awk -F"=" '{print $2}'`
    get_ip_from_subnet $internal_subnet
    internal_ip=$ip
    echo "ip: $internal_ip"
    echo "base port: $port"
    external_subnet=`cat $confPath|grep global.external_subnet|awk -F"=" '{print $2}'`
    get_ip_from_subnet $external_subnet
    external_ip=$ip
    enableExternalServer=true
    # external ip和internal ip一致或external ip为127.0.0.1时不启动external server
    if [ $internal_ip = $external_ip -o $external_ip = "127.0.0.1" ]
    then
        enableExternalServer=false
    else
        echo "external_ip: $external_ip"
    fi

    if [ "$1" = "all" ]
    then
        ret=`lsblk|grep chunkserver|awk '{print $7}'|sed 's/[^0-9]//g'`
        for i in $ret
        do
            start_one $i
        done
    else
        start_one $1
    fi
    sleep 1
    status $1
}

function start_one() {
    if [ $1 -lt 0 ]
    then
        echo "chunkserver num $1 is not ok"
        return 1
    fi

    ps -efl|grep -w "/data/chunkserver$1"|grep -v grep
    if [ $? -eq 0 ]
    then
        echo "chunkserver$1 is already active!"
        return 0
    fi

    mkdir -p ${DATA_DIR}/log/chunkserver$1
    if [ $? -ne 0 ]
    then
        echo "Create log dir failed: ${DATA_DIR}/log/chunkserver$1"
        return 1
    fi

    jemallocpath=/usr/lib/x86_64-linux-gnu/libjemalloc.so.1
    # 检查jemalloc库文件
    if [ ! -f ${jemallocpath} ]
    then
        echo "Not found jemalloc library, Path is ${jemallocpath}"
        exit 1
    fi
    LD_PRELOAD=${jemallocpath} curve-chunkserver \
            -bthread_concurrency=18 -raft_max_segment_size=8388608 \
            -raft_max_install_snapshot_tasks_num=1 -raft_sync=true  \
            -conf=${confPath} \
            -chunkFilePoolDir=${DATA_DIR}/chunkserver$1 \
            -chunkFilePoolMetaPath=${DATA_DIR}/chunkserver$1/chunkfilepool.meta \
            -walFilePoolDir=${DATA_DIR}/chunkserver$1 \
            -walFilePoolMetaPath=${DATA_DIR}/chunkserver$1/walfilepool.meta \
            -chunkServerIp=$internal_ip \
            -enableExternalServer=$enableExternalServer \
            -chunkServerExternalIp=$external_ip \
            -chunkServerPort=$((${port}+${1})) \
            -chunkServerMetaUri=local:///data/chunkserver$1/chunkserver.dat \
            -chunkServerStoreUri=local:///data/chunkserver$1/ \
            -copySetUri=local:///data/chunkserver$1/copysets \
            -raftSnapshotUri=curve:///data/chunkserver$1/copysets \
            -raftLogUri=curve:///data/chunkserver$1/copysets \
            -recycleUri=local:///data/chunkserver$1/recycler \
            -raft_sync_segments=true \
            -graceful_quit_on_sigterm=true \
            -raft_sync_meta=true \
            -raft_sync_segments=true \
            -raft_use_fsync_rather_than_fdatasync=false \
            -log_dir=${DATA_DIR}/log/chunkserver$1 > /dev/null 2>&1 &
}

function stop() {
    if [ $# -lt 1 ]
    then
        help
        return 1
    fi
    # stop all
    if [ "$1" = "all" ]
    then
        echo "kill all chunkservers"
        killall curve-chunkserver
        return $?
    fi

    num=`lsblk|grep chunkserver|wc -l`
    if [ $1 -lt 0 ]
    then
        echo "chunkserver num $1 is not ok"
        return 1
    fi

    if [ $1 -gt $num ]
    then
        echo "chunkserver num $1 is not ok"
        return 1
    fi

    echo "kill chunkserver $1"
    kill `ps -efl|grep -w /data/chunkserver$1|grep -v grep|awk '{print $4}'`
}

function restart() {
    if [ $# -lt 1 ]
    then
        help
        return 1
    fi
    if [ $# -gt 2 ]
    then
	    confPath=$3
    fi
    stop $1
    if [ "$1" = "all" ]
    then
        ret=`lsblk|grep chunkserver|awk '{print $7}'|sed 's/[^0-9]//g'`
        for j in $ret
        do
        {
            wait_stop $j
        } &
        done
        wait
    else
        wait_stop $1
    fi
    start $1
}

function wait_stop() {
    # wait 3秒钟让它退出
    retry_times=0
    while [ $retry_times -le 3 ]
    do
        ((retry_times=$retry_times+1))
        ps -efl|grep -w "/data/chunkserver$1"|grep -v grep > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            sleep 1
            continue
        else
            break
        fi
    done
    # 如果进程还在，就kill -9
    ps -efl|grep -w "/data/chunkserver$1"|grep -v grep > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        echo "The process of chunkserver$1 still exists after 3s, now kill -9 it"
        kill -9 `ps -efl|grep -w /data/chunkserver$1|grep -v grep|awk '{print $4}'`
    fi
}

function status() {
    if [ $# -lt 1 ]
    then
        help
        return 1
    fi
    if [ "$1" = "all" ]
    then
        for i in `cat /etc/fstab|grep chunkserver|awk '{print $2}'|sed 's/[^0-9]//g'`
        do
            status_one $i
        done
    else
        status_one $1
    fi
}

function status_one() {
    num=`lsblk|grep chunkserver|wc -l`
    if [ $1 -lt 0 ]
    then
        echo "chunkserver num $1 is not ok"
        return 1
    fi
    ps -efl|grep -w "/data/chunkserver$1"|grep -v grep > /dev/null 2>&1
    if [ $? -eq 0 ]
    then
        echo "chunkserver$1 is active!"
        return 0
    else
        echo "chunkserver$1 is down"
        return 1
    fi
}

function deploy() {
    if [ $# -lt 1 ]
    then
        help
        return 1
    fi
    if [ "$1" = "all" ]
    then
        do_confirm;
        deploy_all;
        return $?
    fi
    if [ $# -eq 2 ]
    then
        do_confirm;
        deploy_one $@;
        return $?
    fi
    usage;
}

function format() {
    # 格式化chunkfile pool
    curve-format $*
}

function recordmeta() {
    # 将当前的磁盘的uuid及其md5备份到磁盘的disk.meta文件中
    meta_record;
}

function deploy-wal-pool() {
    if [ $# -lt 1 ]
    then
        help
        return 1
    fi
    if [ "$1" = "all" ]
    then
        walfile_pool_prep
        return $?
    fi
    deploy_one_walfile_pool $1
    wait
}

function main() {
    if [ $# -lt 1 ]
    then
        help
        return 1
    fi

    case $1 in
    "start")
        shift # pass first argument
        start $@
        ;;
    "stop")
        shift # pass first argument
        stop $@
        ;;
    "restart")
        shift # pass first argument
        restart $@
        ;;
    "status")
        shift # pass first argument
        status $@
        ;;
    "deploy")
        shift # pass first argument
        deploy $@
        ;;
    "format")
        shift # pass first argument
        format $@
        ;;
    "record-meta")
        shift
        recordmeta
        ;;
    "deploy-wal-pool")
        shift
        deploy-wal-pool $@
        ;;
    *)
        help
        ;;
    esac
}

main $@
