#!/bin/bash

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
    echo "    deploy all disk                     : ./chunkserver_ctl.sh deploy all"
    echo "    deploy one disk                     : ./chunkserver_ctl.sh deploy /dev/sd{id} /data/chunkserver{id}"
    echo "    format by percent                   : ./chunkserver_ctl.sh format -allocatepercent=80 -filesystem_path=/data/chunkserver{id} "
    echo "                                          -chunkfilepool_dir=/data/chunkserver{id}/chunkfilepool/"
    echo "                                          -chunkfilepool_metapath=/data/chunkserver{id}/chunkfilepool.meta"
    echo "    format by chunk numbers             : ./chunkserver_ctl.sh format -allocateByPercent=false -preallocateNum=100"
    echo "                                          -filesystem_path=/data/chunkserver{id} "
    echo "                                          -chunkfilepool_dir=/data/chunkserver{id}/chunkfilepool/"
    echo "                                          -chunkfilepool_metapath==/data/chunkserver{id}/chunkfilepool.meta"
    echo "OPSTIONS:"
    echo "    [-c|--confPath path]  chunkserver conf path need for start command, default:/etc/curve/chunkserver.conf"
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
    subnet=`cat $confPath|grep global.subnet|awk -F"=" '{print $2}'`
    port=`cat $confPath|grep global.port|awk -F"=" '{print $2}'`
    prefix=`echo $subnet|awk -F/ '{print $1}'|awk -F. '{printf "%d", ($1*(2^24))+($2*(2^16))+($3*(2^8))+$4}'`
    mod=`echo $subnet|awk -F/ '{print $2}'`
    mask=$((2**32-2**(32-$mod)))
    echo "subnet: $subnet"
    echo "base port: $port"
    # 对prefix再取一次模，为了支持10.182.26.50/22这种格式
    prefix=$(($prefix&$mask))
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
        return 1
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
    curve-chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_max_install_snapshot_tasks_num=5 -raft_sync=true  \
            -conf=${confPath} \
            -chunkFilePoolDir=${DATA_DIR}/chunkserver$1 \
            -chunkFilePoolMetaPath=${DATA_DIR}/chunkserver$1/chunkfilepool.meta \
            -chunkServerIp=$ip \
            -chunkServerPort=$((${port}+${1})) \
            -chunkServerMetaUri=local:///data/chunkserver$1/chunkserver.dat \
            -chunkServerStoreUri=local:///data/chunkserver$1/ \
            -copySetUri=local:///data/chunkserver$1/copysets \
            -recycleUri=local:///data/chunkserver$1/recycler \
            -raft_sync_segments=true \
            -graceful_quit_on_sigterm=true \
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
    kill `ps -efl|grep -w chunkserver$1|grep -v grep|awk '{print $4}'`
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
    # 确认chunkserver已经停掉再启动
    while true
    do
        ps -efl|grep -w "/data/chunkserver$1"|grep -v grep > /dev/null 2>&1
        if [ $? -eq 0 ]
        then
            sleep 1
            continue
        else
            break
        fi
    done
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

    if [ $1 -gt $num ]
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
    *)
        help
        ;;
    esac
}

main $@
