#!/bin/bash

# curve-mds路径
curveBin=/usr/bin/curve-mds

# 默认配置文件
confPath=/etc/curve/mds.conf

# 日志文件路径
logPath=${HOME}/nohup.out

# mdsAddr
mdsAddr=

# stop时是否停止curve-mds
forceStop=false

# pidfile
pidFile=${HOME}/curve-mds.pid

# daemon log
daemonLog=${HOME}/daemon-mds.log

# 启动mds
function start_mds() {
    # 检查logPath是否可写或者是否能够创建
    touch ${logPath} > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Can't Write or Create mds logfile: ${logPath}"
        exit
    fi

    # 检查daemonLog是否可写或者是否能够创建
    touch ${daemonLog} > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Can't Write or Create daemon logfile: ${daemonLog}"
        exit
    fi

    # 检查daemon
    if ! type daemon &> /dev/null
    then
        echo "No daemon installed"
        exit
    fi

    # 检查curve-mds
    if [ ! -f "/usr/bin/curve-mds" ]
    then
        echo "No curve-mds installed"
        exit
    fi

    # 检查配置文件
    if [ ! -f ${confPath} ]
    then
        echo "Not found mds.conf, Path is ${confPath}"
        exit
    fi

    # 判断是否已经通过daemon启动了curve-mds
    daemon --name curve-mds --pidfile ${pidFile} --running
    if [ $? -eq 0 ]
    then
        echo "Already started curve-mds by daemon"
        exit
    fi

    # pidfile不存在 或 daemon进程不存在
    # 启动daemon,切换路径,并启动curve-mds

    # 未指定mdsAddr
    if [ -z ${mdsAddr} ]
    then
        daemon --name curve-mds --core \
            --respawn --attempts 100 --delay 10 \
            --pidfile ${pidFile} \
            --errlog ${daemonLog} \
            --output ${logPath} \
            -- ${curveBin} -confPath=${confPath}
    else
        daemon --name curve-mds --core \
            --respawn --attempts 100 --delay 10 \
            --pidfile ${pidFile} \
            --errlog ${daemonLog} \
            --output ${logPath} \
            -- ${curveBin} -confPath=${confPath} -mdsAddr=${mdsAddr}
    fi
}

# 停止daemon进程，但不停止curve-mds
function stop_mds() {
    if [ -f ${pidFile} ]
    then
        daemon --stop --name curve-mds --pidfile ${pidFile}

        # stop时加了-f参数，则停止curve-mds进程
        if [ $forceStop ]
        then
            pkill curve-mds > /dev/null 2>&1
        fi
    fi
}

# 使用方式
function usage() {
    echo "Usage:"
    echo "  mds-daemon start -- start deamon process and watch on curve-mds process"
    echo "        [-c|--confPath path]        mds conf path"
    echo "        [-l|--logPath  path]        mds log path"
    echo "        [-a|--mdsAddr  ip:port]     mds address"
    echo "  mds-daemon stop  -- stop daemon process but curve-mds still running"
    echo "        [-f]  also stop curve-mds process"
    echo "Examples:"
    echo "  mds-daemon start -c /etc/curve/mds.conf -l ${HOME}/nohup.out -a 127.0.0.1:6666"
}

# 检查参数启动参数，最少1个
if [ $# -lt 1 ]
then
    usage
    exit
fi

case $1 in
"start")
    shift # pass first argument

    # 解析参数
    while [[ $# -gt 1 ]]
    do
        key=$1

        case $key in
        -c|--confPath)
            confPath=`realpath $2`
            shift # pass key
            shift # pass value
            ;;
        -a|--mdsAddr)
            mdsAddr=$2
            shift # pass key
            shift # pass value
            ;;
        -l|--logPath)
            logPath=`realpath $2`
            shift # pass key
            shift # pass value
            ;;
        *)
            usage
            exit
            ;;
        esac
    done

    start_mds
    ;;
"stop")
    # stop时加了-f参数，则停止curve-mds进程
    if [ $# -eq 2 ] && [ $2 = '-f' ]
    then
        forceStop=true
    fi

    stop_mds
    ;;
*)
    usage
    ;;
esac
