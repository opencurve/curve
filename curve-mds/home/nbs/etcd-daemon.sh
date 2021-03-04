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

# 默认配置文件
confPath=/etc/curve/etcd/etcd.conf.yml

# 日志文件目录
logDir=/data/log/curve/etcd

# 日志文件路径
logPath=${logDir}/etcd.log

# pidfile
pidFile=${HOME}/etcd.pid

# daemon log
daemonLog=${logDir}/daemon-etcd.log

# 启动etcd
function start_etcd() {
    # 创建logDir
    mkdir -p ${logDir} > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Create etcd log dir failed: ${logDir}"
        exit 1
    fi

    # 检查logPath是否有写权限
    if [ ! -w ${logDir} ]
    then
        echo "Write permission denied: ${logDir}"
        exit 1
    fi

    # 检查logPath是否可写或者是否能够创建
    touch ${logPath} > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Can't Write or Create etcd logfile: ${logPath}"
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

    # 检查是否安装etcd
    if [ -z `command -v etcd`  ]
    then
        echo "No etcd installed"
        exit
    fi

    # 检查配置文件
    if [ ! -f ${confPath} ]
    then
        echo "Not found confFile, Path is ${confPath}"
        exit
    fi

    # 判断是否已经通过daemon启动了etcd
    daemon --name etcd --pidfile ${pidFile} --running
    if [ $? -eq 0 ]
    then
        echo "Already started etcd by daemon"
        exit
    fi

    # pidfile不存在 或 daemon进程不存在
    # 启动daemon,切换路径,并启动etcd


    daemon --name etcd --core \
        --respawn --attempts 100 --delay 10 \
        --pidfile ${pidFile} \
        --errlog ${daemonLog} \
        --output ${logPath} \
	--unsafe \
        -- etcd --config-file ${confPath}
}

# 停止daemon进程和etcd
function stop_etcd() {
    # 判断是否已经通过daemon启动了etcd
    daemon --name etcd --pidfile ${pidFile} --running
    if [ $? -ne 0 ]
    then
        echo "Didn't start etcd by daemon"
        exit 0
    fi

    daemon --name etcd --pidfile ${pidFile} --stop
    if [ $? -ne 0 ]
    then
        echo "stop may not success!"
    else
        echo "etcd exit success!"
        echo "daemon exit success!"
    fi
}

# restart
function restart_etcd() {
    # 判断是否已经通过daemon启动了etcd
    daemon --name etcd --pidfile ${pidFile} --running
    if [ $? -ne 0 ]
    then
        echo "Didn't start etcd by daemon"
        exit 1
    fi

    daemon --name etcd --pidfile ${pidFile} --restart
    if [ $? -ne 0 ]
    then
        echo "Restart failed"
    fi
}

# 使用方式
function usage() {
    echo "Usage:"
    echo "  etcd-daemon start -- start deamon process and watch on etcd process"
    echo "        [-c|--confPath path]        etcd conf path"
    echo "        [-l|--logPath  path]        etcd log path"
    echo "  etcd-daemon stop  -- stop daemon process and etcd"
    echo "  etcd-daemon restart -- restart etcd"
    echo "Examples:"
    echo "  etcd-daemon start -c /etcd/etcd.conf.yml -l ${HOME}/etcd.log"
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

    start_etcd
    ;;
"stop")
    # 停止daemon和etcd进程
    stop_etcd
    ;;
"restart")
    # 重启etcd
    restart_etcd
    ;;
*)
    usage
    ;;
esac
