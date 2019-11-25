#!/bin/bash

# curve-snapshotcloneserver路径
curveBin=/usr/bin/curve-snapshotcloneserver

# 默认配置文件
confPath=/etc/curve/snapshot_clone_server.conf

# 日志文件路径
logPath=${HOME}/

# serverAddr
serverAddr=

# stop时是否停止curve-mds
forceStop=false

# pidfile
pidFile=${HOME}/curve-snapshot.pid

# daemon log
daemonLog=${HOME}/daemon-mds.log

# console output
consoleLog=${HOME}/snapshot-console.log

# 启动snapshotcloneserver
function start_server() {
    # 检查daemon
    if ! type daemon &> /dev/null
    then
        echo "No daemon installed"
        exit
    fi

    # 检查curve-snapshotcloneserver
    if [ ! -f ${curveBin} ]
    then
        echo "No curve-snapshotcloneserver installed, Path is ${curveBin}"
        exit
    fi

    # 检查配置文件
    if [ ! -f ${confPath} ]
    then
        echo "Not found snapshot_clone_server.conf, Path is ${confPath}"
        exit
    fi

    # 判断是否已经通过daemon启动了curve-mds
    daemon --name curve-snapshotcloneserver --pidfile ${pidFile} --running
    if [ $? -eq 0 ]
    then
        echo "Already started curve-snapshotcloneserver by daemon"
        exit
    fi

    # TODO(wuhanqing): 交给glog创建
    # 创建logPath
    mkdir -p ${logPath} > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Create log path failed: ${logPath}"
        exit
    fi

    # TODO(wuhanqing): 交给glog检测
    # 检查logPath是否有写入权限
    touch ${logPath}/dummy-file > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Can't Create logfile in ${logPath}, check permission"
        rm -f ${logPath}/dummy-file > /dev/null 2>&1
        exit
    fi
    rm -f ${logPath}/dummy-file > /dev/null 2>&1

    # 检查consoleLog是否可写或者能否创建，初始化glog之前的日志存放在这里
    touch ${consoleLog} > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Can't Write or Create console log: ${consoleLog}"
        exit
    fi

    # 检查daemonLog是否可写或者是否能够创建
    touch ${daemonLog} > /dev/null 2>&1
    if [ $? -ne 0 ]
    then
        echo "Can't Write or Create daemon logfile: ${daemonLog}"
        exit
    fi

    # 未指定serverAddr
    if [ -z ${serverAddr} ]
    then
        daemon --name curve-snapshotcloneserver --core --inherit \
            --respawn --attempts 100 --delay 10 \
            --pidfile ${pidFile} \
            --errlog ${daemonLog} \
            --output ${consoleLog} \
            -- ${curveBin} -conf=${confPath} -log_dir=${logPath} -stderrthreshold=3 -graceful_quit_on_sigterm=true
    else
        daemon --name curve-snapshotcloneserver --core --inherit \
            --respawn --attempts 100 --delay 10 \
            --pidfile ${pidFile} \
            --errlog ${daemonLog} \
            --output ${consoleLog} \
            -- ${curveBin} -conf=${confPath} -addr=${serverAddr} -log_dir=${logPath} -stderrthreshold=3 -graceful_quit_on_sigterm=true
    fi
}

# 停止daemon进程和curve-snapshotcloneserver
function stop_server() {
    daemon --name curve-snapshotcloneserver --pidfile ${pidFile} --stop
}

# restart
function restart_server() {
    # 判断是否已经通过daemon启动了curve-mds
    daemon --name curve-snapshotcloneserver --pidfile ${pidFile} --running
    if [ $? -ne 0 ]
    then
        echo "Didn't started curve-snapshotcloneserver by daemon"
        exit
    fi

    daemon --restart --name curve-snapshotcloneserver --pidfile ${pidFile}
    if [ $? -ne 0 ]
    then
        echo "Restart failed"
    fi
}

# 使用方式
function usage() {
    echo "Usage:"
    echo "  snapshot-daemon start -- start deamon process and watch on curve-snapshotcloneserver process"
    echo "        [-c|--confPath path]        configuration file"
    echo "        [-l|--logPath  path]        log directory"
    echo "        [-a|--serverAddr  ip:port]  listening address"
    echo "  snapshot-daemon stop  -- stop daemon process and curve-snapshotcloneserver"
    echo "  snapshot-daemon restart -- restart curve-snapshotcloneserver"
    echo "Examples:"
    echo "  snapshot-daemon start -c /etc/curve/snapshot_clone_server.conf -l ${HOME}/ -a 127.0.0.1:5555"
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
        -a|--serverAddr)
            serverAddr=$2
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

    start_server
    ;;
"stop")
    stop_server
    ;;
"restart")
    restart_server
    ;;
*)
    usage
    ;;
esac


