#!/bin/bash

# curve-mds路径
curveBin=/usr/bin/curve-mds

# 默认配置文件
confPath=/etc/curve/mds.conf

# 日志文件路径
logPath=${HOME}

# mdsAddr
mdsAddr=

# pidfile
pidFile=${HOME}/curve-mds.pid

# daemon log
daemonLog=${HOME}/curve-mds-daemon.log

# console output
consoleLog=${HOME}/curve-mds-console.log

# 启动mds
function start_mds() {
  # 检查daemon
  if ! type daemon &> /dev/null
  then
    echo "No daemon installed"
    exit 1
  fi

  # 检查curve-mds
  if [ ! -f ${curveBin} ]
  then
    echo "No curve-mds installed"
    exit 1
  fi

  # 检查配置文件
  if [ ! -f ${confPath} ]
  then
    echo "Not found mds.conf, Path is ${confPath}"
    exit 1
  fi

  # 判断是否已经通过daemon启动了curve-mds
  daemon --name curve-mds --pidfile ${pidFile} --running
  if [ $? -eq 0 ]
  then
    echo "Already started curve-mds by daemon"
    exit 1
  fi

  # 创建logPath
  mkdir -p ${logPath} > /dev/null 2>&1
  if [ $? -ne 0 ]
  then
    echo "Create mds log dir failed: ${logPath}"
    exit 1
  fi

  # 检查logPath是否有写权限
  if [ ! -w ${logPath} ]
  then
    echo "Write permission denied: ${logPath}"
    exit 1
  fi

  # 检查consoleLog是否可写或者是否能够创建
  touch ${consoleLog} > /dev/null 2>&1
  if [ $? -ne 0 ]
  then
    echo "Can't Write or Create console Log: ${consoleLog}"
    exit 1
  fi

  # 检查daemonLog是否可写或者是否能够创建
  touch ${daemonLog} > /dev/null 2>&1
  if [ $? -ne 0 ]
  then
    echo "Can't Write or Create daemon logfile: ${daemonLog}"
    exit 1
  fi

  # 未指定mdsAddr, 从配置文件中解析出网段
  if [ -z ${mdsAddr} ]
  then
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
                    mdsAddr=$i
                    break
                fi
        done
        if [ -z "$mdsAddr" ]
        then
                echo "no ip matched!\n"
                return 1
        fi
    fi

  daemon --name curve-mds --core --inherit \
    --respawn --attempts 100 --delay 10 \
    --pidfile ${pidFile} \
    --errlog ${daemonLog} \
    --output ${consoleLog} \
    -- ${curveBin} -confPath=${confPath} -mdsAddr=${mdsAddr}:${port} -log_dir=${logPath} -graceful_quit_on_sigterm=true -stderrthreshold=3

  sleep 1
  show_status
}

# 停止daemon进程，且停止curve-mds
function stop_mds() {
  # 判断是否已经通过daemon启动了curve-mds
  daemon --name curve-mds --pidfile ${pidFile} --running
  if [ $? -ne 0 ]
  then
    echo "Didn't start curve-mds by daemon"
    exit 1
  fi

  daemon --name curve-mds --pidfile ${pidFile} --stop
  if [ $? -ne 0 ]
  then
    echo "stop may not success!"
  else
    echo "curve-mds exit success!"
    echo "daemon exit success!"
  fi
}

# restart
function restart_mds() {
  # 判断是否已经通过daemon启动了curve-mds
  daemon --name curve-mds --pidfile ${pidFile} --running
  if [ $? -ne 0 ]
  then
    echo "Didn't start curve-mds by daemon"
    exit 1
  fi

  daemon --name curve-mds --pidfile ${pidFile} --restart
}

# show status
function show_status() {
  # 判断是否已经通过daemon启动了curve-mds
  daemon --name curve-mds --pidfile ${pidFile} --running
  if [ $? -ne 0 ]
  then
    echo "Didn't start curve-mds by daemon"
    exit 1
  fi

  # 查询leader的IP
  leaderAddr=`tac ${consoleLog}|grep -m 1 -B 1000000 "load mds configuration"|grep "leader"|grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}"|head -n1`

  # 如果load mds configuration之后的日志，没有leader相关日志
  # 那么leaderAddr为空, mds应该没有起来
  if [ -z ${leaderAddr} ]
  then
    echo "MDS may not start successfully, check log"
    exit 1
  fi

  if [ ${leaderAddr} = "127.0.0.1" ]
  then
    echo "Current MDS is LEADER"
  else
    # 查询是否和自身ip相等
    for ip in `(hostname -I)`
    do
      if [ ${leaderAddr} = ${ip} ]
      then
        echo "Current MDS is LEADER"
        exit
      fi
    done

    echo "Current MDS is FOLLOWER, LEADER is ${leaderAddr}"
  fi
}

# 使用方式
function usage() {
  echo "Usage:"
  echo "  ./mds-daemon.sh start -- start deamon process and watch on curve-mds process"
  echo "    [-c|--confPath path]    mds conf path"
  echo "    [-l|--logPath  path]    mds log path"
  echo "    [-a|--mdsAddr  ip:port]   mds address"
  echo "  ./mds-daemon.sh stop  -- stop daemon process and curve-mds"
  echo "  ./mds-daemon.sh restart -- restart curve-mds"
  echo "  ./mds-daemon.sh status -- show mds status [LEADER/STATUS]"
  echo "Examples:"
  echo "  ./mds-daemon.sh start -c /etc/curve/mds.conf -l ${HOME}/ -a 127.0.0.1:6666"
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
"restart")
  restart_mds
  ;;
"status")
  show_status
  ;;
*)
  usage
  ;;
esac
