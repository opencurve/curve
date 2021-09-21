#!/usr/bin/env bash

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

# NOTE: If you want to use this script,
# you should replace the variable which with '__' prefix

############################  GLOBAL VARIABLES
g_cmd=""
g_daemon=0
g_project_name="__PROJECT__"
g_bin_filename="__BIN_FILENAME__"
g_project_prefix="$(dirname $(dirname $(realpath $0)))"
g_bin_file="$g_project_prefix/sbin/$g_bin_filename"
g_pid_file="$g_project_prefix/logs/$g_project_name.pid"
g_conf_file="$g_project_prefix/conf/$g_project_name.conf"
g_daemon_stdout_log="$g_project_prefix/logs/daemon_stdout.log"
g_daemon_error_log="$g_project_prefix/logs/daemon_error.log"
g_stdout_log="$g_project_prefix/logs/stdout.log"
g_error_log="$g_project_prefix/logs/error.log"
g_start_args="__START_ARGS__"

g_color_yellow=`printf '\033[33m'`
g_color_red=`printf '\033[31m'`
g_color_normal=`printf '\033[0m'`

############################  BASIC FUNCTIONS
msg() {
    printf '%b' "$1" >&2
}

success() {
    msg "$g_color_yellow[✔]$g_color_normal ${1}${2}"
}

die() {
    msg "$g_color_red[✘]$g_color_normal ${1}${2}"
    exit 1
}

program_must_exist() {
    command -v $1 >/dev/null 2>&1

    if [ $? -ne 0 ]; then
        die "You must have '$1' installed to continue\n"
    fi
}

############################  FUNCTIONS
usage () {
    cat << _EOC_
Usage:
    servicectl start|stop|restart|status [--args=ARGS] [--daemon]

Examples:
    servicectl start
    servicectl start --deamon
    servicectl start --args='--config-file /path/to/config'
    servicectl stop
    servicectl restart --deamon
    servicectl restart
    servicectl status
    servicectl version
_EOC_
}

get_options() {
    local long_opts="start,stop,restart,status,version,daemon,args:,help"
    local args=`getopt -o 0d --long $long_opts -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            -o|--args)
                g_start_args=$2
                shift 2
                ;;
            -d|--daemon)
                g_daemon=1
                shift 1
                ;;
            -h|--help)
                usage
                exit 1
                ;;
            --)
                shift
                break
                ;;
            *)
                exit 1
                ;;
        esac
    done

    g_cmd="$1"
}

_pid() {
    local pid=-1
    if [ -f $g_pid_file ]; then
        pid=`cat $g_pid_file 2>&1` || pid=-1
    fi
    echo "$pid"
}

# 0: stopped
# 1: running
# 2: running with daemon
_status() {
    local status=0
    local pid=$1
    if [ $pid -ne -1 ]; then
        local cmd=`ps -p $pid -o comm=`
        if [ "$cmd" == "daemon" ]; then
            daemon --name $g_project_name --pidfile $g_pid_file running
            if [ $? -eq 0 ]; then
                status=2
            fi
        elif [ "$cmd" != "" ]; then
            status=1
        fi
    fi
    echo -n $status
}

stime4process() {
    local pid=$1
    local stime=`ps -p $pid -o lstart=` &&
    local str4stime=`date -d "$stime" +'%Y-%m-%d %H:%M:%S'`
    if [ $? -ne 0 ]; then
        echo "-"
    else
        echo "$str4stime"
    fi
}

rtime4process() {
    local pid=$1
    local rtime=`ps -p $pid -o etime=`
    if [ $? -ne 0 ]; then
        echo "-"
    else
        echo ${rtime##""}
    fi
}

mem4process() {
    local pid=$1
    local mem=`ps -p $pid -o rss=`
    if [ $? -ne 0 ]; then
        echo 0
    else
        echo $mem
    fi
}

listen4process() {
    local pid=$1
    netstat -antlp 2>&1 | awk -v pid=$pid 'BEGIN { first = 1 }
        {
            if ($6 == "LISTEN" && $7 ~ pid"/") {
                if (first) {
                    first = 0
                    listens = $4
                } else {
                    listens = listens", "$4
                }
            }
        }
        END {
            print listens
        }'
}

print_status() {
    local pid=$1
    local daemon="$2"
    local active=""
    local mem=0
    local listen="-"
    if [ $pid -eq -1 ]; then
        active="[${g_color_red}STOPPED${g_color_normal}]"
    else
        local stime=`stime4process $pid`
        local rtime=`rtime4process $pid`
        active="[${g_color_yellow}RUNNING${g_color_normal}] since $stime; $rtime ago"
        mem=`mem4process $pid`
        listen=`listen4process $pid`
    fi

    cat << _EOC_
$g_project_name.service - CurveFS ${g_project_name^}
    Active: $active
  Main PID: $pid ($g_bin_filename)
    Daemon: $daemon
    Listen: $listen
       Mem: $mem KB
_EOC_
}

status() {
    local pid=`_pid`
    local status=`_status $pid`
    if [ $status -eq 2 ]; then
        pid=`ps --ppid $pid -o pid=` || pid=-1
        pid=`expr $pid`  # convert to number
        print_status "$pid" "True"
    elif [ "$status" -eq 1 ]; then
        print_status "$pid" "False"
    else
        print_status "-1" "False"
    fi
}

# TODO(@Wine93): start all service with --config-file
_start_normal() {
    nohup $g_bin_file $g_start_args >$g_stdout_log 2>$g_error_log &
    if [ $? -eq 0 ]; then
        echo -n $! > $g_pid_file
    fi
}

_start_daemon() {
    daemon --unsafe --core --respawn \
        --attempts 100 --delay 10 \
        --name $g_project_name \
        --pidfile $g_pid_file \
        --errlog $g_daemon_error_log \
        --output $g_daemon_stdout_log \
        -- $g_bin_file $g_start_args
}

start() {
    local pid=`_pid`
    if [[ $pid -ne -1 && `_status $pid` -ne 0 ]]; then
        die "(pid=$pid): already start\n"
    elif [ $g_daemon -eq 1 ]; then
        _start_daemon
    else
        _start_normal
    fi

    if [ $? -eq 0 ]; then
        success "start $g_project_name success\n"
    else
        die "start $g_project_name failed\n"
    fi
}

stop() {
    local pid=`_pid`
    local status=`_status $pid`
    if [[ $pid -eq -1 || $status -eq 0 ]]; then
        die "(pid=$pid): not running\n"
    elif [ $status -eq 2 ]; then
        daemon --name ${g_project_name} --pidfile ${g_pid_file} --stop
    else
        kill -9 $pid
    fi

    if [ $? -eq 0 ]; then
        success "stop $g_project_name success\n"
    else
        die "stop $g_project_name failed\n"
    fi
}

restart() {
    local pid=`_pid`
    local status=`_status $pid`
    if [[ $pid -eq -1 || $status -eq 0 ]]; then
        die "(pid=$pid): not running\n"
    fi

    stop
    if [[ $status -eq 2 && $g_daemon -eq 0 ]]; then
        sleep 1  # wait for daemon exit
    fi
    start
}

version() {
    $g_bin_file --version
    if [ $? -ne 0 ]; then
        die "get version failed"
    fi
}

main() {
    get_options "$@"

    case $g_cmd in
        start)
            start
            ;;
        stop)
            stop
            ;;
        restart)
            restart
            ;;
        status)
            status
            ;;
        version)
            version
            ;;
        *)
            usage
            exit 1
    esac
}

############################  MAIN()
main "$@"
