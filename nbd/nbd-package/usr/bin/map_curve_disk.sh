#!/bin/bash

### BEGIN INIT INFO
# Provides:          curve-nbd
# Required-Start:    $local_fs $remote_fs $network nebd-daemon
# Required-Stop:     $local_fs $remote_fs $network
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: curve-nbd auto map service
# Description:       curve-nbd auto map service and associated helpers
### END INIT INFO

# default confpath
# file format is:dealflag \t device \t image \t mountpoint(option)
# +	/dev/nbd0	cbd:pool//curvefile_test_	/test
confPath=/etc/curve/curvetab

# check sudo
if [[ $(id -u) -ne 0 ]]
then
    echo "Please run with sudo"
    exit 1
fi

# usage
function usage() {
    echo "Usage: ./map_curve_disk.sh start"
    echo "  -c/--confPath: set the confPath (default /etc/curve/curvetab)"
    echo "    file format is:dealflag \t device \t image \t mountpoint(optional) \t mountoption(optional)"
    echo "    example: +	/dev/nbd0	cbd:pool//curvefile_test_	/test   -t ext4 -o discard"
    echo "  -h/--help: get the script usage"
    echo "Examples:"
    echo "  ./map_curve_disk.sh start //use the default configuration"
    echo "  ./map_curve_disk.sh start --confPath yourConfPath //use the new confPath"
}

# remove extra records
function dealcurvetab() {
    if [ ! -f ${confPath} ]
    then
        echo "ERROR: not found configuration file, confPath is ${confPath}!"
        exit
    fi

    declare -A recordmap
    while read line
    do
        flag=$(echo "$line" | awk '{print $1}')
        device=$(echo "$line" | awk '{print $2}')
        if [ "$flag" = "+" ]
        then
            recordmap["$device"]=$line
        elif [ "$flag" = "-" ]
        then
            unset recordmap[$device]
        fi
    done < ${confPath}
    for key in ${!recordmap[@]}; do
	echo "${recordmap[$key]}" >> ${confPath}.bak
    done
}

# map the curve disk based on curvetab
function map() {
    if [ ! -f ${confPath}.bak ]
    then
        echo "ERROR: not found configuration file, confPath is ${confPath}.bak!"
        exit
    fi

    cat ${confPath}.bak | grep -v '#' | while read line
    do
        device=$(echo "$line" | awk -F'\t' '{print $2}')
        image=$(echo "$line" | awk -F'\t' '{print $3}')
        mountpoint=$(echo "$line" | awk -F'\t' '{print $4}')
        option=$(echo "$line" | awk -F'\t' '{print $5}')
        curve-nbd map $image --device $device
        if [ "$mountpoint" != "" ]
        then
            mount $option $device $mountpoint
        fi
    done
    mv ${confPath}.bak ${confPath}
}

if [ $# -lt 1 ]
then
    usage
    exit
fi

case $1 in
"start")
    shift # pass first argument

    while [[ $# -gt 1 ]]
    do
        key=$1

        case $key in
        -c|--confPath)
            confPath=`realpath $2`
            shift # pass key
            shift # pass value
            ;;
        *)
            usage
            exit
            ;;
        esac
    done
    dealcurvetab
    map
    ;;
*)
    usage
    ;;
esac
