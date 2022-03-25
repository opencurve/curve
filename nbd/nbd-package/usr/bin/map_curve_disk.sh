#!/bin/bash

# default confpath
# file format is:dealflag \t device \t image \t mapoptions \t mountpoint(option)
# +	/dev/nbd0	cbd:pool//curvefile_test_   defaults	/test
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
    echo "    file format is:dealflag \t device \t image \t map-options \t mountpoint(option)  \t mountoption(optional)"
    echo "    example: +	/dev/nbd0	cbd:pool//curvefile_test_   defaults or try-netlink,timeout=7200    /test  -t ext4 -o discard"
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

function convert_map_opts() {
    if [ $# -ne 1 ]; then
        echo ""
        return
    fi

    PARAMS=$1
    if [ "${PARAMS}" == "defaults" ]; then
        echo ""
        return
    fi

    out=$(echo ${PARAMS} | sed 's/,/ --/g')
    out="--${out}"
    echo ${out}
}

# map the curve disk based on curvetab
function map() {
    if [ ! -f ${confPath}.bak ]
    then
        exit 0
    fi

    cat ${confPath}.bak | grep -v '#' | while read line
    do
        device=$(echo "$line" | awk -F'\t' '{print $2}')
        image=$(echo "$line" | awk -F'\t' '{print $3}')
        mapopt=$(echo "$line" | awk -F'\t' '{print $4}')
        mountpoint=$(echo "$line" | awk -F'\t' '{print $5}')
        option=$(echo "$line" | awk -F'\t' '{print $6}')

        mapopt=$(convert_map_opts "${mapopt}")
        curve-nbd map $image --device $device ${mapopt}

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
