#!/bin/bash

### BEGIN INIT INFO
# Provides:          mount curve disk
# Required-Start:    $nebd-daemon
# Required-Stop:
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: monut curve cloud disk
# Description:       monut curve cloud disk for k8s calculate node
### END INIT INFO


# default confpath
confPath=/etc/nbd/k8s_curve.conf


# usage
function usage() {
    echo "Usage: ./mount_curve_clouddisk.sh [OPTIONS...]"
    echo "  -c/--confPath: set the confPath (default /etc/nbd/k8s_curve.conf)"
    echo "  --filename: name of the curve file"
    echo "  --filelength: length of the curve file (unit GB)"
    echo "  --user: owner of the curve file"
    echo "  --mountpoint: the mountpoint which the curve nbd device to mount"
    echo "  -h/--help: get the script usage"
    echo "Note: the explicit OPTIONS will override the confPath OPTIONS!!!"
    echo "Examples:"
    echo "  ./mount_curve_clouddisk.sh //use the default configuration"
    echo "  ./mount_curve_clouddisk.sh --confPath yourConfPath //use the new confPath"
    echo "  ./mount_curve_clouddisk.sh --filename curvefile --filelength 1024 --user k8s --mountpoint /data //use specify OPTIONS"
}

function run() {
    # check required OPTIONS
    if [ ! "$filename" -o ! "$filelength" -o ! "$user" -o ! "$mountpoint" ]
    then
        echo "ERROR: the required OPTIONS missed!"
        usage
        exit
    fi

    # create CURVE volume
    echo "1. create CURVE volume"
    volume=$(sudo curve_ops_tool list --fileName=/ | grep ${filename})
    if [ ! "$volume" ]
    then
        echo "CURVE volume info: filename: ${filename}, filelength: ${filelength}, user: ${user}"
        sudo curve create --filename /${filename} --length ${filelength} --user ${user}
    else
        echo "CURVE volume ${filename} exists"
    fi

    # map curve volume to nbd device
    echo "2. map CURVE volume to nds device"
    volume=$(sudo curve_ops_tool list --fileName=/ | grep ${filename})
    nbddevice=$(sudo curve-nbd list-mapped | grep ${filename})
    if [ ! "$volume" ]
    then
        echo "ERROR: create CURVE volume error!"
        exit
    elif [ ! "$nbddevice" ]
    then
        echo "curve-nbd map cbd:pool//${filename}_${user}_"
        sudo curve-nbd map cbd:pool//${filename}_${user}_
    fi

    # format the nbd device to ext4 filesystem
    echo "3. format the nbd device to ext4 filesystem"
    nbddevice=$(sudo curve-nbd list-mapped | grep ${filename} | cut -d " " -f 3)
    fstype=$(sudo blkid -s TYPE ${nbddevice} | awk -F '"' '{print $2}')
    if [ ! "$nbddevice" ]
    then
        echo "ERROR: map curve volume to nbd device error!"
        exit
    elif [ "$fstype" != "ext4" ]
    then
        echo "format ${nbddevice} to ext4"
        sudo mkfs.ext4 ${nbddevice}
    fi

    # mount nbd device to /data
    echo "4. mount nbd device to ${mountpoint}"
    fstype=$(sudo blkid -s TYPE ${nbddevice} | awk -F '"' '{print $2}')
    hasmount=$(df -T | grep ${nbddevice})
    if [ "$fstype" != "ext4" ]
    then
        echo "ERROR: nbd device fstype error!"
        exit
    elif [ ! "$hasmount" ]
    then
        echo "mount ${nbddevice} to ${mountpoint}"
        sudo mount ${nbddevice} ${mountpoint}
    fi
}

# get required OPTIONS from confPath
function getOptions() {
    filename=`cat ${confPath} | grep -v '#' | grep filename= | awk -F "=" '{print $2}'`
    filelength=`cat ${confPath} | grep -v '#' | grep filelength= | awk -F "=" '{print $2}'`
    user=`cat ${confPath} | grep -v '#' | grep user= | awk -F "=" '{print $2}'`
    mountpoint=`cat ${confPath} | grep -v '#' | grep mountpoint= | awk -F "=" '{print $2}'`
}

count=1
for i in "$@"
do
    if [ "$i" == "-c" -o "$i" == "--confPath" ]
    then
        let count+=1
        eval confPath=$(echo \$$count)
        break
    else
        let count+=1
    fi
done

if [ ! -f ${confPath} ]
then
    echo "ERROR: not found configuration file, confPath is ${confPath}!"
    exit
else
    getOptions
    while [[ $# -gt 0 ]]
    do
        key=$1

        case $key in
        -c|--confPath)
            confPath="$2"
            shift 2 # pass key & value
            ;;
        --filename)
            filename="$2"
            shift 2
            ;;
        --filelength)
            filelength="$2"
            shift 2
            ;;
        --user)
            user="$2"
            shift 2
            ;;
        --mountpoint)
            mountpoint="$2"
            shift 2
            ;;
        *)
            usage
            exit
            ;;
        esac
    done
    run
fi