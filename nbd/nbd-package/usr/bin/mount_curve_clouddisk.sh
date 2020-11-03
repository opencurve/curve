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


#filename corresponds to the machine hostname
#volume size 1T
#user k8s
filename=$HOSTNAME"_curvefile"
filelength=1024
user=k8s
mountpoint=/data


# usage
if [ "$1" == "-h" -o "$1" == "--help" ]; then
  echo "Usage: ./mount_curve_clouddisk.sh -- run the script to create and mount curve disk to ${mountpoint}"
  exit 0
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