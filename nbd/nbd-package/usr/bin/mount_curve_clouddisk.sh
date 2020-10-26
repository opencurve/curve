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

# create CURVE volume
volume=$(sudo curve_ops_tool list --fileName=/ | grep ${filename})
if [ ! "$volume" ]
then
    sudo curve create --filename /${filename} --length ${filelength} --user ${user}
    sudo echo -e "create CURVE volume.\nfilename: ${filename}, filelength: ${filelength}, user: ${user}" > curve_volume_info
fi

# map curve volume to nbd device
volume=$(sudo curve_ops_tool list --fileName=/ | grep ${filename})
nbddevice=$(sudo curve-nbd list-mapped | grep ${filename})
if [ ! "$volume" ]
then
    echo "create volume error!"
    exit
elif [ ! "$nbddevice" ]
then
    sudo curve-nbd map cbd:pool//${filename}_${user}_
fi

# format the nbd device to ext4 filesystem
nbddevice=$(sudo curve-nbd list-mapped | grep ${filename} | cut -d " " -f 3)
fstype=$(sudo blkid -s TYPE ${nbddevice} | awk -F '"' '{print $2}')
if [ ! "$nbddevice" ]
then
    echo "map curve volume to nbd device error!"
    exit
elif [ "$fstype" != "ext4" ]
then
    sudo mkfs.ext4 ${nbddevice}
fi

# mount nbd device to /data
fstype=$(sudo blkid -s TYPE ${nbddevice} | awk -F '"' '{print $2}')
hasmount=$(df -T | grep ${nbddevice})
if [ "$fstype" != "ext4" ]
then
    echo "nbd device: ${nbddevice} fstype error!"
    exit
elif [ ! "$hasmount" ]
then
    sudo mount ${nbddevice} ${mountpoint}
fi