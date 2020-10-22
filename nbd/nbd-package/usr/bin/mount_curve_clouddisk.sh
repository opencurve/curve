#!/bin/bash

#filename corresponds to the machine hostname
#volume size 1T
#user k8s
filename=$HOSTNAME"_curvefile"
filelength=1024
user=k8s
mountpoint=/data


#create CURVE volume
volume=$(sudo curve_ops_tool list --fileName=/ | grep ${filename})
if [ ! "$volume" ]
then
	sudo curve create --filename /${filename} --length ${filelength} --user ${user}
	sudo echo -e "create CURVE volume.\nfilename: ${filename}, filelength: ${filelength}, user: ${user}" > curve_volume_info
fi


#map curve volume to nbd device
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


#format the nbd device to ext4 filesystem
nbddevice=$(sudo curve-nbd list-mapped | grep ${filename} | cut -d " " -f 3)
format=$(sudo blkid -s TYPE ${nbddevice} | awk -F '"' '{print $2}')
if [ ! "$nbddevice" ]
then
	echo "map curve volume to nbd device error!"
	exit
elif [ "$format" != "ext4" ]
then
	sudo mkfs.ext4 ${nbddevice}
fi


#mount nbd device to /data
format=$(sudo blkid -s TYPE ${nbddevice} | awk -F '"' '{print $2}')
hasmount=$(df -T | grep ${nbddevice})
if [ "$format" != "ext4" ]
then
	echo "nbd device: ${nbddevice} format error!"
	exit
elif [ ! "$hasmount" ]
then
	sudo mount ${nbddevice} ${mountpoint}
fi


#restart auto mount
UUID=$(sudo blkid -s UUID ${nbddevice} | awk -F '"' '{print $2}')
mountpoint=$(df -T | grep ${nbddevice} | awk '{print $7}')
filetype=$(df -T | grep ${nbddevice} | awk '{print $2}')
hasadd=$(cat /etc/fstab | grep ${UUID})
if [ ! "$hasadd" ]
then
	sudo echo -e "\n#curve nbd device: ${nbddevice}" >> /etc/fstab
	sudo echo -e "UUID=${UUID}\t${mountpoint}\t${filetype}\tdefaults\t0\t0" >> /etc/fstab
fi