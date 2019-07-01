#!/bin/bash
#增加confirm提示，防止误操作
echo "This deployment script will format the disk and delete all the data."
echo "Please confirm that you want to do this action! If you confirm it, please \
input:"
echo "Yes, I do!"
read tips
if [ "$tips" != "Yes, I do!" ]
then
	exit
fi

#步骤1：清理环境
#通过lsscsi的输出过滤出curve使用的数据盘，将数据盘挂载的目录都卸载掉，为下一步格式化磁盘做准备
#prepare deploy env
for i in `lsscsi |grep ATA|awk '{print $7}'|awk -F"/" '{print $3}'`
do
	mntdir=`lsblk|grep $i|awk '{print $7}'`
	if [ -z $mntdir ]
	then
		continue
	fi
	echo "umount $mntdir"
	fuser -kv $mntdir
	umount $mntdir
	if [ $? -ne 0 ]
	then
		echo "umount $mntdir failed"
		exit
	fi
done
#步骤2
#记录磁盘的盘符信息和磁盘的wwn信息，将信息持久化到diskinfo文件
DATA_DIR=/data
#check and record disk info
declare -A disk_map

for i in `lsblk -O|grep ATA|awk '{print $1}'`
do
	wwn=`lsblk -O|grep ATA|grep $i|awk '{print $32}'`
	disk_map["$i"]=$wwn
done

echo "disk_map length: " ${#disk_map[@]}

diskinfo=./diskinfo

[ -e $diskinfo ] && rm -r diskinfo

for m in ${!disk_map[@]}
do
	echo "Disk:"$m" <==> ""wwn:"${disk_map[$m]} >> $diskinfo
done
#步骤3
#根据磁盘数量创建数据目录，目前的格式统一是/data/chunkserver+num
#create data directory
if [ -d ${DATA_DIR} ]
then
	rm -rf ${DATA_DIR}
	if [ $? -ne 0 ]
	then
		echo "rm $DATA_DIR failed"
		exit
	fi
fi
mkdir -p ${DATA_DIR}
echo $((${#disk_map[@]}-1))
for i in `seq 0 $((${#disk_map[@]}-1))`
do
	mkdir -p ${DATA_DIR}/chunkserver$i
done
#步骤4
#格式化磁盘文件系统
#format disk
for disk in ${!disk_map[@]}
do
	echo $disk
	mkfs.ext4 -F /dev/$disk 2>&1 > /dev/null &
done
#步骤5
#将创建好的数据目录按照顺序挂载到格式化好的磁盘上，并记录挂载信息到mount.info
#mount data directory
while [ 1 ]
do
	ps -efl|grep mkfs|grep -v grep 2>&1 >/dev/null
	if [ $? -eq 0 ]
	then
		echo "doing mkfs, sleep 5s and retry."
		sleep 5
		continue
	fi
	break
done

j=0
for i in `cat $diskinfo |awk '{print $1}'|awk -F":" '{print $2}'`
do
	mount /dev/$i $DATA_DIR/chunkserver$j
	if [ $? -ne 0 ]
	then
		echo "mount $i failed"
		exit
	fi
	((j++));
done

lsblk > ./mount.info
#步骤6
#持久化挂载信息到fstab文件，防止系统重启后丢失
#update fstab
grep curvefs /etc/fstab
if [ $? -ne 0 ]
then
echo "#curvefs" >> /etc/fstab
for i in `lsblk|grep chunkserver|awk '{print $1}'`
do
	echo "UUID=`ls -l /dev/disk/by-uuid/|grep $i|awk '{print $9}'`    `lsblk|grep $i|awk '{print $7}'`    ext4  rw,errors=remount-ro    0    0" >> /etc/fstab
done
fi
#init chunkfilepool
#ret=`lsblk|grep chunkserver|wc -l`
#for i in `seq 0 $((${ret}-1))`
#do
#	curve-format -allocatepercent=80 \
#	-chunkfilepool_dir=/data/chunkserver$i/chunkfilepool \
#	-chunkfilepool_metapath=/data/chunkserver$i/chunkfilepool.meta \
#	-filesystem_path=/data/chunkserver$i/chunkfilepool &
#done

