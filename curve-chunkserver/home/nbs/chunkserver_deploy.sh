#!/bin/bash

#
#  Copyright (c) 2020 NetEase Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

#confirm提示，防止误操作
function do_confirm {
  echo "This deployment script will format the disk and delete all the data."
  echo "Please confirm that you want to do this action!"
  echo "If you confirm it, please input:"
  echo "Yes, I do!"
  read tips
  if [ "$tips" != "Yes, I do!" ]
  then
    echo "Exit without actions"
    exit
  fi
}

function deploy_prep {
#清理/etc/fstab残留信息
  grep curvefs /etc/fstab
  if [ $? -eq 0 ]
  then
    sed -i '/curvefs/d' /etc/fstab
    sed -i '/\/data\/chunkserver/d' /etc/fstab
  fi
#通过lsscsi的输出过滤出curve使用的数据盘，将数据盘挂载的目录都卸载掉，为下一步格式化磁盘做准备
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
}

#记录磁盘的盘符信息和磁盘的wwn信息，将信息持久化到diskinfo文件
DATA_DIR=/data
declare -A disk_map
diskinfo=./diskinfo
function record_diskinfo {
  for i in `lsblk -O|grep ATA|awk '{print $1}'`
  do
    wwn=`lsblk -o NAME,WWN|grep $i|awk '{print $2}'`
    disk_map["$i"]=$wwn
  done

  echo "disk_map length: " ${#disk_map[@]}

  [ -e $diskinfo ] && rm -r diskinfo

  for m in ${!disk_map[@]}
  do
    echo "Disk:"$m" <==> ""wwn:"${disk_map[$m]} >> $diskinfo
  done
}

#根据磁盘数量创建数据目录和日志目录，目前的数据目录格式统一是/data/chunkserver+num，日志目录在/data/log/chunkserver+num
function chunk_dir_prep {
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
    mkdir -p ${DATA_DIR}/log/chunkserver$i
  done
}

#格式化磁盘文件系统
function disk_format {
  for disk in ${!disk_map[@]}
  do
    echo $disk
    mkfs.ext4 -F /dev/$disk 2>&1 > /dev/null &
  done
}

#将创建好的数据目录按照顺序挂载到格式化好的磁盘上，并记录挂载信息到mount.info
function mount_dir {
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
}

#持久化挂载信息到fstab文件，防止系统重启后丢失
function fstab_record {
  grep curvefs /etc/fstab
  if [ $? -ne 0 ]
  then
  echo "#curvefs" >> /etc/fstab
  for i in `lsblk|grep chunkserver|awk '{print $1}'`
  do
    echo "UUID=`ls -l /dev/disk/by-uuid/|grep $i|awk '{print $9}'`    `lsblk|grep $i|awk '{print $7}'`    ext4  rw,errors=remount-ro    0    0" >> /etc/fstab
  done
  fi
}

#将当前的uuid持久化到磁盘上做备份，防止系统重启后uuid发生变化
function meta_record {
  grep curvefs /etc/fstab
  if [ $? -eq 0 ]
  then
  for i in `cat /etc/fstab | grep "/data/chunkserver" | awk '{print $1 $2}' | awk -F '=' '{print $2}'`
  do
    uuid=`echo $i | awk -F / '{print $1}'`
    uuidmd5=`echo -n $uuid | md5sum | cut -d ' ' -f1`
    datadir=`echo $i | awk -F / '{print "/" $2 "/" $3}'`
    touch $datadir/disk.meta
    echo "uuid=$uuid" > $datadir/disk.meta
    echo "uuidmd5=$uuidmd5" >> $datadir/disk.meta
  done
  fi
}

#初始化chunkfile pool
function chunkfile_pool_prep {
ret=`lsblk|grep chunkserver|wc -l`
for i in `seq 0 $((${ret}-1))`
do
  curve-format -allocatePercent=85 \
  -filePoolDir=/data/chunkserver$i/chunkfilepool \
  -filePoolMetaPath=/data/chunkserver$i/chunkfilepool.meta \
  -fileSystemPath=/data/chunkserver$i/chunkfilepool  &
done
wait
}

function deploy_one_walfile_pool {
  curve-format -allocatePercent=10 \
  -filePoolDir=/data/chunkserver$1/walfilepool \
  -filePoolMetaPath=/data/chunkserver$1/walfilepool.meta \
  -fileSize=8388608 \
  -fileSystemPath=/data/chunkserver$1/walfilepool  &
}

function release_disk_reserved_space {
  disks=`lsscsi |grep ATA|awk '{print $7}'|awk -F/ '{print $3}'`
  for disk in ${disks}
  do
    sudo tune2fs -m 0 /dev/$disk
  done
}

# format walfile pool
function walfile_pool_prep {
  ret=`lsblk|grep chunkserver|wc -l`
  for i in `seq 0 $((${ret}-1))`
  do
    deploy_one_walfile_pool $i
  done
  wait
}

function usage {
  echo "HELP: this tool will prepare the chunkserver running env."
  echo "      you can deploy all the disks by setting all"
  echo "      or deploy one disk by setting the diskname"
  echo "Example:"
  echo "        ./chunkserver_deploy2.sh all"
  echo "        ./chunkserver_deploy2.sh /dev/sd{id} /data/chunkserver{id}"
  exit
}

function deploy_all {
    deploy_prep;
    record_diskinfo;
    chunk_dir_prep;
    disk_format;
    mount_dir;
    fstab_record;
    meta_record;
    chunkfile_pool_prep;
    release_disk_reserved_space;
}

function deploy_one {
  local diskname=$1
  local dirname=$2
  #磁盘不存在
  lsscsi|grep -w $diskname
  if [ $? -ne 0 ]
  then
    echo "$diskname is not exist!"
    exit
  fi
  #目录不存在
  if [ ! -d $dirname ]
  then
    echo "$dirname is not exist!"
    exit
  fi
  #磁盘正在挂载使用
  mount | grep -w $diskname
  if [ $? -eq 0 ]
  then
    echo "$diskname is being used"
    exit
  fi
  #目录正在挂载使用
  mount | grep -w $dirname
  if [ $? -eq 0 ]
  then
    echo "$dirname is being used"
    exit
  fi
  #排除一下系统盘(目前都是通过ATA字段区分，后续硬件环境变动可能需要修改)
  lsscsi|grep -v ATA|grep -w $diskname
  if [ $? -eq 0 ]
  then
    echo "$diskname is system bootdisk, you can not use it!"
    exit
  fi
  # remove mount from fstab
  line_num=`grep -n $dirname /etc/fstab | cut -f1 -d:`
  if [ -n "$line_num" ]
  then
    sed -i ''${line_num}'d' /etc/fstab
  fi
  # mkfs
  mkfs.ext4 -F $diskname 2>&1 > /dev/null &
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
  record_diskinfo;
  mount $diskname $dirname
  lsblk > ./mount.info
  #更新fstab
  short_diskname=`echo $diskname|awk -F"/" '{print $3}'`
  ls -l /dev/disk/by-uuid|grep -w $short_diskname
  if [ $? -ne 0 ]
  then
    echo "$short_diskname is not exist"
    exit
  fi
  uuid=`ls -l /dev/disk/by-uuid/|grep -w ${short_diskname}|awk '{print $9}'`
  echo "UUID=$uuid    $dirname    ext4  rw,errors=remount-ro    0    0" >> /etc/fstab
  # 将uuid及其md5写到diskmeta中
  uuidmd5=`echo -n $uuid | md5sum | cut -d ' ' -f1`
  touch $dirname/disk.meta
  echo "uuid=$uuid" > $dirname/disk.meta
  echo "uuidmd5=$uuidmd5" >> $dirname/disk.meta
  #格式化chunkfile pool
  curve-format -allocatePercent=85 \
  -filePoolDir=$dirname/chunkfilepool \
  -filePoolMetaPath=$dirname/chunkfilepool.meta \
  -fileSystemPath=$dirname/chunkfilepool  &
  wait
  # release disk reserved space
  sudo tune2fs -m 0 $diskname
  exit
}
