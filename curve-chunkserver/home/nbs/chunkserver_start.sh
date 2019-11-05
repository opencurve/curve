#!/bin/bash

# default config path
conf=/etc/curve

# args check and print help
if [ $# -lt 1 ]
then
	echo "Usage: ./chunserver_start.sh {chunkserverID}"
	echo "        [-c|--confing path]  chunkserver conf path, default:/etc/curve/chunkserver.conf"
	echo "Examples:"
	echo "start all: ./chunkserver_start.sh all"
	echo "start one: ./chunkserver_start.sh 1"
	exit
fi

if [ $# -gt 2 ]
then
	conf=$3
fi

# parse subnet mask from config
confPath=`echo $conf/chunkserver.conf`
subnet=`cat $confPath|grep global.subnet|awk -F"=" '{print $2}'`
port=`cat $confPath|grep global.port|awk -F"=" '{print $2}'`

prefix=`echo $subnet|awk -F/ '{print $1}'|awk -F. '{printf "%d", ($1*(2^24))+($2*(2^16))+($3*(2^8))+$4}'`
mod=`echo $subnet|awk -F/ '{print $2}'`
mask=$((2**32-2**(32-$mod)))
# 对prefix再取一次模，为了支持10.182.26.50/22这种格式
prefix=$(($prefix&$mask))
for i in `/sbin/ifconfig -a|grep inet|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
do
	# 把ip转换成整数
	ip_int=`echo $i|awk -F. '{printf "%d\n", ($1*(2^24))+($2*(2^16))+($3*(2^8))+$4}'`
	if [ $(($ip_int&$mask)) -eq $prefix ]
	then
		ip=$i
		break
	fi
done

if [ -z "$ip" ]
then
	echo "no ip matched!\n"
	exit
fi

#start all
DATA_DIR=/data

if [ "$1" = "all" ]
then
#ret=`lsblk|grep chunkserver|wc -l`
ret=`lsblk|grep chunkserver|awk '{print $7}'|sed 's/[^0-9]//g'`
for i in $ret
do
	ps -efl|grep -w "/data/chunkserver$i"|grep -v grep
	if [ $? -eq 0 ]
	then
		echo "chunkserver$i is already active!"
		continue
	fi
	mkdir -p ${DATA_DIR}/log/chunkserver$i/
	if [ $? -ne 0 ]
	then
	    echo "Create log dir failed: ${DATA_DIR}/log/chunkserver$i"
		exit
	fi
	curve-chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_max_install_snapshot_tasks_num=5 -raft_sync=true  \
		    -conf=${conf}/chunkserver.conf \
		    -chunkFilePoolDir=${DATA_DIR}/chunkserver$i \
		    -chunkFilePoolMetaPath=${DATA_DIR}/chunkserver$i/chunkfilepool.meta \
		    -chunkServerIp=$ip \
		    -chunkServerPort=$((${port}+${i}))\
		    -chunkServerMetaUri=local:///data/chunkserver$i/chunkserver.dat \
		    -chunkServerStoreUri=local:///data/chunkserver$i/ \
		    -copySetUri=local:///data/chunkserver$i/copysets \
		    -recycleUri=local:///data/chunkserver$i/recycler \
		    -raft_sync_segments=true \
		    -log_dir=${DATA_DIR}/log/chunkserver$i/ > /dev/null 2>&1 &
done
exit
fi

num=`lsblk|grep chunkserver|wc -l`
if [ $1 -lt 0 ]
then
	echo "chunkserver num $1 is not ok"
	exit
fi

if [[ $1 -gt $num ]]
then
	echo "chunkserver num $1 is not ok"
	exit
fi

ps -efl|grep -w "/data/chunkserver$1"|grep -v grep
if [ $? -eq 0 ]
then
	echo "chunkserver$1 is already active!"
	exit
fi

mkdir -p ${DATA_DIR}/log/chunkserver$1
if [ $? -ne 0 ]
then
    echo "Create log dir failed: ${DATA_DIR}/log/chunkserver$1"
	exit
fi
curve-chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_max_install_snapshot_tasks_num=5 -raft_sync=true  \
	    -conf=${conf}/chunkserver.conf \
	    -chunkFilePoolDir=${DATA_DIR}/chunkserver$1 \
	    -chunkFilePoolMetaPath=${DATA_DIR}/chunkserver$1/chunkfilepool.meta \
	    -chunkServerIp=$ip \
	    -chunkServerPort=$((${port}+${1})) \
	    -chunkServerMetaUri=local:///data/chunkserver$1/chunkserver.dat \
	    -chunkServerStoreUri=local:///data/chunkserver$1/ \
	    -copySetUri=local:///data/chunkserver$1/copysets \
	    -recycleUri=local:///data/chunkserver$1/recycler \
	    -raft_sync_segments=true \
	    -log_dir=${DATA_DIR}/log/chunkserver$1 > /dev/null 2>&1 &
