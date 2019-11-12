#!/bin/bash

# args check and print help
if [ $# -ne 3 ]
then
	echo "Usage: ./chunserver_start.sh {chunkserverID}  {ip}   {port}"
	echo "start all: ./chunkserver_start.sh all xxx.xxx.xxx.xxx yyy"
	echo "start one: ./chunkserver_start.sh chunkserverID xxx.xxx.xxx.xxx yyy "
	exit
fi
#start all
DATA_DIR=/data
#ip check
if [ -z $2 ]
then
echo "ip should'n be empty"
exit
fi

#port check
if [ -z $3 ]
then
echo "port should'n be empty"
exit
fi

ip=$2
port=$3
conf=/etc/curve
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
		    -graceful_quit_on_sigterm=true \
		    -log_dir=${DATA_DIR}/log/chunkserver$i/ &
done
exit
fi

num=`lsblk|grep chunkserver|wc -l`
if [ $1 -lt 0 ]
then
	echo "chunkserver num $1 is not ok"
	exit
fi

if [ $1 -gt $num ]
then
	echo "chunkserver num $1 is not ok"
	exit
fi

ps -efl|grep -w "/data/chunkserver$1"|grep -v grep
if [ $? -eq 0 ]
then
	echo "chunkserver$i is already active!"
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
	    -graceful_quit_on_sigterm=true \
	    -log_dir=${DATA_DIR}/log/chunkserver$1 &

