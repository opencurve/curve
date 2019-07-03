#!/bin/bash

if [ $# -ne 1 ]
then
	echo "Usage: ./chunkserver_stop.sh all/chunkserverID"
	exit
fi
#stop all
if [ "$1" = "all" ]
then
	echo "kill all chunkservers"
	killall curve-chunkserver
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

echo "kill chunkserver $1"
kill -9 `ps -efl|grep -w chunkserver$1|grep -v grep|awk '{print $4}'`

