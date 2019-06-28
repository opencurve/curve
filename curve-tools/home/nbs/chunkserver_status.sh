#!/bin/bash

for i in `lsblk|grep chunkserver|awk '{print $7}'|awk -F"/" '{print $3}'`
do 
	ps -efl|grep $i|grep -v grep 2>&1 > /dev/null
	if [ $? -eq 0 ]
	then
		echo "$i is active"
	else
		echo "$i is down"
	fi
done

