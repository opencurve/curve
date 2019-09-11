#!/bin/bash

i=1
while true; do
    # 每10s随机kill一个nebd-server进程
    t1=$(($i % 35))
    if [ $t1 = 0 ]; then
        pids=`pidof nebd-server`
        len=`echo $pids | wc -w`
        if [ $len -gt 0 ]; then
            pid=`echo $pids | awk '{print $1}'`
            echo going to kill -15 $pid
            sudo kill -15 $pid
        fi
    fi

    # 每33s kill掉所有nebd-server进程
    ta=$(($i % 127))
    if [ $ta = 0 ]; then
        echo going to killall -15 nebd-server
        sudo killall -15 nebd-server
    fi

    ((i++))
    sleep 1
done
