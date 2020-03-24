#!/bin/bash

nebd_pid=""
nebd_port=""
# rtt超过该值，则认为是不符合预期的
slowrtt=10
# rtt在连续时间内超过上面的值，则认为连接不正常，将进程kill
looptimes=5
# 超时连接的计数器
declare -A slow_times

# 获取nebd server进程号及metric的端口号
function get_nebdport()
{
    nebd_process_info=`sudo ps -ef |grep /usr/bin/nebd-server|grep -v daemon|grep -v grep`
    pid=`echo $nebd_process_info|awk '{print $2}'`
    if [[ $pid -eq $nebd_pid ]];then
        return 0
    fi
    nebd_pid=$pid
    nebd_port_info=`sudo lsof -i|grep $nebd_pid|grep LISTEN`
    nebd_port=`echo $nebd_port_info|awk '{print $9}'|cut -b 3-6`
    echo "check nebd connection healthy status. pid : $nebd_pid, client port : $nebd_port"
}

# 增加超时连接的计数
function incref()
{
    if [[ "${!slow_times[*]}" =~ "$1" ]];then
        slow_times[$1]=$((${slow_times[$1]}+1))
    else
        slow_times[$1]=1
    fi
    echo "slowtimes[$1]=${slow_times[$1]}"
}

# 检查连接的rtt是否不符合预期，如果是，增加计数
function check_rtt()
{
    connection_rtt_str=`sudo curl -i -s 127.0.0.1:$nebd_port/connections |grep 'baidu_std'`
    oldifs="$IFS"
    IFS=$'\n'

    for rttline in $connection_rtt_str;do
        rtt=`echo $rttline|awk '{print $(NF-1)}' |sed 's/|\(.*\)\/.*/\1/g'`
        if [[ $rtt -eq "|-" ]];then
            echo "skip this line: $rttline"
            continue
        fi
        localport=`echo $rttline|awk '{print $2}' |sed 's/|\(.*\)|.*/\1/g'`
        isslowtcp=`echo "$rtt $slowrtt"|awk '{print ($1 > $2)}'`
        if [[ $isslowtcp -eq 1 ]];then
            incref $localport
            echo "connection_port $localport connection rtt is slow than $slowrtt, rtt is : $rtt"
        fi
    done
    IFS="$oldifs"
}

# 连续时间内出现超时，则将进程强制kill
function kill_nebd_ifunhealthy()
{
    for connection in ${!slow_times[*]};do
        if [[ ${slow_times[$connection]} -eq $looptimes ]];then
            echo "connection_port $connection is unhealthy, nebd server will be killed. pid : $nebd_pid"
            sudo kill -9 $nebd_pid
        fi
        echo "connection_port $connection slow times: ${slow_times[$connection]}"
    done
}

loop=1
while [[ $loop -le $looptimes ]];do
    echo "check ${loop}th times. "

    get_nebdport
    if [ $? -ne 0 ];then
        echo "get nebd port failed"
        exit 1
    fi

    check_rtt
    if [ $? -ne 0 ];then
        echo "get nebd rtt failed"
        exit 1
    fi

    if [[ $loop -eq $looptimes ]];then
        kill_nebd_ifunhealthy
        loop=1
        # 清空计数器
        unset slow_times
        declare -A slow_times
    else
        let loop++
    fi

    sleep 1
done