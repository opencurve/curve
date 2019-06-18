#!/bin/bash

bridgename="integ_br"
nummds=3
numcs=9
numetcd=3
netsegment=192.168.200
netns="integ"

#Create The Network
if ! ip link show $bridgename 2>&1 >/dev/null ; then
    echo "Network not exist, generate it at first..."
    sudo ./generate_network.sh $nummds $numcs $numetcd $bridgename $netsegment $netns
    if [ $? -ne 0 ] ; then
	./generate_network.sh $nummds $numcs $numetcd $bridgename $netsegment $netns
    fi
fi

if [ $? -ne 0 ] ; then
    echo "generate_network error please check!"
    exit 1
fi