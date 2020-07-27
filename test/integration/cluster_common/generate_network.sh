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

set -e -o pipefail
basedir=$(dirname ${BASH_SOURCE:-$0})
script=${0##${basedir}/}
basedir=$(cd ${basedir};pwd)

usage="USAGE: $script <mdsNum> <csNum> <etcdNum> <bridgename> <netsegment> <netns>"
mdsNum=${1:?"undefined <mdsNum>: $usage"};shift
csNum=${1:?"undefined <csNum>: $usage"};shift
etcdNum=${1:?"undefined <etcdNum>: $usage"};shift
bridge=${1:?"undefined <bridge>: $usage"};shift
networksegment=${1:?"undefined  <network segment>: $usage"};shift
netns=${1:?"undefiend <network ns>: $usage"};shift

add_bridge(){
	local usage="add_bridge <bridge-name> <subnet>"
	local bridge=${1:?"undefined <bridge-name>: $usage"};shift
	local subnet=${1:?"undefined <subnet>: $usage"};shift

	del_bridge $bridge
	ip link add $bridge type bridge
	ip link set dev $bridge up
	return 0
}

del_bridge(){
	local usage="del_bridge <bridge-name>"
	local bridge=${1:?"undefined <bridge-name>:$usage"};shift

	if ip link list | grep "\<$bridge\>" >/dev/null 2>&1;then
		ip link set dev $bridge down
		ip link delete dev $bridge
	fi
	return 0
}

allocate_netns(){
	local usage="allocate_netns <netns-name> <ip> <bridge-name>"
	local netns=${1:?"undefined <netns>: $usage"};shift
	local ip=${1:?"undefined <ip>: $usage"};shift
	local bridge=${1:?"undefined <bridge-name>: $usage"};shift

	local eth=${netns}_eth
	local br_eth=br_${netns}

	if ip netns ls |grep "\<$netns\>";then
		ip netns del $netns
	fi

	if ip link |grep "\<$eth\>"; then
		ip link set $eth down
		ip link del $eth
	fi

	if ip link |grep "\<$br_eth\>"; then
		ip link set $br_eth down
		ip link del $br_eth
	fi

	ip netns add $netns
	ip link add $eth type veth peer name $br_eth
	ip link set $eth netns $netns
	ip netns exec $netns ip link set dev $eth name "eth0"
	ip netns exec $netns ifconfig "eth0" $ip netmask 255.255.255.0 up

	ip link set dev $br_eth master $bridge
	ip link set dev $br_eth up
	return 0
}

create_nefs_network(){
	local usage="create_nefs_network <mdsNum> <csNum> <bridge-name> <subnet>"
	local mdsNum=${1:?"undefined <mdsNum>: $usage"};shift
	local csNum=${1:?"undefined <csNum>: $usage"};shift
	local bridge=${1:?"undefined <bridge-name>: $usage"};shift
	local subnet=${1:?"undefined <subnet>: $usage"};shift

	# bridge
	add_bridge $bridge $subnet
	ifconfig $bridge ${subnet}.1
	# mds
	for i in $(eval "echo {1..$(($mdsNum))}");do
		local ip="${subnet}.$((30+$i))"
		allocate_netns ${netns}_mds$i $ip $bridge
	done

	# cs
	for i in $(eval "echo {1..$(($csNum))}");do
		local ip="${subnet}.$((40+$i))"
		allocate_netns ${netns}_cs$i $ip $bridge
    done

    # etcd
    for i in $(eval "echo {1..$(($etcdNum))}");do
        local ip="${subnet}.$((50+$i))"
        allocate_netns ${netns}_etcd$i $ip $bridge
	done
}

create_nefs_network $mdsNum $csNum $bridge $networksegment $netns
