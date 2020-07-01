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