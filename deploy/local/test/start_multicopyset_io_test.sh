#!/bin/sh

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

bin=bazel-bin
#bin=.
raftconf="127.0.0.1:8200:0,127.0.0.1:8201:0,127.0.0.1:8202:0"
io_count=10
io_time=5
io_mode=async
iodepth=4
request_size=4096
io_pattern=randwrite
#request_size=4194304
#io_pattern=write
wait_mode=efficiency
thread_num=2
verbose=-verbose
#verbose=

${bin}/multi-copyset-io-test --raftconf=${raftconf} -io_count ${io_count} -request_size ${request_size} -io_pattern ${io_pattern} -io_time ${io_time} -io_mode=${io_mode} -wait_mode=${wait_mode} -iodepth=${iodepth} -thread_num ${thread_num} ${verbose}
#${bin}/multi-copyset-io-test --raftconf=${raftconf} -io_count ${io_count} -io_time ${io_time} -thread_num ${thread_num} ${verbose}
