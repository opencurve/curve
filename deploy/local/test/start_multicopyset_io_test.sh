#!/bin/sh

bin=bazel-bin
#bin=.
raftconf="172.17.0.2:8200:0,172.17.0.2:8201:0,172.17.0.2:8202:0"
io_count=100
io_time=5
io_mode=async
iodepth=4
request_size=4096
io_pattern=randwrite
#request_size=4194304
#io_pattern=write
wait_mode=efficiency
thread_num=2
#verbose=-verbose
verbose=

${bin}/multi-copyset-io-test --raftconf=${raftconf} -io_count ${io_count} -request_size ${request_size} -io_pattern ${io_pattern} -io_time ${io_time} -io_mode=${io_mode} -wait_mode=${wait_mode} -iodepth=${iodepth} -thread_num ${thread_num} ${verbose}
#${bin}/multi-copyset-io-test --raftconf=${raftconf} -io_count ${io_count} -io_time ${io_time} -thread_num ${thread_num} ${verbose}
