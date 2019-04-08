#!/bin/sh

home=deploy/local/chunkserver
loghome=log
#home=.
conf=${home}/conf
bin=bazel-bin/src/chunkserver
#bin=.

[ -f ${loghome} ] || mkdir -p ${loghome}
[ -f ${loghome}/0 ] || mkdir -p ${loghome}/0
[ -f ${loghome}/1 ] || mkdir -p ${loghome}/1
[ -f ${loghome}/2 ] || mkdir -p ${loghome}/2

${bin}/chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_sync=true -minloglevel=0 -v 19 -conf=${conf}/chunkserver.conf.0 2>${loghome}/0/chunkserver.err &
${bin}/chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_sync=true -minloglevel=0 -v 19 -conf=${conf}/chunkserver.conf.1 2>${loghome}/1/chunkserver.err &
${bin}/chunkserver -bthread_concurrency=18 -raft_max_segment_size=8388608 -raft_sync=true -minloglevel=0 -v 19 -conf=${conf}/chunkserver.conf.2 2>${loghome}/2/chunkserver.err &
