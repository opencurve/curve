#!/bin/bash
set -e
#bazel build tools/... --compilation_mode=dbg --collect_code_coverage  --jobs=64 --copt   -DHAVE_ZLIB=1 --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX > tools.log 
#bazel build src/... --compilation_mode=dbg --collect_code_coverage  --jobs=64 --copt   -DHAVE_ZLIB=1 --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX 
bazel build test/...  --collect_code_coverage  --jobs=64 --copt   -DHAVE_ZLIB=1 --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX 2>&1 > test.log
for i in 0 1 2 3; do mkdir -p $i/{copysets,recycler}; done
for i in `find bazel-bin/test/ -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1'}`;do sudo $i 2>&1 > $i.log;done

for i in `find bazel-bin/test/ -name *.log `;do a=`grep "Failure"  $i`;if [ "$a" != "" ];then echo "fail test is $i"; cat $i;exit -1; fi;done
for i in `find bazel-bin/test/ -name *.log `;do a=`grep "FAILED  ]"  $i`;if [ "$a" != "" ];then echo "fail test is $i"; cat $i;exit -1; fi;done
