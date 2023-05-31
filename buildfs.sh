#!/bin/sh

cpplint --recursive curvefs
if [ $? -ne 0 ]
then
    echo "cpplint failed"
    exit
fi

if [ `gcc -dumpversion | awk -F'.' '{print $1}'` -le 6 ]
then
    bazelflags=''
else
    bazelflags='--copt -faligned-new'
fi

if [ "$1" = "debug" ]
then
DEBUG_FLAG="--compilation_mode=dbg"
fi

bazel build curvefs/...  --copt -DHAVE_ZLIB=1 ${DEBUG_FLAG} -s \
--define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt \
-DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${curve_version} --linkopt -L/usr/local/lib ${bazelflags}

if [ $? -ne 0 ]
then
    echo "build curvefs failed"
    exit
fi
echo "build curvefs success"

if [ "$1" = "test" ]
then
./bazel-bin/curvefs/test/metaserver/curvefs_metaserver_test
if [ $? -ne 0 ]
then
    echo "metaserver_test failed"
    exit
fi
./bazel-bin/curvefs/test/mds/curvefs_mds_test
if [ $? -ne 0 ]
then
    echo "mds_test failed"
    exit
fi
fi
echo "end compile"
