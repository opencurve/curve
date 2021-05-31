#!/bin/sh

cpplint --recursive curvefs
if [ $? -ne 0 ]
then
    echo "cpplint failed"
    exit
fi

if [ "$1" = "debug" ]
then
DEBUG_FLAG="--compilation_mode=dbg"
fi

bazel build curvefs/src/metaserver:curvefs_metaserver  --copt -DHAVE_ZLIB=1 ${DEBUG_FLAG} -s --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${curve_version} --linkopt -L/usr/local/lib
if [ $? -ne 0 ]
then
    echo "build metaserver failed"
    exit
fi

bazel build curvefs/src/mds:curvefs_mds  --copt -DHAVE_ZLIB=1 ${DEBUG_FLAG} -s --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${curve_version} --linkopt -L/usr/local/lib
if [ $? -ne 0 ]
then
    echo "build mds failed"
    exit
fi

bazel build curvefs/test/metaserver:metaserver_test  --copt -DHAVE_ZLIB=1 ${DEBUG_FLAG} -s --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${curve_version} --linkopt -L/usr/local/lib
if [ $? -ne 0 ]
then
    echo "build mds failed"
    exit
fi

bazel build curvefs/test/mds:mds_test  --copt -DHAVE_ZLIB=1 ${DEBUG_FLAG} -s --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${curve_version} --linkopt -L/usr/local/lib
if [ $? -ne 0 ]
then
    echo "build mds failed"
    exit
fi

if [ "$1" = "test" ]
then
./bazel-bin/curvefs/test/metaserver/metaserver_test
if [ $? -ne 0 ]
then
    echo "metaserver_test failed"
    exit
fi
./bazel-bin/curvefs/test/mds/mds_test
if [ $? -ne 0 ]
then
    echo "mds_test failed"
    exit
fi
fi