#!/bin/bash

set -e

sed -i "s;https://dl.google.com/go/go1.12.8.linux-amd64.tar.gz;https://curve-build.nos-eastchina1.126.net/go1.12.8.linux-amd64.tar.gz;g" thirdparties/etcdclient/Makefile
sed -i "s;https://github.com/etcd-io/etcd;https://gitee.com/mirrors/etcd;g" thirdparties/etcdclient/Makefile

# braft
sed -i "s;https://github.com/baidu/braft;https://gitee.com/baidu/braft;g" WORKSPACE

# zlib
sed -i "s;https://zlib.net/zlib-1.2.11.tar.gz;https://curve-build.nos-eastchina1.126.net/zlib-1.2.11.tar.gz;g" WORKSPACE

# protobuf
sed -i "s;https://github.com/google/protobuf/archive/v3.6.1.3.zip;https://curve-build.nos-eastchina1.126.net/protobuf-3.6.1.3.zip;g" WORKSPACE

# googletest
sed -i "s;https://github.com/google/googletest;https://gitee.com/mirrors/googletest;g" WORKSPACE

# gflags
sed -i "s;https://github.com/gflags/gflags/archive/v2.2.2.tar.gz;https://curve-build.nos-eastchina1.126.net/gflags-2.2.2.tar.gz;g" WORKSPACE

# leveldb
sed -i "s;https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz;https://curve-build.nos-eastchina1.126.net/leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz;g" WORKSPACE

# brpc
sed -i "s;https://github.com/apache/incubator-brpc;https://gitee.com/baidu/BRPC;g" WORKSPACE

# jsoncpp
sed -i "s;https://github.com/open-source-parsers/jsoncpp.git;https://gitee.com/mirrors/jsoncpp;g" WORKSPACE

# aws-sdk-cpp
sed -i "s;https://github.com/aws/aws-sdk-cpp/archive/1.7.340.tar.gz;https://curve-build.nos-eastchina1.126.net/aws-sdk-cpp-1.7.340.tar.gz;g" WORKSPACE

# glog
sed -i "s;https://github.com/google/glog;https://gitee.com/mirrors/glog;g" WORKSPACE

# aws_c_common
sed -i "s;https://github.com/awslabs/aws-c-common/archive/v0.4.29.tar.gz;https://curve-build.nos-eastchina1.126.net/aws-c-common-0.4.29.tar.gz;g" WORKSPACE

# aws_c_event_stream
sed -i "s;https://github.com/awslabs/aws-c-event-stream/archive/v0.1.4.tar.gz;https://curve-build.nos-eastchina1.126.net/aws-c-event-stream-0.1.4.tar.gz;g" WORKSPACE

# aws_checksums
sed -i "s;https://github.com/awslabs/aws-checksums/archive/v0.1.5.tar.gz;https://curve-build.nos-eastchina1.126.net/aws-checksums-0.1.5.tar.gz;g" WORKSPACE

# abseil-cpp
sed -i "s;https://github.com/abseil/abseil-cpp/archive/refs/tags/20210324.2.tar.gz;https://curve-build.nos-eastchina1.126.net/abseil-cpp-20210324.2.tar.gz;g" WORKSPACE

# platforms
sed -i "s;https://github.com/bazelbuild/platforms/archive/98939346da932eef0b54cf808622f5bb0928f00b.zip;https://curve-build.nos-eastchina1.126.net/platforms-98939346da932eef0b54cf808622f5bb0928f00b.zip;g" WORKSPACE

# rules_cc
sed -i "s;https://github.com/bazelbuild/platforms/archive/98939346da932eef0b54cf808622f5bb0928f00b.zip;https://curve-build.nos-eastchina1.126.net/rules_cc-9e10b8a6db775b1ecd358d8ddd3dab379a2c29a5.zip;g" WORKSPACE

# curve-nbd
sed -i "s;https://github.com/opencurve/curve-nbd;https://gitee.com/NetEase_Curve/curve-nbd;g" .gitmodules
