#!/bin/bash

set -e

sed -i "s;https://dl.google.com/go/go1.12.8.linux-amd64.tar.gz;https://curve-build.nos-eastchina1.126.net/go1.12.8.linux-amd64.tar.gz;g" thirdparties/etcdclient/Makefile
sed -i "s;https://github.com/etcd-io/etcd;https://gitee.com/mirrors/etcd;g" thirdparties/etcdclient/Makefile

# braft
sed -i "s;https://github.com/baidu/braft;https://gitee.com/baidu/braft;g" WORKSPACE

# protobuf
sed -i "s;https://github.com/google/protobuf/archive/v3.5.0.zip;https://curve-build.nos-eastchina1.126.net/protobuf-3.5.0.zip;g" WORKSPACE

# googletest
sed -i "s;https://github.com/google/googletest;https://gitee.com/mirrors/googletest;g" WORKSPACE

# gflags
sed -i "s;https://mirror.bazel.build/github.com/gflags/gflags/archive/v2.2.2.tar.gz;https://curve-build.nos-eastchina1.126.net/gflags-2.2.2.tar.gz;g" WORKSPACE

# leveldb
sed -i "s;https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz;https://curve-build.nos-eastchina1.126.net/leveldb-a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz;g" WORKSPACE

# brpc
sed -i "s;https://github.com/apache/incubator-brpc;https://gitee.com/baidu/BRPC;g" WORKSPACE

# jsoncpp
sed -i "s;https://github.com/open-source-parsers/jsoncpp.git;https://gitee.com/mirrors/jsoncpp;g" WORKSPACE

# aws
sed -i "s;https://github.com/aws/aws-sdk-cpp/archive/1.7.340.tar.gz;https://curve-build.nos-eastchina1.126.net/aws-sdk-cpp-1.7.340.tar.gz;g" WORKSPACE

# glog
sed -i "s;https://github.com/google/glog;https://gitee.com/mirrors/glog;g" WORKSPACE