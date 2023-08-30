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

dir=`pwd`
#Step 1 Clear generated directories and files
bazel clean
rm -rf curvefs_python/BUILD
rm -rf curvefs_python/tmplib/

git submodule update --init
if [ $? -ne 0 ]
then
    echo "submodule init failed"
    exit
fi

#Step 2 Obtaining Tag Version and Git Submission Version Information
#Get Tag Version
tag_version=`git status | grep -w "HEAD detached at" | awk '{print $NF}' | awk -F"v" '{print $2}'`
if [ -z ${tag_version} ]
then
    echo "not found version info, set version to 9.9.9"
    tag_version=9.9.9
fi

#Obtain git submission version information
commit_id=`git show --abbrev-commit HEAD|head -n 1|awk '{print $2}'`
if [ "$1" = "debug" ]
then
    debug="+debug"
else
    debug=""
fi

curve_version=${tag_version}+${commit_id}${debug}


#Step 3 Execute Compilation
# check bazel verion, bazel vesion must = 4.2.2
bazel_version=`bazel version | grep "Build label" | awk '{print $3}'`
if [ -z ${bazel_version} ]
then
    echo "please install bazel 4.2.2 first"
    exit
fi
if [ ${bazel_version} != "4.2.2" ]
then
    echo "bazel version must 4.2.2"
    echo "now version is ${bazel_version}"
    exit
fi
echo "bazel version : ${bazel_version}"

# check gcc version, gcc version must >= 4.8.5
gcc_version_major=`gcc -dumpversion | awk -F'.' '{print $1}'`
gcc_version_minor=`gcc -dumpversion | awk -F'.' '{print $2}'`
gcc_version_pathlevel=`gcc -dumpversion | awk -F'.' '{print $3}'`
if [ ${gcc_version_major} -lt 4 ]
then
    echo "gcc version must >= 4.8.5, current version is "`gcc -dumpversion`
    exit
fi

if [[ ${gcc_version_major} -eq 4 ]] && [[ ${gcc_version_minor} -lt 8 ]]
then
    echo "gcc version must >= 4.8.5, current version is "`gcc -dumpversion`
    exit
fi

if  [[ ${gcc_version_major} -eq 4 ]] && [[ ${gcc_version_minor} -eq 8 ]] && [[ ${gcc_version_pathlevel} -lt 5 ]]
then
    echo "gcc version must >= 4.8.5, current version is "`gcc -dumpversion`
    exit
fi
echo "gcc version : "`gcc -dumpversion`

echo "start compile"
cd ${dir}/thirdparties/etcdclient
make clean
make all
if [ $? -ne 0 ]
then
    echo "make etcd client failed"
    exit
fi
cd ${dir}

cp ${dir}/thirdparties/etcdclient/libetcdclient.h ${dir}/include/etcdclient/etcdclient.h

if [ `gcc -dumpversion | awk -F'.' '{print $1}'` -le 6 ]
then
    bazelflags=''
else
    bazelflags='--copt -faligned-new'
fi

if [ "$1" = "debug" ]
then
bazel build ... --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s --define=with_glog=true \
--define=libunwind=true --copt -DGFLAGS_NS=google --copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${curve_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
    echo "build phase1 failed"
    exit
fi
bash ./curvefs_python/configure.sh
if [ $? -ne 0 ]
then
    echo "configure failed"
    exit
fi
bazel build curvefs_python:curvefs  --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s \
--define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
--copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
-L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
    echo "build phase2 failed"
    exit
fi
else
bazel build ... --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
--define=libunwind=true --copt -DGFLAGS_NS=google --copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${curve_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
    echo "build phase1 failed"
    exit
fi
bash ./curvefs_python/configure.sh
if [ $? -ne 0 ]
then
    echo "configure failed"
    exit
fi
bazel build curvefs_python:curvefs  --copt -DHAVE_ZLIB=1 --copt -O2 -s \
--define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
--copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
-L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
    echo "build phase2 failed"
    exit
fi
fi
echo "end compile"