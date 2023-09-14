#!/bin/bash

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

dir=$(pwd)

# Step1 Clear generated directories and files
bazel clean

cleandir=(
    curvefs_python/BUILD
    curvefs_python/tmplib/
    curvesnapshot_python/BUILD
    curvesnapshot_python/tmplib/
    *.deb
    *.whl
    *.tar.gz
    build
)

rm -rf "${cleandir[@]}"

git submodule update --init

# Step2 Obtaining Tag Version and Git Submission Version Information
# Get Tag Version
tag_version=$(git status | grep -Ew "HEAD detached at|On branch" | awk '{print $NF}' | awk -F"v" '{print $2}')
if [ -z ${tag_version} ]; then
    echo "not found version info, set version to 9.9.9"
    tag_version=9.9.9
fi

# Obtain git submission version information
commit_id=$(git rev-parse --short HEAD)
if [ "$1" = "debug" ]; then
    debug="+debug"
else
    debug=""
fi

curve_version=${tag_version}+${commit_id}${debug}

function create_python_wheel() {
    local PYTHON_VER=$(basename $1)
    local curdir=$(pwd)
    local basedir="build/curvefs_${PYTHON_VER}/"

    mkdir -p ${basedir}/tmplib
    mkdir -p ${basedir}/curvefs

    cp ./curvefs_python/tmplib/* ${basedir}/tmplib
    cp ./curvefs_python/setup.py ${basedir}/setup.py
    cp ./curvefs_python/__init__.py ${basedir}/curvefs
    cp ./curvefs_python/curvefs.py ${basedir}/curvefs
    cp ./bazel-bin/curvefs_python/libcurvefs.so ${basedir}/curvefs/_curvefs.so

    cd ${basedir}
    sed -i "s/version-anchor/${curve_version}/g" setup.py

    deps=$(ldd curvefs/_curvefs.so | awk '{ print $1 }' | sed '/^$/d')
    for i in $(find tmplib/ -name "lib*so"); do
        basename=$(basename $i)
        if [[ $deps =~ $basename ]]; then
            echo $i
            cp $i curvefs
        fi
    done

    ${1} setup.py bdist_wheel
    cp dist/*whl ${curdir}

    cd ${curdir}
}

function build_curvefs_python() {
    for bin in "/usr/bin/python3" "/usr/bin/python2"; do
        if [ ! -f ${bin} ]; then
            echo "${bin} not exist"
            continue
        fi

        if ! bash ./curvefs_python/configure.sh $(basename ${bin}); then
            echo "configure for ${bin} failed"
            continue
        fi

        # backup and recover python depends shared libraries
        mkdir -p ./build/py_deps_libs
        cp ./curvefs_python/tmplib/* ./build/py_deps_libs/
        cp ./build/py_deps_libs/* ./curvefs_python/tmplib/

        rm -rf ./bazel-bin/curvefs_python

        if [ "$1" = "release" ]; then
            bazel build curvefs_python:curvefs --copt -DHAVE_ZLIB=1 --copt -O2 -s \
                --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
                --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
                -L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
                ${bazelflags}
        else
            bazel build curvefs_python:curvefs --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s \
                --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
                --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
                -L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
                ${bazelflags}
        fi

        create_python_wheel ${bin}
    done
}

# Step3 Execute Compilation
bazel_version=$(bazel version | grep "Build label" | awk '{print $3}')
if [ -z ${bazel_version} ]; then
    echo "please install bazel 4.2.2 first"
    exit 1
fi
if [ ${bazel_version} != "4.2.2" ]; then
    echo "bazel version must be 4.2.2"
    echo "current version is ${bazel_version}"
    exit 1
fi
echo "bazel version : ${bazel_version}"

# check gcc version, gcc version must >= 4.8.5
gcc_version_major=$(gcc -dumpversion | awk -F'.' '{print $1}')
gcc_version_minor=$(gcc -dumpversion | awk -F'.' '{print $2}')
gcc_version_pathlevel=$(gcc -dumpversion | awk -F'.' '{print $3}')
if [ ${gcc_version_major} -lt 4 ]; then
    echo "gcc version must >= 4.8.5, current version is "$(gcc -dumpversion)
    exit 1
fi

if [[ ${gcc_version_major} -eq 4 ]] && [[ ${gcc_version_minor} -lt 8 ]]; then
    echo "gcc version must >= 4.8.5, current version is "$(gcc -dumpversion)
    exit 1
fi

if [[ ${gcc_version_major} -eq 4 ]] && [[ ${gcc_version_minor} -eq 8 ]] && [[ ${gcc_version_pathlevel} -lt 5 ]]; then
    echo "gcc version must >= 4.8.5, current version is "$(gcc -dumpversion)
    exit 1
fi
echo "gcc version : "$(gcc -dumpversion)

echo "start compiling"

cd ${dir}/thirdparties/etcdclient &&
    make clean &&
    make all &&
    cd $OLDPWD

cp ${dir}/thirdparties/etcdclient/libetcdclient.h ${dir}/include/etcdclient/etcdclient.h

if [ $(gcc -dumpversion | awk -F'.' '{print $1}') -le 6 ]; then
    bazelflags=''
else
    bazelflags='--copt -faligned-new'
fi

if [ "$1" = "debug" ]; then
    make build stor=bs release=0 dep=1 only=src/*

    fail_count=0
    for python in "python2" "python3"; do
        if ! bash ./curvefs_python/configure.sh ${python}; then
            echo "configure ${python} failed"
            let fail_count++
        fi
    done

    if [[ $fail_count -ge 2 ]]; then
        echo "configure python2/3 failed"
        exit
    fi

    bazel build curvefs_python:curvefs --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s \
        --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
        --copt \
        -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
        -L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
        --linkopt -L/usr/local/lib ${bazelflags}
else
    make build stor=bs release=1 dep=1 only=src/*

    fail_count=0
    for python in "python2" "python3"; do
        if ! bash ./curvefs_python/configure.sh ${python}; then
            echo "configure ${python} failed"
            let fail_count++
        fi
    done

    if [[ $fail_count -ge 2 ]]; then
        echo "configure python2/3 failed"
        exit
    fi

    bazel build curvefs_python:curvefs --copt -DHAVE_ZLIB=1 --copt -O2 -s \
        --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
        --copt \
        -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
        -L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
        --linkopt -L/usr/local/lib ${bazelflags}
fi
echo "end compile"

# Step4 Create a temporary directory, copy binaries, lib libraries, and configuration templates
echo "start copy"
mkdir -p build/curve/
# curve-mds
mkdir -p build/curve/curve-mds/bin
mkdir -p build/curve/curve-mds/lib
cp ./bazel-bin/src/mds/main/curvemds build/curve/curve-mds/bin/curve-mds
cp thirdparties/etcdclient/libetcdclient.so \
    build/curve/curve-mds/lib/libetcdclient.so
cp ./bazel-bin/tools/curvefsTool build/curve/curve-mds/bin/curve-tool
# curve-tools
mkdir -p build/curve/curve-tools/bin
cp ./bazel-bin/src/tools/curve_tool \
    build/curve/curve-tools/bin/curve_ops_tool
cp -r tools/snaptool build/curve/curve-tools/bin/snaptool-lib
cp tools/snaptool/snaptool build/curve/curve-tools/bin/snaptool
chmod a+x build/curve/curve-tools/bin/snaptool
# curve-chunkserver
mkdir -p build/curve/curve-chunkserver/bin
cp ./bazel-bin/src/chunkserver/chunkserver \
    build/curve/curve-chunkserver/bin/curve-chunkserver
cp ./bazel-bin/src/tools/curve_chunkserver_tool \
    build/curve/curve-chunkserver/bin/curve_chunkserver_tool
cp ./bazel-bin/src/tools/curve_format \
    build/curve/curve-chunkserver/bin/curve-format
# curve-sdk
mkdir -p build/curve/curve-sdk/curvefs
mkdir -p build/curve/curve-sdk/bin
mkdir -p build/curve/curve-sdk/lib
mkdir -p build/curve/curve-sdk/include
cp ./bazel-bin/curvefs_python/libcurvefs.so \
    build/curve/curve-sdk/curvefs/_curvefs.so
cp curvefs_python/curvefs.py build/curve/curve-sdk/curvefs/curvefs.py
cp curvefs_python/__init__.py build/curve/curve-sdk/curvefs/__init__.py
cp curvefs_python/curvefs_tool.py build/curve/curve-sdk/curvefs/curvefs_tool.py
cp curvefs_python/parser.py build/curve/curve-sdk/curvefs/parser.py
cp curvefs_python/curve build/curve/curve-sdk/bin/curve
chmod a+x build/curve/curve-sdk/bin/curve
cp curvefs_python/tmplib/* build/curve/curve-sdk/lib/
cp include/client/libcurve.h build/curve/curve-sdk/include
cp include/client/libcbd.h build/curve/curve-sdk/include
cp include/client/libcurve_define.h build/curve/curve-sdk/include
# curve-snapshotcloneserver
mkdir -p build/curve/curve-snapshotcloneserver/bin
cp ./bazel-bin/src/snapshotcloneserver/snapshotcloneserver \
    build/curve/curve-snapshotcloneserver/bin/curve-snapshotcloneserver
mkdir -p build/curve/curve-snapshotcloneserver/lib
cp thirdparties/etcdclient/libetcdclient.so \
    build/curve/curve-snapshotcloneserver/lib/libetcdclient.so
# curve-nginx
mkdir -p build/curve/curve-nginx/app/etc
mkdir -p build/curve/curve-nginx/conf
# ansible
cp -r curve-ansible build/curve/
# README

# curve-monitor
mkdir -p build/curve-monitor
cp -r monitor/* build/curve-monitor/
echo "end copy"

# step 4.1 prepare for nebd-package
mkdir -p build/nebd-package/include/nebd
mkdir -p build/nebd-package/bin
mkdir -p build/nebd-package/lib/nebd

for i in $(find bazel-bin/ | grep -w so | grep -v solib | grep -v params | grep -v test | grep -v fake); do
    cp -f $i build/nebd-package/lib/nebd
done

cp nebd/src/part1/libnebd.h build/nebd-package/include/nebd
cp bazel-bin/nebd/src/part2/nebd-server build/nebd-package/bin

# step 4.2 prepare for curve-nbd package
mkdir -p build/nbd-package/bin
mkdir -p build/nbd-package/etc
cp bazel-bin/nbd/src/curve-nbd build/nbd-package/bin
cp nbd/nbd-package/usr/bin/map_curve_disk.sh build/nbd-package/bin
cp nbd/nbd-package/etc/curve/curvetab build/nbd-package/etc
cp nbd/nbd-package/etc/systemd/system/map_curve_disk.service build/nbd-package/etc

# Step5 Packaging tar package
echo "start make tarball"
cd ${dir}/build
curve_name="curve_${curve_version}.tar.gz"
echo "curve_name: ${curve_name}"
tar zcf ${curve_name} curve
cp ${curve_name} $dir
monitor_name="curve-monitor_${curve_version}.tar.gz"
echo "monitor_name: ${monitor_name}"
tar zcf ${monitor_name} curve-monitor
cp ${monitor_name} $dir
nebd_name="nebd_${curve_version}.tar.gz"
echo "nebd_name: ${nebd_name}"
tar zcf ${nebd_name} nebd-package
cp ${nebd_name} $dir
nbd_name="nbd_${curve_version}.tar.gz"
echo "nbd_name: ${nbd_name}"
tar zcf ${nbd_name} nbd-package
cp ${nbd_name} $dir
echo "end make tarball"

# Step6 Clean up temporary files that appear during libetcdclient.so compilation
echo "start clean etcd"
cd ${dir}/thirdparties/etcdclient
make clean
cd ${dir}
echo "end clean etcd"

# Step7 Packaging python wheel
echo "start make python wheel"
build_curvefs_python $1
echo "end make python wheel"
