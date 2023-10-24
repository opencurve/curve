#!/usr/bin/env bash

#  Copyright (c) 2023 NetEase Inc.
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

set -o errexit

set -x

dir=$(pwd)
echo "Working directory: ${dir}"

tag_version=$(git status | grep -Ew "HEAD detached at|On branch" | awk '{print $NF}' | awk -F"v" '{print $2}')
if [ -z ${tag_version} ]; then
    echo "not found version info, set version to 9.9.9"
    tag_version=9.9.9
fi

commit_id=$(git rev-parse --short HEAD)
if [ "${RELEASE:-}" != "1" ]; then
    debug="+debug"
else
    debug=""
fi

curve_version=${tag_version}+${commit_id}${debug}

echo "curve version: ${curve_version}"

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

if [ "${CREATE_PY_WHEEL}" == "1" ]; then
    create_python_wheel /usr/bin/python3
    exit 0
fi

if [[ "$1" != "tar" && "$1" != "deb" ]]; then
    echo "Usage: $0 <tar|deb>" 1>&2
    exit 1
fi

source "$(dirname "${BASH_SOURCE}")/docker_opts.sh"

outdir="bazel-bin-merged"

cleandir=(
    curvefs_python/BUILD
    curvefs_python/tmplib/
    curvesnapshot_python/BUILD
    curvesnapshot_python/tmplib/
    *.deb
    *.whl
    *.tar.gz
    build
    $outdir
)

rm -rf "${cleandir[@]}"

echo "start compiling"

make build stor=bs release=${RELEASE:-0} dep=${DEP:-0}

mkdir -p $outdir
for i in $(readlink -f bazel-bin)/*; do
    cp -rf $i $outdir
done

for _ in {1..2}; do
    sudo docker run \
        -it --rm \
        -w /curve \
        -v $(pwd):/curve \
        ${g_docker_opts[@]} \
        -e BAZEL_BIN=${outdir} \
        opencurvedocker/curve-base:build-${OS:-debian11} \
        bash ./curvefs_python/configure.sh python3 # python2 is not built against anymore

    if [ "${RELEASE:-}" == "1" ]; then
        sudo docker run \
            -it --rm \
            -w /curve \
            -v $(pwd):/curve \
            ${g_docker_opts[@]} \
            -e RELEASE=${RELEASE:-0} \
            -e DEP=${DEP:-0} \
            opencurvedocker/curve-base:build-${OS:-debian11} \
            bazel build curvefs_python:curvefs --config=gcc7-later --copt -DHAVE_ZLIB=1 --copt -O2 -s \
            --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
            --copt \
            -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
            -L/curve/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
            --linkopt -L/usr/local/lib ${bazelflags}
    else
        sudo docker run \
            -it --rm \
            -w /curve \
            -v $(pwd):/curve \
            ${g_docker_opts[@]} \
            -e RELEASE=${RELEASE:-0} \
            -e DEP=${DEP:-0} \
            opencurvedocker/curve-base:build-${OS:-debian11} \
            bazel build curvefs_python:curvefs --config=gcc7-later --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s \
            --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
            --copt \
            -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
            -L/curve/curvefs_python/tmplib/ --copt -DCURVEVERSION=${curve_version} \
            --linkopt -L/usr/local/lib ${bazelflags}
    fi

    for i in $(readlink -f bazel-bin)/curvefs*; do
        cp -rf $i bazel-bin-merged
    done
done

echo "end compilation"

function build_deb() {
    # step4 create temporary dir, copy binary, libs, and conf
    mkdir build
    cp -r curve-mds build/
    cp -r curve-chunkserver build/

    cp -r curve-sdk build/
    cp -r curve-tools build/
    cp -r curve-monitor build/
    cp -r curve-snapshotcloneserver build/
    cp -r curve-nginx build/

    mkdir -p build/curve-mds/usr/bin

    mkdir -p build/curve-mds/etc/curve
    mkdir -p build/curve-mds/usr/lib
    mkdir -p build/curve-tools/usr/bin
    cp $outdir/src/mds/main/curvemds build/curve-mds/usr/bin/curve-mds
    cp thirdparties/etcdclient/libetcdclient.so \
        build/curve-mds/usr/lib/libetcdclient.so
    cp $outdir/tools/curvefsTool build/curve-mds/usr/bin/curve-tool
    cp -r tools/snaptool build/curve-tools/usr/bin/snaptool-lib
    cp tools/snaptool/snaptool build/curve-tools/usr/bin/snaptool
    chmod a+x build/curve-tools/usr/bin/snaptool
    cp $outdir/src/tools/curve_tool \
        build/curve-tools/usr/bin/curve_ops_tool
    mkdir -p build/curve-chunkserver/usr/bin
    mkdir -p build/curve-chunkserver/etc/curve
    cp $outdir/src/chunkserver/chunkserver \
        build/curve-chunkserver/usr/bin/curve-chunkserver
    cp $outdir/src/tools/curve_chunkserver_tool \
        build/curve-chunkserver/usr/bin/curve_chunkserver_tool

    cp $outdir/src/tools/curve_format \
        build/curve-chunkserver/usr/bin/curve-format

    mkdir -p build/curve-sdk/usr/curvefs
    mkdir -p build/curve-sdk/usr/bin
    mkdir -p build/curve-sdk/etc/curve
    mkdir -p build/curve-sdk/usr/lib
    mkdir -p build/curve-sdk/usr/include
    cp $outdir/curvefs_python/libcurvefs.so \
        build/curve-sdk/usr/curvefs/_curvefs.so
    cp curvefs_python/curvefs.py build/curve-sdk/usr/curvefs/curvefs.py
    cp curvefs_python/__init__.py build/curve-sdk/usr/curvefs/__init__.py
    cp curvefs_python/curvefs_tool.py build/curve-sdk/usr/curvefs/curvefs_tool.py
    cp curvefs_python/parser.py build/curve-sdk/usr/curvefs/parser.py
    cp curvefs_python/curve build/curve-sdk/usr/bin/curve
    chmod a+x build/curve-sdk/usr/bin/curve
    cp curvefs_python/tmplib/* build/curve-sdk/usr/lib/
    cp include/client/libcurve.h build/curve-sdk/usr/include
    cp include/client/libcbd.h build/curve-sdk/usr/include
    cp include/client/libcurve_define.h build/curve-sdk/usr/include
    mkdir -p build/curve-monitor/etc/curve/monitor
    cp -r monitor/* build/curve-monitor/etc/curve/monitor
    mkdir -p build/curve-snapshotcloneserver/usr/bin
    cp $outdir/src/snapshotcloneserver/snapshotcloneserver \
        build/curve-snapshotcloneserver/usr/bin/curve-snapshotcloneserver

    mkdir -p build/curve-nginx/etc/curve/nginx/app/etc
    mkdir -p build/curve-nginx/etc/curve/nginx/conf
    # step 4.1 prepare for nebd-package
    cp -r nebd/nebd-package build/
    mkdir -p build/nebd-package/usr/include/nebd
    mkdir -p build/nebd-package/usr/bin
    mkdir -p build/nebd-package/usr/lib/nebd

    mkdir -p k8s/nebd/nebd-package/usr/bin
    cp nebd/nebd-package/usr/bin/nebd-daemon k8s/nebd/nebd-package/usr/bin
    sed -i '/^baseLogPath=/cbaseLogPath=/var/log/nebd' k8s/nebd/nebd-package/usr/bin/nebd-daemon
    cp -r k8s/nebd/nebd-package build/k8s-nebd-package
    mkdir -p build/k8s-nebd-package/usr/bin
    mkdir -p build/k8s-nebd-package/usr/lib/nebd

    for i in $(find $outdir/ | grep -w so | grep -v solib | grep -v params | grep -v test | grep -v fake); do
        cp -f $i build/nebd-package/usr/lib/nebd
        cp -f $i build/k8s-nebd-package/usr/lib/nebd
    done

    cp nebd/src/part1/libnebd.h build/nebd-package/usr/include/nebd
    cp $outdir/nebd/src/part2/nebd-server build/nebd-package/usr/bin
    cp $outdir/nebd/src/part2/nebd-server build/k8s-nebd-package/usr/bin

    # step 4.2 prepare for curve-nbd package
    cp -r nbd/nbd-package build
    mkdir -p build/nbd-package/usr/bin
    cp $outdir/nbd/src/curve-nbd build/nbd-package/usr/bin

    cp -r k8s/nbd/nbd-package build/k8s-nbd-package
    mkdir -p build/k8s-nbd-package/usr/bin
    cp $outdir/nbd/src/curve-nbd build/k8s-nbd-package/usr/bin

    # step5 记录到debian包的配置文件，打包debian包
    version="Version: ${curve_version}"
    echo ${version} >>build/curve-mds/DEBIAN/control
    echo ${version} >>build/curve-sdk/DEBIAN/control
    echo ${version} >>build/curve-chunkserver/DEBIAN/control
    echo ${version} >>build/curve-tools/DEBIAN/control
    echo ${version} >>build/curve-monitor/DEBIAN/control
    echo ${version} >>build/curve-snapshotcloneserver/DEBIAN/control
    echo ${version} >>build/curve-nginx/DEBIAN/control
    echo ${version} >>build/nebd-package/DEBIAN/control
    echo ${version} >>build/k8s-nebd-package/DEBIAN/control
    echo ${version} >>build/nbd-package/DEBIAN/control
    echo ${version} >>build/k8s-nbd-package/DEBIAN/control

    dpkg-deb -b build/curve-mds .
    dpkg-deb -b build/curve-sdk .
    dpkg-deb -b build/curve-chunkserver .
    dpkg-deb -b build/curve-tools .
    dpkg-deb -b build/curve-monitor .
    dpkg-deb -b build/curve-snapshotcloneserver .
    dpkg-deb -b build/curve-nginx .
    dpkg-deb -b build/nebd-package .
    dpkg-deb -b build/k8s-nebd-package .
    dpkg-deb -b build/nbd-package .
    dpkg-deb -b build/k8s-nbd-package .
}

function build_tar() {
    # step4 create temporary dir, copy binary, libs, and conf
    echo "start copy"
    mkdir -p build/curve/
    # curve-mds
    mkdir -p build/curve/curve-mds/bin
    mkdir -p build/curve/curve-mds/lib
    cp $outdir/src/mds/main/curvemds build/curve/curve-mds/bin/curve-mds
    cp thirdparties/etcdclient/libetcdclient.so \
        build/curve/curve-mds/lib/libetcdclient.so
    cp $outdir/tools/curvefsTool build/curve/curve-mds/bin/curve-tool
    # curve-tools
    mkdir -p build/curve/curve-tools/bin
    cp $outdir/src/tools/curve_tool \
        build/curve/curve-tools/bin/curve_ops_tool
    cp -r tools/snaptool build/curve/curve-tools/bin/snaptool-lib
    cp tools/snaptool/snaptool build/curve/curve-tools/bin/snaptool
    chmod a+x build/curve/curve-tools/bin/snaptool
    # curve-chunkserver
    mkdir -p build/curve/curve-chunkserver/bin
    cp $outdir/src/chunkserver/chunkserver \
        build/curve/curve-chunkserver/bin/curve-chunkserver
    cp $outdir/src/tools/curve_chunkserver_tool \
        build/curve/curve-chunkserver/bin/curve_chunkserver_tool
    cp $outdir/src/tools/curve_format \
        build/curve/curve-chunkserver/bin/curve-format
    # curve-sdk
    mkdir -p build/curve/curve-sdk/curvefs
    mkdir -p build/curve/curve-sdk/bin
    mkdir -p build/curve/curve-sdk/lib
    mkdir -p build/curve/curve-sdk/include
    cp $outdir/curvefs_python/libcurvefs.so \
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
    cp $outdir/src/snapshotcloneserver/snapshotcloneserver \
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

    for i in $(find $outdir/ | grep -w so | grep -v solib | grep -v params | grep -v test | grep -v fake); do
        cp -f $i build/nebd-package/lib/nebd
    done

    cp nebd/src/part1/libnebd.h build/nebd-package/include/nebd
    cp $outdir/nebd/src/part2/nebd-server build/nebd-package/bin

    # step 4.2 prepare for curve-nbd package
    mkdir -p build/nbd-package/bin
    mkdir -p build/nbd-package/etc
    cp $outdir/nbd/src/curve-nbd build/nbd-package/bin
    cp nbd/nbd-package/usr/bin/map_curve_disk.sh build/nbd-package/bin
    cp nbd/nbd-package/etc/curve/curvetab build/nbd-package/etc
    cp nbd/nbd-package/etc/systemd/system/map_curve_disk.service build/nbd-package/etc

    # step5 package tar
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
    cd $OLDPWD
}

if [ "$1" == "tar" ]; then
    build_tar
elif [ "$1" == "deb" ]; then
    build_deb
else
    echo "Usage: $0 <tar|deb>" 1>&2
    exit 1
fi

# step7 package python wheel
mkdir -p ./build/py_deps_libs
cp -rf ./curvefs_python/tmplib/* ./build/py_deps_libs/
cp -rf ./build/py_deps_libs/* ./curvefs_python/tmplib/
sudo docker run \
    -it --rm \
    -w /curve \
    -v $(pwd):/curve \
    ${g_docker_opts[@]} \
    -e RELEASE=${RELEASE:-0} \
    -e DEP=${DEP:-0} \
    -e CREATE_PY_WHEEL=1 \
    opencurvedocker/curve-base:build-${OS:-debian11} \
    $0
