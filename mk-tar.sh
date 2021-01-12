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
#step1 清除生成的目录和文件
bazel clean
rm -rf curvefs_python/BUILD
rm -rf curvefs_python/tmplib/
rm -rf curvesnapshot_python/BUILD
rm -rf curvesnapshot_python/tmplib/
rm -rf *.whl
rm -rf *.tar.gz
rm -rf build

git submodule update --init
if [ $? -ne 0 ]
then
    echo "submodule init failed"
    exit
fi

#step2 获取tag版本和git提交版本信息
#获取tag版本
tag_version=`git status | grep -w "HEAD detached at" | awk '{print $NF}' | awk -F"v" '{print $2}'`
if [ -z ${tag_version} ]
then
    echo "not found version info, set version to 9.9.9"
    tag_version=9.9.9
fi

#获取git提交版本信息
commit_id=`git show --abbrev-commit HEAD|head -n 1|awk '{print $2}'`
if [ "$1" = "debug" ]
then
    debug="+debug"
else
    debug=""
fi

curve_version=${tag_version}+${commit_id}${debug}

function create_python_wheel() {
    PYTHON_VER=$(basename $1)
    curdir=$(pwd)
    basedir="build/curvefs_${PYTHON_VER}/"

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

        bash ./curvefs_python/configure.sh $(basename ${bin})

        if [ $? -ne 0 ]; then
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

#step3 执行编译
bazel_version=`bazel version | grep "Build label" | awk '{print $3}'`
if [ -z ${bazel_version} ]
then
    echo "please install bazel 0.17.2 first"
    exit
fi
if [ ${bazel_version} != "0.17.2" ]
then
    echo "bazel version must 0.17.2"
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
bash ./curvefs_python/configure.sh python2
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
bash ./curvefs_python/configure.sh python2
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

#step4 创建临时目录，拷贝二进制、lib库和配置模板
echo "start copy"
mkdir -p build/curve/
if [ $? -ne 0 ]
then
    exit
fi
# curve-mds
mkdir -p build/curve/curve-mds/bin
if [ $? -ne 0 ]
then
    exit
fi
mkdir -p build/curve/curve-mds/lib
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/src/mds/main/curvemds build/curve/curve-mds/bin/curve-mds
if [ $? -ne 0 ]
then
    exit
fi
cp thirdparties/etcdclient/libetcdclient.so \
build/curve/curve-mds/lib/libetcdclient.so
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/tools/curvefsTool build/curve/curve-mds/bin/curve-tool
if [ $? -ne 0 ]
then
    exit
fi
# curve-tools
mkdir -p build/curve/curve-tools/bin
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/src/tools/curve_tool \
build/curve/curve-tools/bin/curve_ops_tool
if [ $? -ne 0 ]
then
    exit
fi
cp -r tools/snaptool build/curve/curve-tools/bin/snaptool-lib
cp tools/snaptool/snaptool build/curve/curve-tools/bin/snaptool
chmod a+x build/curve/curve-tools/bin/snaptool
if [ $? -ne 0 ]
then
      exit
fi
# curve-chunkserver
mkdir -p build/curve/curve-chunkserver/bin
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/src/chunkserver/chunkserver \
build/curve/curve-chunkserver/bin/curve-chunkserver
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/src/tools/curve_chunkserver_tool \
build/curve/curve-chunkserver/bin/curve_chunkserver_tool
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/src/tools/curve_format \
build/curve/curve-chunkserver/bin/curve-format
if [ $? -ne 0 ]
then
    exit
fi
# curve-sdk
mkdir -p build/curve/curve-sdk/curvefs
if [ $? -ne 0 ]
then
    exit
fi
mkdir -p build/curve/curve-sdk/bin
if [ $? -ne 0 ]
then
    exit
fi
mkdir -p build/curve/curve-sdk/lib
if [ $? -ne 0 ]
then
    exit
fi
mkdir -p build/curve/curve-sdk/include
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/curvefs_python/libcurvefs.so \
build/curve/curve-sdk/curvefs/_curvefs.so
if [ $? -ne 0 ]
then
    exit
fi
cp curvefs_python/curvefs.py build/curve/curve-sdk/curvefs/curvefs.py
if [ $? -ne 0 ]
then
    exit
fi
cp curvefs_python/__init__.py build/curve/curve-sdk/curvefs/__init__.py
if [ $? -ne 0 ]
then
    exit
fi
cp curvefs_python/curvefs_tool.py build/curve/curve-sdk/curvefs/curvefs_tool.py
if [ $? -ne 0 ]
then
    exit
fi
cp curvefs_python/parser.py build/curve/curve-sdk/curvefs/parser.py
if [ $? -ne 0 ]
then
    exit
fi
cp curvefs_python/curve build/curve/curve-sdk/bin/curve
if [ $? -ne 0 ]
then
    exit
fi
chmod a+x build/curve/curve-sdk/bin/curve
cp curvefs_python/tmplib/* build/curve/curve-sdk/lib/
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/src/client/libcurve.so build/curve/curve-sdk/lib/
cp include/client/libcurve.h build/curve/curve-sdk/include
if [ $? -ne 0 ]
then
    exit
fi
# curve-snapshotcloneserver
mkdir -p build/curve/curve-snapshotcloneserver/bin
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/src/snapshotcloneserver/snapshotcloneserver \
build/curve/curve-snapshotcloneserver/bin/curve-snapshotcloneserver
if [ $? -ne 0 ]
then
    exit
fi
mkdir -p build/curve/curve-snapshotcloneserver/lib
cp thirdparties/etcdclient/libetcdclient.so \
build/curve/curve-snapshotcloneserver/lib/libetcdclient.so
if [ $? -ne 0 ]
then
    exit
fi
# curve-nginx
mkdir -p build/curve/curve-nginx/app/etc
if [ $? -ne 0 ]
then
    exit
fi
cp -r ./curve-snapshotcloneserver-nginx/app/lib \
build/curve/curve-nginx/app
if [ $? -ne 0 ]
then
    exit
fi
cp -r ./curve-snapshotcloneserver-nginx/app/src \
build/curve/curve-nginx/app
if [ $? -ne 0 ]
then
    exit
fi
mkdir -p build/curve/curve-nginx/conf
if [ $? -ne 0 ]
then
    exit
fi
cp ./curve-snapshotcloneserver-nginx/conf/mime.types \
build/curve/curve-nginx/conf/
if [ $? -ne 0 ]
then
    exit
fi
cp -r ./curve-snapshotcloneserver-nginx/docker \
build/curve/curve-nginx/
if [ $? -ne 0 ]
then
    exit
fi
# ansible
cp -r curve-ansible build/curve/
if [ $? -ne 0 ]
then
    exit
fi
# README

# curve-monitor
mkdir -p build/curve-monitor
cp -r monitor/* build/curve-monitor/
if [ $? -ne 0 ]
then
    exit
fi
echo "end copy"

# step 4.1 prepare for nebd-package
mkdir -p build/nebd-package/bin
mkdir -p build/nebd-package/lib/nebd

for i in `find bazel-bin/|grep -w so|grep -v solib|grep -v params|grep -v test|grep -v fake`
do
    cp -f $i build/nebd-package/lib/nebd
done

cp bazel-bin/nebd/src/part2/nebd-server build/nebd-package/bin

# step 4.2 prepare for curve-nbd package
mkdir -p build/nbd-package/bin
mkdir -p build/nbd-package/etc
cp bazel-bin/nbd/src/curve-nbd build/nbd-package/bin
cp nbd/nbd-package/usr/bin/map_curve_disk.sh build/nbd-package/bin
cp nbd/nbd-package/etc/curve/curvetab build/nbd-package/etc

#step5 打包tar包
echo "start make tarball"
cd ${dir}/build
curve_name="curve_${curve_version}.tar.gz"
echo "curve_name: ${curve_name}"
tar zcvf ${curve_name} curve
cp ${curve_name} $dir
monitor_name="curve-monitor_${curve_version}.tar.gz"
echo "curve_name: ${curve_name}"
tar zcvf ${monitor_name} curve-monitor
cp ${monitor_name} $dir
nebd_name="nebd_${curve_version}.tar.gz"
echo "nebd_name: ${nebd_name}"
tar zcvf ${nebd_name} nebd-package
cp ${nebd_name} $dir
nbd_name="nbd_${curve_version}.tar.gz"
echo "nbd_name: ${nbd_name}"
tar zcvf ${nbd_name} nbd-package
cp ${nbd_name} $dir
echo "end make tarball"

#step6 清理libetcdclient.so编译出现的临时文件
echo "start clean etcd"
cd ${dir}/thirdparties/etcdclient
make clean
cd ${dir}
echo "end clean etcd"

# step7 打包python whell
echo "start make python whell"
build_curvefs_python $1
echo "end make python whell"
