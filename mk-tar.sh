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

#获取tag版本
tag_version=`git status | grep -w "HEAD detached at" | awk '{print $NF}' | awk -F"v" '{print $2}'`
if [ -z ${tag_version} ]
then
    echo "not found version info, set version to 9.9.9"
	tag_version=9.9.9
fi

#step2 执行编译
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
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${tag_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
	echo "build phase1 failed"
	exit
fi
sh ./curvefs_python/configure.sh
if [ $? -ne 0 ]
then
	echo "configure failed"
	exit
fi
bazel build curvefs_python:curvefs  --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s \
--define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
--copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
-L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${tag_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
	echo "build phase2 failed"
	exit
fi
else
bazel build ... --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
--define=libunwind=true --copt -DGFLAGS_NS=google --copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${tag_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
	echo "build phase1 failed"
	exit
fi
sh ./curvefs_python/configure.sh
if [ $? -ne 0 ]
then
	echo "configure failed"
	exit
fi
bazel build curvefs_python:curvefs  --copt -DHAVE_ZLIB=1 --copt -O2 -s \
--define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google \
--copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --linkopt \
-L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${tag_version} \
--linkopt -L/usr/local/lib ${bazelflags}
if [ $? -ne 0 ]
then
	echo "build phase2 failed"
	exit
fi
fi
echo "end compile"

#step3 创建临时目录，拷贝二进制、lib库和配置模板
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
# aws
mkdir -p build/curve/aws-sdk
if [ $? -ne 0 ]
then
	exit
fi
cp -r thirdparties/aws-sdk/usr/include \
build/curve/aws-sdk/
if [ $? -ne 0 ]
then
	exit
fi
cp -r thirdparties/aws-sdk/usr/lib \
build/curve/aws-sdk/
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
mkdir -p build/curve-monitor/bin
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-monitor/etc/curve/monitor
if [ $? -ne 0 ]
then
	exit
fi
cp monitor/curve-monitor.sh build/curve-monitor/bin/curve-monitor.sh
if [ $? -ne 0 ]
then
	exit
fi
cp -r monitor/* build/curve-monitor/etc/curve/monitor
if [ $? -ne 0 ]
then
	exit
fi
echo "end copy"

# step 3.1 prepare for nebd-package
mkdir -p build/nebd-package/bin
mkdir -p build/nebd-package/lib/nebd

for i in `find bazel-bin/|grep -w so|grep -v solib|grep -v params|grep -v test|grep -v fake`
do
    cp -f $i build/nebd-package/lib/nebd
done

cp bazel-bin/nebd/src/part2/nebd-server build/nebd-package/bin

# step 3.2 prepare for curve-nbd package
mkdir -p build/nbd-package/bin
cp bazel-bin/nbd/src/curve-nbd build/nbd-package/bin

#step4 获取git提交版本信息
commit_id=`git show --abbrev-commit HEAD|head -n 1|awk '{print $2}'`
if [ "$1" = "debug" ]
then
	debug="+debug"
else
	debug=""
fi

#step5 打包tar包
echo "start make tarball"
cd ${dir}/build
curve_name="curve_${tag_version}+${commit_id}${debug}.tar.gz"
echo "curve_name: ${curve_name}"
tar zcvf ${curve_name} curve
cp ${curve_name} $dir
monitor_name="curve-monitor_${tag_version}+${commit_id}${debug}.tar.gz"
echo "curve_name: ${curve_name}"
tar zcvf ${monitor_name} curve-monitor
cp ${monitor_name} $dir
nebd_name="nebd_${tag_version}+${commit_id}${debug}.tar.gz"
echo "nebd_name: ${nebd_name}"
tar zcvf ${nebd_name} nebd-package
cp ${nebd_name} $dir
nbd_name="nbd_${tag_version}+${commit_id}${debug}.tar.gz"
echo "nbd_name: ${nbd_name}"
tar zcvf ${nbd_name} nbd-package
cp ${nbd_name} $dir
echo "end make tarball"

#step6 清理libetcdclient.so编译出现的临时文件
echo "start clean etcd"
cd ${dir}/thirdparties/etcdclient
make clean
echo "end clean etcd"

# step7 打包python whell
echo "start make python whell"
cd ${dir}/build
cp -r curve/curve-sdk python-wheel
cp ${dir}/curvefs_python/setup.py python-wheel/
cd python-wheel/

# 复制依赖的so文件到curvefs
deps=`ldd curvefs/_curvefs.so | awk '{ print $1 }' | sed '/^$/d'`
for i in `find lib/ -name "lib*so"`
do
    basename=$(basename $i)
    if [[ $deps =~ $basename ]]
    then
        echo $i
        cp $i curvefs
    fi
done

# 替换curvefs setup.py中的版本号
sed -i "s/version-anchor/${tag_version}+${commit_id}${debug}/g" setup.py

python2 setup.py bdist_wheel
cp dist/*whl $dir
echo "end make python whell"
