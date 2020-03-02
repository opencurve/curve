#!/bin/sh
dir=`pwd`
#step1 清除生成的目录和文件
bazel clean
rm -rf curvefs_python/BUILD
rm -rf curvefs_python/tmplib/
rm -rf curvesnapshot_python/BUILD
rm -rf curvesnapshot_python/tmplib/
rm -rf *.deb
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
if [ "$1" = "debug" ]
then
bazel build ... --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s --define=with_glog=true \
--define=libunwind=true --copt -DGFLAGS_NS=google --copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${tag_version}
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
-L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${tag_version}
if [ $? -ne 0 ]
then
	echo "build phase2 failed"
	exit
fi
else
bazel build ... --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
--define=libunwind=true --copt -DGFLAGS_NS=google --copt \
-Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX --copt -DCURVEVERSION=${tag_version}
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
-L${dir}/curvefs_python/tmplib/ --copt -DCURVEVERSION=${tag_version}
if [ $? -ne 0 ]
then
	echo "build phase2 failed"
	exit
fi
fi

#step3 创建临时目录，拷贝二进制、lib库和配置模板
mkdir build
if [ $? -ne 0 ]
then
	exit
fi
cp -r curve-mds build/
if [ $? -ne 0 ]
then
	exit
fi
cp -r curve-chunkserver build/
if [ $? -ne 0 ]
then
	exit
fi
cp -r curve-sdk build/
if [ $? -ne 0 ]
then
	exit
fi
cp -r curve-tools build/
if [ $? -ne 0 ]
then
	exit
fi
cp -r curve-monitor build/
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-mds/usr/bin
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-mds/etc/curve
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-mds/usr/lib
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-tools/usr/bin
if [ $? -ne 0 ]
then
	exit
fi
cp ./bazel-bin/src/mds/main/curvemds build/curve-mds/usr/bin/curve-mds
if [ $? -ne 0 ]
then
	exit
fi
#cp ./bazel-bin/src/tools/curve_status_tool \
#build/curve-mds/usr/bin/curve_status_tool
#if [ $? -ne 0 ]
#then
#	exit
#fi
cp thirdparties/etcdclient/libetcdclient.so \
build/curve-mds/usr/lib/libetcdclient.so
if [ $? -ne 0 ]
then
	exit
fi
#cp ./conf/mds.conf build/curve-mds/etc/curve/mds.conf
#if [ $? -ne 0 ]
#then
#	exit
#fi
cp ./bazel-bin/tools/curvefsTool build/curve-mds/usr/bin/curve-tool
if [ $? -ne 0 ]
then
	exit
fi
cp ./bazel-bin/src/tools/curve_tool \
build/curve-tools/usr/bin/curve_ops_tool
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-chunkserver/usr/bin
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-chunkserver/etc/curve
if [ $? -ne 0 ]
then
	exit
fi
cp ./bazel-bin/src/chunkserver/chunkserver \
build/curve-chunkserver/usr/bin/curve-chunkserver
if [ $? -ne 0 ]
then
	exit
fi
cp ./bazel-bin/src/tools/curve_chunkserver_tool \
build/curve-chunkserver/usr/bin/curve_chunkserver_tool
if [ $? -ne 0 ]
then
	exit
fi
#cp ./conf/chunkserver.conf.example \
#build/curve-chunkserver/etc/curve/chunkserver.conf
#if [ $? -ne 0 ]
#then
#	exit
#fi
#cp ./conf/s3.conf build/curve-chunkserver/etc/curve/s3.conf
#if [ $? -ne 0 ]
#then
#	exit
#fi

cp ./bazel-bin/src/tools/curve_format \
build/curve-chunkserver/usr/bin/curve-format
if [ $? -ne 0 ]
then
	exit
fi

mkdir -p build/curve-sdk/usr/curvefs
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-sdk/usr/bin
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-sdk/etc/curve
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-sdk/usr/lib
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-sdk/usr/include
if [ $? -ne 0 ]
then
    exit
fi
cp ./bazel-bin/curvefs_python/libcurvefs.so \
build/curve-sdk/usr/curvefs/_curvefs.so
if [ $? -ne 0 ]
then
	exit
fi
cp curvefs_python/curvefs.py build/curve-sdk/usr/curvefs/curvefs.py
if [ $? -ne 0 ]
then
	exit
fi
cp curvefs_python/__init__.py build/curve-sdk/usr/curvefs/__init__.py
if [ $? -ne 0 ]
then
	exit
fi
cp curvefs_python/tmplib/* build/curve-sdk/usr/lib/
if [ $? -ne 0 ]
then
	exit
fi
cp ./bazel-bin/src/client/libcurve.so build/curve-sdk/usr/lib
cp include/client/libcurve.h build/curve-sdk/usr/include
if [ $? -ne 0 ]
then
    exit
fi
#cp ./conf/client.conf build/curve-sdk/etc/curve/client.conf
#if [ $? -ne 0 ]
#then
#	exit
#fi
mkdir -p build/curve-monitor/usr/bin
if [ $? -ne 0 ]
then
	exit
fi
mkdir -p build/curve-monitor/etc/curve/monitor
if [ $? -ne 0 ]
then
	exit
fi
cp monitor/curve-monitor.sh build/curve-monitor/usr/bin/curve-monitor.sh
if [ $? -ne 0 ]
then
	exit
fi
cp -r monitor/* build/curve-monitor/etc/curve/monitor
if [ $? -ne 0 ]
then
	exit
fi

#step4 获取git提交版本信息，记录到debian包的配置文件
commit_id=`git show --abbrev-commit HEAD|head -n 1|awk '{print $2}'`
if [ "$1" = "debug" ]
then
	debug="+debug"
else
	debug=""
fi
version="Version: ${tag_version}+${commit_id}${debug}"
echo ${version} >> build/curve-mds/DEBIAN/control
echo ${version} >> build/curve-sdk/DEBIAN/control
echo ${version} >> build/curve-chunkserver/DEBIAN/control
echo ${version} >> build/curve-tools/DEBIAN/control
echo ${version} >> build/curve-monitor/DEBIAN/control

#step5 打包debian包
dpkg-deb -b build/curve-mds .
dpkg-deb -b build/curve-sdk .
dpkg-deb -b build/curve-chunkserver .
dpkg-deb -b build/curve-tools .
dpkg-deb -b build/curve-monitor .
#aws-c-common(commit=0302570a3cbabd98293ee03971e0867f28355086)
#aws-checksums(commit=78be31b81a2b0445597e60ecb2412bc44e762a99)
#aws-c-event-stream(commit=ad9a8b2a42d6c6ef07ccf251b5038b89487eacb3)
#aws-sdk-cpp(commit=2330d84a30ac32ad04d4fb9baf88cda4f8b9b190)
dpkg-deb -b thirdparties/aws-sdk .
