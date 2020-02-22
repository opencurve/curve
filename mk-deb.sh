#!/bin/bash
dir=`pwd`
# step1 清楚生成的目录和文件
bazel clean
rm -rf *deb
rm -rf build

# step2 编译
if [ "$1" = "debug" ]
then
    bazel build ... --copt -DHAVE_ZLIB=1 --compilation_mode=dbg -s --define=with_glog=true \
        --define=libunwind=true --copt -DGFLAGS_NS=google --copt \
        -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX
    if [ $? -ne 0 ]
    then
        echo "build phase1 failed"
        exit
    fi
else
    bazel build ... --copt -DHAVE_ZLIB=1 --copt -O2 -s --define=with_glog=true \
        --define=libunwind=true --copt -DGFLAGS_NS=google --copt \
        -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX
    if [ $? -ne 0 ]
    then
        echo "build phase1 failed"
        exit
    fi
fi
bazel shutdown

# step3 创建临时目录，拷贝二进制、lib库和配置模板
mkdir build
cp -r nebd-package build/
mkdir -p build/nebd-package/usr/bin
mkdir -p build/nebd-package/usr/lib

for i in `find bazel-bin/|grep -w so|grep -v solib|grep -v params| grep -v test`
    do
        cp -f $i build/nebd-package/usr/lib
    done

cp bazel-bin/src/part2/nebd-server build/nebd-package/usr/bin
cp -r etc build/nebd-package/

# step4 获取git提交版本信息，记录到debian包的配置文件
commit_id=`git show --abbrev-commit HEAD|head -n 1|awk '{print $2}'`
if [ "$1" = "debug" ]
then
	debug="+debug"
else
	debug=""
fi
version="Version: 0.0.1+${commit_id}${debug}"
echo $version >> build/nebd-package/DEBIAN/control

# step5 打包debian包
dpkg-deb -b build/nebd-package .
