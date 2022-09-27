#!/bin/bash
WORKSPACE="/var/lib/jenkins/workspace/curve/curve_multijob/"
sudo mkdir /var/lib/jenkins/log/curve_unittest/$BUILD_NUMBER
set -e

echo "core_%e_%p_%t_%s" | sudo tee /proc/sys/kernel/core_pattern
cat /proc/sys/kernel/core_pattern

ulimit -a
ulimit -c unlimited
ulimit -a

ps -ef | grep chunkserver | grep -v grep | grep -v gcc | awk '{print $2}' | sudo xargs kill -9 || true
ps -ef | grep mds | grep -v grep | grep -v gcc | awk '{print $2}' | sudo xargs kill -9 || true
ps -ef | grep etcd | grep -v grep | grep -v gcc | awk '{print $2}' | sudo xargs kill -9 || true
ps -ef | grep test | grep -v grep | grep -v gcc | awk '{print $2}' | sudo xargs kill -9 || true

################################################################ __ROCKSDB__
g_build_opts=()
kernel_version=`uname -r | awk -F . '{print $1 * 1000 + $2}'`
if [ $kernel_version -gt 5001 ]; then
    g_build_opts+=("--define IO_URING_SUPPORT=1")
fi
g_rocksdb_root="${PWD}/thirdparties/rocksdb"
(cd ${g_rocksdb_root} && make build && make install prefix=${g_rocksdb_root})
################################################################ __ROCKSDB__

g_aws_sdk_root="${PWD}/thirdparties/aws/"
(cd ${g_aws_sdk_root} && make)

if [ -f /home/nbs/etcdclient/libetcdclient.h ] && [ -f /home/nbs/etcdclient/libetcdclient.so ]
then
    cp /home/nbs/etcdclient/libetcdclient.h ${WORKSPACE}thirdparties/etcdclient
    cp /home/nbs/etcdclient/libetcdclient.so ${WORKSPACE}thirdparties/etcdclient
else
    cd ${WORKSPACE}thirdparties/etcdclient && make all
    #sudo cp /curve/curve_multijob/thirdparties/etcdclient/libetcdclient.so /usr/lib/
    #make clean
fi

export LD_LIBRARY_PATH=${g_aws_sdk_root}/aws-sdk-cpp/build/aws-cpp-sdk-core:${g_aws_sdk_root}/aws-sdk-cpp/build/aws-cpp-sdk-s3-crt:${LD_LIBRARY_PATH}
export LD_LIBRARY_PATH=${WORKSPACE}thirdparties/etcdclient:${LD_LIBRARY_PATH}

cd ${WORKSPACE}
bash replace-curve-repo.sh
mkdir runlog storage
bazel clean --async
sleep 5
git submodule update --init -- nbd

set +e

g_succ=1
g_invalid=()
function bazel_lint() {
    egrep "cc_binary|cc_library|cc_test" $1 >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        egrep "CURVE_DEFAULT_COPTS|CURVE_TEST_COPTS" $1 >/dev/null 2>&1
        if [ $? -ne 0 ]; then
            g_succ=0
            g_invalid+=($1)
        fi
    fi
}

for file in $(find . -name "BUILD"); do
    bazel_lint $file
done

if [ $g_succ -ne 1 ]; then
    echo 'BUILD file with cc_binary|cc_library|cc_test should load copts from "copts.bzl"'
    echo 'And set copts = CURVE_DEFAULT_COPTS or CURVE_TEST_COPTS'
    echo ''
    echo 'Example:'
    echo '  Source Code BUILD: load("//:copts.bzl", "CURVE_DEFAULT_COPTS")'
    echo '  Test Code BULD:    load("//:copts.bzl", "CURVE_TEST_COPTS")'
    echo ''
    echo 'Invalid BUILD files:'

    for f in "${g_invalid[@]}"; do
        echo "  $f"
    done

    exit -1
fi

set -e

################################################################ __GCC VERSION__
if [ `gcc -dumpversion | awk -F'.' '{print $1}'` -gt 6 ]; then
    g_build_opts+=("--config=gcc7-later")
fi

bazel build ... -c dbg --collect_code_coverage --copt -DHAVE_ZLIB=1 --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX ${g_build_opts[@]}

#test_bin_dirs="bazel-bin/test/ bazel-bin/nebd/test/ bazel-bin/curvefs/test/"
if [ $1 == "curvebs" ];then
test_bin_dirs="bazel-bin/test/ bazel-bin/nebd/test/"
elif [ $1 == "curvefs" ];then
test_bin_dirs="bazel-bin/curvefs/test/"
fi
echo $test_bin_dirs


for i in 0 1 2 3; do mkdir -p $i/{copysets,recycler}; done

# run all unittests background
for i in `find ${test_bin_dirs} -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_fake_mds|curvefs_client_memcached_test' | awk -F":" '{print $1}' | sed -n '1,40p' ` ;do sudo $i 2>&1 | tee $i.log  & done
if [ $1 == "curvebs" ];then
sleep 360
fi
for i in `find ${test_bin_dirs} -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1}' | sed -n '41,$p' ` ;do sudo $i 2>&1 | tee $i.log  & done


count=2
check=0
while [ $count -gt 0 ]
do
    sleep 60
    count=`ps -ef | grep test | grep -v 'test[0-9]' | grep -v grep | wc -l`
    echo "==========================================================================================================================================="

    process=`ps -ef | grep test | grep -v 'test[0-9]' `
    echo $process
    echo ${count}
    echo "==========================================================================================================================================="

   #for i in `find bazel-bin/test/ -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1'}`;do cat $i.log;done

    echo "==========================================================================================================================================="
    f1=""
    f2=""
    f1_file=""
    f2_file=""
    now_test=`ps -ef | grep test | grep -v 'test[0-9]' | grep -v grep | awk '{print $8}'`
    echo "now_test case is "$now_test

    for i in `find ${test_bin_dirs} -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1'}`;do a=`cat $i.log | grep "FAILED  ]" | wc -l`;if [ $a -gt 0 ];then f1=`cat $i.log | grep "FAILED  ]"`;f1_file="${i}.log"; echo "fail test is $i"; check=1; fi;done
    for i in `find ${test_bin_dirs} -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1'}`;do b=`cat $i.log | grep "Failure" | wc -l`;if [ $b -gt 0 ];then f2=`cat $i.log | grep "Failure"`; f2_file="${i}.log";echo "fail test is $i"; check=1; fi;done
    if [ $check -eq 1 ];then
         echo "=========================test fail,Here is the logs of failed use cases========================="
         echo "=========================test fail,Here is the logs of failed use cases========================="
         echo "=========================test fail,Here is the logs of failed use cases========================="
         echo $f1
         echo $f2
         if [ "$f1" != "" ];then
             echo "test ${f1_file} log is --------------------------------------------->>>>>>>>"
             cat ${f1_file}
         fi
         if [ "$f2" != "" ];then
         echo "test ${f2_file} log is --------------------------------------------->>>>>>>>"
         cat ${f2_file}
         fi
         sudo find ${test_bin_dirs} -name "*.log" | sudo xargs tar -cvf /var/lib/jenkins/log/curve_unittest/$BUILD_NUMBER/unittest.tgz || true
         sudo cp -r ${WORKSPACE}runlog /var/lib/jenkins/log/curve_unittest/$BUILD_NUMBER/
         exit -1
    fi
done

sudo find ${test_bin_dirs} -name "*.log" | sudo xargs tar -cvf /var/lib/jenkins/log/curve_unittest/$BUILD_NUMBER/unittest.tgz || true
sudo cp -r ${WORKSPACE}runlog /var/lib/jenkins/log/curve_unittest/$BUILD_NUMBER/

#echo "==========================================================================================================================================="

#for i in `find bazel-bin/test/ -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1'}`;do cat $i.log;done

sleep 30
sudo cp  ${WORKSPACE}*core* /var/lib/jenkins/log/curve_unittest/$BUILD_NUMBER/ || true

cd bazel-bin
cp ${WORKSPACE}/tools/ci/*.py ./

cp ${WORKSPACE}/tools/ci/check_coverage.sh ./
if [ $1 == "curvebs" ];then
./gen-coverage_bs.py
./check_coverage.sh "curvebs"
elif [ $1 == "curvefs" ];then
./gen-coverage_fs.py
./check_coverage.sh "curvefs"
fi
cp -r coverage ${WORKSPACE}
if [ $1 == "curvebs" ];then
gcovr -x -r src -e ".*test/.*" -e ".*\.h" -e ".*usr/include/.*" -e ".*/thirdparties/*" -e "/usr/lib/*" -e ".*/external/*" -e ".*/bazel_out/*" -e "/usr/local/include/*" -e "test/*" -e ".*main\.cpp" -e ".*/_objs/snapshotcloneserver/*" -e ".*/_objs/mds/*" --output coverage.xml
elif [ $1 == "curvefs" ];then
gcovr -x -r curvefs/src -e ".*test/.*" -e ".*\.h" -e ".*usr/include/.*" -e ".*/thirdparties/*" -e "/usr/lib/*" -e ".*/external/*" -e ".*/bazel_out/*" -e "/usr/local/include/*" -e "test/*" -e ".*main\.cpp" -e ".*/_objs/snapshotcloneserver/*" -e ".*/_objs/mds/*" --output coverage.xml
fi
cp coverage.xml ${WORKSPACE}
