#!/bin/bash
WORKSPACE="/var/lib/jenkins/workspace/curve/curve_multijob/"
sudo mkdir /var/lib/jenkins/log/curve_unittest/$BUILD_NUMBER
set -e
ulimit -a
ulimit -c unlimited
ulimit -a
ps -ef | grep chunkserver | grep -v grep | awk '{print $2}' | sudo xargs kill -9 || true
ps -ef | grep mds | grep -v grep | awk '{print $2}' | sudo xargs kill -9 || true
ps -ef | grep etcd | grep -v grep | awk '{print $2}' | sudo xargs kill -9 || true
ps -ef | grep test | grep -v grep | awk '{print $2}' | sudo xargs kill -9 || true

if [ -f /home/nbs/etcdclient/libetcdclient.h ] && [ -f /home/nbs/etcdclient/libetcdclient.so ]
then
    cp /home/nbs/etcdclient/libetcdclient.h ${WORKSPACE}thirdparties/etcdclient
    cp /home/nbs/etcdclient/libetcdclient.so ${WORKSPACE}thirdparties/etcdclient
else
    cd ${WORKSPACE}thirdparties/etcdclient && make all
    #sudo cp /curve/curve_multijob/thirdparties/etcdclient/libetcdclient.so /usr/lib/
    #make clean
fi
export LD_LIBRARY_PATH=${WORKSPACE}thirdparties/etcdclient:${LD_LIBRARY_PATH}
cd ${WORKSPACE}
bash replace-curve-repo.sh
mkdir runlog
bazel clean --async
sleep 5
git submodule update --init
bazel build ... --compilation_mode=dbg --collect_code_coverage  --jobs=64 --copt   -DHAVE_ZLIB=1 --define=with_glog=true --define=libunwind=true --copt -DGFLAGS_NS=google --copt -Wno-error=format-security --copt -DUSE_BTHREAD_MUTEX

test_bin_dirs="bazel-bin/test/ bazel-bin/nebd/test/ bazel-bin/nbd/test/"

for i in 0 1 2 3; do mkdir -p $i/{copysets,recycler}; done
for i in `find ${test_bin_dirs} -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1'}`;do sudo $i 2>&1 | tee $i.log  & done  

count=2
check=0
while [ $count -gt 1 ]
do
    sleep 60
    count=`ps -ef | grep test | wc -l`
    echo "==========================================================================================================================================="

    process=`ps -ef  | grep test`
    echo $process
    echo ${count}
    echo "==========================================================================================================================================="

   #for i in `find bazel-bin/test/ -type f -executable -exec file -i '{}' \; | grep  -E 'x-executable|x-sharedlib' | grep "charset=binary" | grep -v ".so"|grep test | grep -Ev 'snapshot-server|snapshot_dummy_server|client-test|server-test|multi|topology_dummy|curve_client_workflow|curve_client_workflow|curve_fake_mds' | awk -F":" '{print $1'}`;do cat $i.log;done
    
    echo "==========================================================================================================================================="
    f1=""
    f2=""
    f1_file=""
    f2_file=""
    now_test=`ps -ef | grep '\-test' | grep -v grep | awk '{print $8}'`
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
./gen-coverage.py
cp ${WORKSPACE}/tools/ci/check_coverage.sh ./
./check_coverage.sh
cp -r coverage ${WORKSPACE}
gcovr -x -r src -e ".*test/.*" -e ".*\.h" -e ".*usr/include/.*" -e ".*/thirdparties/*" -e "/usr/lib/*" -e ".*/external/*" -e ".*/bazel_out/*" -e "/usr/local/include/*" -e "test/*" -e ".*main\.cpp" -e ".*/_objs/snapshotcloneserver/*" -e ".*/_objs/mds/*" --output coverage.xml
cp coverage.xml ${WORKSPACE}
