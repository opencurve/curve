#!/usr/bin/env bash

set -e

build_jni_library() {
    local maven_repo="/home/${USER}/.m2/repository"
    cd /curve/curvefs/sdk/java/src/main/java &&
    javac -cp "${maven_repo}/org/apache/commons/commons-compress/1.24.0/commons-compress-1.24.0.jar":. \
        -h \
        /curve/curvefs/sdk/java/native/ \
        io/opencurve/curve/fs/libfs/CurveFSMount.java

    ## TODO: --compilation_mode=opt --copt -g
    cd /curve &&
    bazel build \
        --config=gcc7-later \
        --compilation_mode=dbg \
        --copt -DUNINSTALL_SIGSEGV=1 \
        --copt -DCLIENT_CONF_PATH="\"${PWD}/curvefs/conf/client.conf\"" \
        //curvefs/sdk/java/native:curvefs_jni
}

copy_jni_library() {
    local dest="/curve/curvefs/sdk/java/native/build/libcurvefs"
    rm -rf  "${dest}" && mkdir -p "${dest}"

    local jni_so="$(realpath /curve/bazel-bin/curvefs/sdk/java/native/libcurvefs_jni.so)"
    ldd "${jni_so}" | grep '=>' | awk '{ print $1, $3 }' | while read -r line;
    do
        name=$(echo "${line}" | cut -d ' ' -f1)
        path=$(echo "${line}" | cut -d ' ' -f2)
        sudo cp "$(realpath "${path}")" "${dest}/${name}"
    done
    sudo cp "${jni_so}" "${dest}"

    cd /curve/curvefs/sdk/java/native/build/ &&
        tar -cf libcurvefs.tar libcurvefs &&
        rm -rf libcurvefs
}

build_hadoop_jar() {
    cd /curve/curvefs/sdk/java && mvn clean && mvn package
}

display_output() {
    local src="/curve/curvefs/sdk/java/target/curvefs-hadoop-1.0-SNAPSHOT.jar"
    local dest="/curve/curvefs/sdk/output";
    rm -rf ${dest} &&
    mkdir -p ${dest} &&
    cp "${src}" ${dest}

    echo -e "\nBuild SDK success => ${dest}/curvefs-hadoop-1.0-SNAPSHOT.jar"
}

main() {
    build_jni_library
    copy_jni_library
    build_hadoop_jar
    display_output
}

main
