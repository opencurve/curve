#!/usr/bin/env bash

set -e

g_root="${PWD}"

build_jni_library() {
    # 1) generate jni native header file
    local maven_repo="/home/${USER}/.m2/repository"
    cd "${g_root}/curvefs/sdk/java/src/main/java"
    javac -cp "${maven_repo}/org/apache/commons/commons-compress/1.24.0/commons-compress-1.24.0.jar":. \
        -h \
        "${g_root}/curvefs/sdk/java/native/" \
        io/opencurve/curve/fs/libfs/CurveFSMount.java

    # 2) build libcurvefs_jni.so
    cd "${g_root}"
    bazel build \
        --config=gcc7-later \
        --compilation_mode=opt \
        --copt -g \
        --copt -DUNINSTALL_SIGSEGV=1 \
        --copt -DCLIENT_CONF_PATH="\"${g_root}/curvefs/conf/client.conf\"" \
        //curvefs/sdk/java/native:curvefs_jni
}

copy_jni_library() {
    local dest="${g_root}/curvefs/sdk/java/native/build/libcurvefs"
    rm -rf  "${dest}"
    mkdir -p "${dest}"

    # 1) copy the dependencies of the libcurvefs_jni.so
    local jni_so="$(realpath "${g_root}/bazel-bin/curvefs/sdk/java/native/libcurvefs_jni.so")"
    ldd "${jni_so}" | grep '=>' | awk '{ print $1, $3 }' | while read -r line;
    do
        name=$(echo "${line}" | cut -d ' ' -f1)
        path=$(echo "${line}" | cut -d ' ' -f2)
        sudo cp "$(realpath "${path}")" "${dest}/${name}"
    done

    # 2) copy libcurvefs_jni.so
    sudo cp "${jni_so}" "${dest}"

    # 3) achive libcurvefs
    cd "${g_root}/curvefs/sdk/java/native/build/"
    tar -cf libcurvefs.tar libcurvefs
    rm -rf libcurvefs
}

build_hadoop_jar() {
    # build curvefs-hadoop-1.0-SNAPSHOT.jar
    cd "${g_root}/curvefs/sdk/java"
    mvn clean
    mvn package
}

display_output() {
    local src="${g_root}/curvefs/sdk/java/target/curvefs-hadoop-1.0-SNAPSHOT.jar"
    local dest="${g_root}/curvefs/sdk/output";
    rm -rf "${dest}"
    mkdir -p "${dest}"
    cp "${src}" "${dest}"

    echo -e "\nBuild SDK success => ${dest}/curvefs-hadoop-1.0-SNAPSHOT.jar"
}

main() {
    build_jni_library
    copy_jni_library
    build_hadoop_jar
    display_output
}

main
