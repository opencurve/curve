#!/usr/bin/env bash

set -e

g_hadoop_prefix="/home/${USER}/.local/hadoop"
g_hadoop_lib="${g_hadoop_prefix}/share/hadoop/common/lib"
g_hadoop_etc="${g_hadoop_prefix}/etc/hadoop/core-site.xml"
g_libcurvefs_jni="/curve/bazel-bin/curvefs/sdk/java/native/libcurvefs_jni.so"
g_curvefs_hadoop_jar="/curve/curvefs/sdk/java/target/curvefs-hadoop-1.0-SNAPSHOT.jar"

build_jni() {
    (
        cd /curve/curvefs/sdk/java/src/main/java &&
        javac -h /curve/curvefs/sdk/java/native/ io/opencurve/curve/fs/libfs/CurveFSMount.java
    )
    (
        cd /curve &&
        bazel build --compilation_mode=dbg --config=gcc7-later //curvefs/sdk/java/native:curvefs_jni
    )
    (
        rm -rf /curve/curvefs/sdk/java/native/build &&
        mkdir -p /curve/curvefs/sdk/java/native/build &&
        cp "$(realpath ${g_libcurvefs_jni})" /curve/curvefs/sdk/java/native/build
    )
}

build_curvefs_hadoop() {
    (
        cd /curve/curvefs/sdk/java &&
        rm -rf target/classes/libcurvefs_jni.so &&
        mvn package
    )
}

setup_hadoop() {
    (
        cd "${g_hadoop_lib}" &&
        rm -f curvefs-hadoop-1.0-SNAPSHOT.jar &&
        cp "${g_curvefs_hadoop_jar}" curvefs-hadoop-1.0-SNAPSHOT.jar
    )
}

display_output() {
    g_output=/curve/curvefs/sdk/output
    rm -rf ${g_output}
    mkdir -p ${g_output}
    (
        cd "${g_output}" &&
        cp "${g_hadoop_etc}" . &&
        cp "${g_curvefs_hadoop_jar}" .
    )

    echo -e "\nBuild SDK success :"
    echo "-------------------"
    ls -l ${g_output}/*
}

build_jni
build_curvefs_hadoop
setup_hadoop
display_output
