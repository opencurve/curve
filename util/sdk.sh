#!/usr/bin/env bash

set -e

(cd /curve/curvefs/sdk/java/src/main/java && javac -h /curve/curvefs/sdk/java/native/ io/opencurve/curve/fs/CurveMount.java)
bazel build --compilation_mode=dbg --config=gcc7-later //curvefs/sdk/java/native:curvefs_jni
(cd /curve/curvefs/sdk/java && mvn package)
(cd /curve/curvefs-hadoop && mvn package)

# cd /home/wine93/.local/hadoop-3.3.6/share/hadoop/common/lib
# ln -s /curve/curvefs/sdk/java/target/libcurvefs-mock-1.0-SNAPSHOT.jar \
#       libcurvefs-mock-1.0-SNAPSHOT.jar

# cd /home/wine93/.local/hadoop-3.3.6/share/hadoop/common/lib
# ln -s /curve/curvefs-hadoop/target/curvefs-hadoop-1.0-SNAPSHOT.jar \
#      curvefs-hadoop-1.0-SNAPSHOT.jar

# cd /usr/lib
# ln -s /curve/bazel-bin/curvefs/sdk/java/native/libcurvefs_jni.so libcurvefs_jni.so
