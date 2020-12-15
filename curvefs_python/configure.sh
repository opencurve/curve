#!/bin/bash

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

echo "********************************************************************"
echo "***********Prepare for curvefs python API build env.****************"
echo "********************************************************************"
echo "First of all, you must build curve all. Then you can run this script."

curve_path=$PWD

echo "curve workspace path is $curve_path"

cd $curve_path/curvefs_python/

echo "Prepare bazel build file"
cp BUILD_bak BUILD -f

usage() {
    echo "Usage: ./configure.sh [python2|python3]"
}

PYTHON_VER=python2

if [ $# -ge 1 ]; then
    PYTHON_VER=$1
fi

echo "${PYTHON_VER}"

if [ "${PYTHON_VER}" = "python2" ] || [ "${PYTHON_VER}" = "python3" ]; then
    PYTHON_H_DIR=`find /usr/include/ -name "${PYTHON_VER}*" | grep "/usr/include/${PYTHON_VER}*" | head -n1`
else
    usage
    exit 1
fi

if [ -z $PYTHON_H_DIR ] || [ ! -f $PYTHON_H_DIR/Python.h ]; then
    echo "Not found include path for Python.h"
    exit 1
fi

sed -i "s%PYTHON_H_DIR%${PYTHON_H_DIR}%g" ${PWD}/BUILD

echo "copy libs to tmplib directory"
libs=`cat BUILD | tr -d "[:blank:]" | grep "^\"-l" | sed 's/[",]//g' | awk '{ print substr($0, 3) }'`

mkdir tmplib
for i in `find $curve_path/bazel-bin/|grep -w so|grep -v solib|grep -v params`
  do
    basename=$(basename $i)
    linkname=`echo $basename | awk -F'.' '{ print $1 }' | awk '{ print substr($0, 4) }'`
    if [[ $libs =~ $linkname ]]
    then
        echo $i
        cp -f $i ./tmplib/
    fi
  done

echo "Prepare env done, you can build curvefs now."
echo "Plesae add --linkopt -L/path/to/tmplib to bazel build command line."
