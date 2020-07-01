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

#!/bin/sh
echo "********************************************************************"
echo "***********Prepare for curvefs python API build env.****************"
echo "********************************************************************"
echo "First of all, you must build curve all. Then you can run this script."

curve_path=$PWD

echo "curve workspace path is $curve_path"

cd $curve_path/curvesnapshot_python/

echo "Prepare bazel build file"
cp BUILD_bak BUILD -f

echo "copy libs to tmplib directory"

mkdir tmplib
for i in `find $curve_path/bazel-bin/|grep -w so|grep -v solib|grep -v params`
  do
    echo $i
    cp -f $i ./tmplib/
  done

echo "Prepare env done, you can build curvefs now."
echo "Plesae add --linkopt -L/path/to/tmplib to bazel build command line."
