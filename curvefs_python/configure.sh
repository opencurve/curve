#!/bin/bash
echo "********************************************************************"
echo "***********Prepare for curvefs python API build env.****************"
echo "********************************************************************"
echo "First of all, you must build curve all. Then you can run this script."

curve_path=$PWD

echo "curve workspace path is $curve_path"

cd $curve_path/curvefs_python/

echo "Prepare bazel build file"
cp BUILD_bak BUILD -f

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
