#!/usr/bin/env bash

# tmpl.sh = /usr/local/metaserver.conf /tmp/metaserver.conf
function tmpl() {
    dsv=$1
    src=$2
    dst=$3
    regex="^([^$dsv]+$dsv[[:space:]]*)(.+)__CURVEADM_TEMPLATE__[[:space:]]+(.+)[[:space:]]+__CURVEADM_TEMPLATE__(.*)$"
    while IFS= read -r line; do
        if [[ ! $line =~ $regex ]]; then
            echo "$line"
        else
            echo "${BASH_REMATCH[1]}${BASH_REMATCH[3]}"
        fi
    done < $src > $dst
}

prefix="$(pwd)/docker/$2/curvebs"
mkdir -p $prefix $prefix/conf
make install prefix="$prefix"
make install prefix="$prefix" only=etcd
make install prefix="$prefix" only=monitor
cp -f ./thirdparties/aws/aws-sdk-cpp/build/aws-cpp-sdk-core/libaws-cpp-sdk-core.so docker/$2
cp -f ./thirdparties/aws/aws-sdk-cpp/build/aws-cpp-sdk-s3-crt/libaws-cpp-sdk-s3-crt.so docker/$2
for path in `ls conf/* nebd/etc/nebd/*`;
do
    dir=`dirname $path`
    file=`basename $path`

    # delimiter
    dsv="="
    if [ $file = "etcd.conf" ]; then
        dsv=": "
    fi

    # destination
    dst=$file
    if [ $file = "snapshot_clone_server.conf" ]; then
        dst="snapshotclone.conf"
    fi

    tmpl $dsv "$dir/$file" "$prefix/conf/$dst"
done

docker pull opencurvedocker/curve-base:$2
docker build -t "$1" "$(pwd)/docker/$2"
