#!/usr/bin/env bash

# tmpl.sh = /usr/local/metaserver.conf /tmp/metaserver.conf
# $1: tag $2: os
function tmpl() {
    dsv=$1
    src=$2
    dst=$3
    regex="^([^$dsv]+$dsv[[:space:]]*)(.+)__CURVEADM_TEMPLATE__[[:space:]]+(.+)[[:space:]]+__CURVEADM_TEMPLATE__(.*)$"
    while IFS= read -r line; do
        if [[ ! $line =~ $regex ]]; then
            echo $line
        else
            echo ${BASH_REMATCH[1]}${BASH_REMATCH[3]}
        fi
    done < $src > $dst
}

prefix="$(pwd)/docker/$2/curvefs"
mkdir -p $prefix $prefix/conf
make install prefix="$prefix"
make install prefix="$prefix" only=etcd
make install prefix="$prefix" only=monitor
cp -f ../thirdparties/aws/aws-sdk-cpp/build/aws-cpp-sdk-core/libaws-cpp-sdk-core.so docker/$2
cp -f ../thirdparties/aws/aws-sdk-cpp/build/aws-cpp-sdk-s3-crt/libaws-cpp-sdk-s3-crt.so docker/$2
cp -f ../thirdparties/memcache/libmemcached-1.1.2/build-libmemcached/src/libmemcached/libmemcached.so docker/$2
for file in `ls conf`;
do
    dsv="="
    if [ $file = "etcd.conf" ]; then
        dsv=": "
    fi

    tmpl $dsv "conf/$file" "$prefix/conf/$file"
done

cp ../conf/client.conf "$prefix/conf/curvebs-client.conf"

docker pull opencurvedocker/curve-base:$2
docker build -t "$1" "$(pwd)/docker/$2"
