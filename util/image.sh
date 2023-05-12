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

prefix="$(pwd)/docker/curvebs"
mkdir -p $prefix $prefix/conf
install_pkg $1 $prefix
install_pkg $1 $prefix etcd
install_pkg $1 $prefix monitor

if [ "$1" == "fs" ];then 
    copy_file ./thirdparties/memcache/libmemcached-1.1.2/build-libmemcached/src/libmemcached/libmemcached.so $docker_prefix
    copy_file ./thirdparties/memcache/libmemcached-1.1.2/build-libmemcached/src/libmemcached/libmemcached.so.11 $docker_prefix
    copy_file ./thirdparties/memcache/libmemcached-1.1.2/build-libmemcached/src/libhashkit/libhashkit.so.2 $docker_prefix 
fi
copy_file thirdparties/etcdclient/libetcdclient.so $docker_prefix


if [ "$1" == "bs" ]; then
    paths=`ls conf/* nebd/etc/nebd/*`
else
    paths=`ls curvefs/conf/*`
fi
paths="$paths tools-v2/pkg/config/curve.yaml"
for path in $paths;
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

docker pull opencurvedocker/curvebs-base:debian9
docker build -t "$1" "$(pwd)/docker"
