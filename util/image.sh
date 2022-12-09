#!/usr/bin/env bash

# $1: bs/fs $2: tag $3: os

############################  BASIC FUNCTIONS
msg() {
    printf '%b' "$1" >&2
}

success() {
    msg "\33[32m[✔]\33[0m ${1}${2}"
}

die() {
    msg "\33[31m[✘]\33[0m ${1}${2}"
    exit 1
}

############################ FUNCTIONS
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

copy_file() {
    cp -f "$1" "$2"
    if [ $? -eq 0 ]; then
        success "copy file $1 to $2 success\n"
    else
        die "copy file $1 to $2 failed\n"
    fi
}


install_pkg() {
    if [ $# -eq 2 ]; then
        pkg=$1
        make install stor=$1 prefix=$2
    else
        pkg=$3
        make install stor=$1 prefix=$2 only=$3
    fi

    if [ $? -eq 0 ]; then
        success "install $pkg success\n"
    else
        die "install $pkg failed\n"
    fi
}

############################  MAIN()

if [[ "$1" != "bs" && "$1" != "fs" ]]; then
    die "\$1 must be either bs or fs\n"
fi

if [ "$1" == "bs" ]; then
    docker_prefix="$(pwd)/docker/$3"
else
    docker_prefix="$(pwd)/curvefs/docker/$3"
fi
prefix="$docker_prefix/curve$1"
mkdir -p $prefix $prefix/conf
install_pkg $1 $prefix
install_pkg $1 $prefix etcd
install_pkg $1 $prefix monitor
copy_file ./thirdparties/memcache/libmemcached-1.1.2/build-libmemcached/src/libmemcached/libmemcached.so $docker_prefix
copy_file ./thirdparties/memcache/libmemcached-1.1.2/build-libmemcached/src/libmemcached/libmemcached.so.11 $docker_prefix
copy_file ./thirdparties/memcache/libmemcached-1.1.2/build-libmemcached/src/libhashkit/libhashkit.so.2 $docker_prefix

if [ "$1" == "bs" ]; then
    paths=`ls conf/* nebd/etc/nebd/*`
else
    paths=`ls curvefs/conf/*`
fi
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

docker pull opencurvedocker/curve-base:$3
docker build -t "$2" "$docker_prefix"
