#!/bin/sh

echo "startfs.sh [ start [ s3 | volume ] mountdir | stop | restart [ s3 | volume ] mountdir]"

stop(){
    sudo killall -9 curve_fs_space_allocator_main
    sudo killall -9 curvefs_metaserver
    sudo killall -9 curvefs_mds
    sudo killall -9 fuse_client
    sudo umount ${mountdir}
    echo "stoped"
}

start(){
    echo "start curve_fs_space_allocator_main"
    ./bazel-bin/curvefs/src/space_allocator/curve_fs_space_allocator_main &
    echo "start curvefs_metaserver"
    ./bazel-bin/curvefs/src/metaserver/curvefs_metaserver &
    echo "start curvefs_mds"
    ./bazel-bin/curvefs/src/mds/curvefs_mds &

    if [ "$1" = "volume" ]
    then
    echo "start fuse_client volume"
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.1 ./bazel-bin/curvefs/src/client/fuse_client -f -o volume=/fs -o user=test -o conf=./curvefs/conf/curvefs_client.conf $2 &
    elif [ "$1" = "s3" ]
    then
    echo "start fuse_client s3"
    LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.1 ./bazel-bin/curvefs/src/client/fuse_client -f -o fsname=s3_1 -o fstype=s3 -o user=test -o conf=./curvefs/conf/curvefs_client.conf $2 &
    fi
    ps -aux | grep -v grep | grep curve_fs_space_allocator_main
    ps -aux | grep -v grep | grep curvefs_metaserver
    ps -aux | grep -v grep | grep curvefs_mds
    ps -aux | grep -v grep | grep fuse_client
}

restart(){
    stop
    start $1
}

printhelp(){
    echo "startfs.sh [ start [ s3 | volume ] | stop | restart [ s3 | volume ]]"
}

case "$1" in
"stop")
    stop
    ;;
"start")
    start $2 $3
    ;;
"restart")
    restart $2 $3
    ;;
"help")
    printhelp
    ;;
esac

exit