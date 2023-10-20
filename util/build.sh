#!/usr/bin/env bash
set -x

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
g_stor=""
g_list=0
g_depend=0
g_target=""
g_release=0
g_ci=0
g_build_rocksdb=0
g_build_opts=(
    "--define=with_glog=true"
    "--define=libunwind=true"
    "--copt -DHAVE_ZLIB=1"
    "--copt -DGFLAGS_NS=google"
    "--copt -DUSE_BTHREAD_MUTEX"
)

g_os="debian11"
source "$(dirname "${BASH_SOURCE}")/docker_opts.sh"



main() {
    source "util/basic.sh"
    source "util/build_functions.sh"

    get_options "$@"
    get_version
    sudo docker run \
        --rm \
        -w /curve \
        -v $(pwd):/curve \
        ${g_docker_opts[@]} \
        opencurvedocker/curve-base:build-$g_os \
        bash util/build_in_image.sh "$@"
}

############################  MAIN()
main "$@"
