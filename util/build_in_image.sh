#!/usr/bin/env bash
set -x

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES

g_root="${PWD}"
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
    "--copt -DCLIENT_CONF_PATH=\"${g_root}/curvefs/conf/client.conf\""
)
# allow user to specify extra build options 
# using environment variable BUILD_OPTS , if any.
# custom build options will be appended to g_build_opts
if [ -n "$BUILD_OPTS" ]; then
    echo "Custom build options: $BUILD_OPTS"
    g_build_opts+=("$BUILD_OPTS")
fi

g_os="debian11"


main() {
    source "util/basic.sh"
    source "util/build_functions.sh"
    get_options "$@"
    get_version

    if [[ "$g_stor" != "bs" && "$g_stor" != "fs" ]]; then
        die "stor option must be either bs or fs\n"
    fi

    if [ "$g_list" -eq 1 ]; then
        list_target
    elif [[ "$g_target" == "" && "$g_depend" -ne 1 ]]; then
        die "must not disable both only option or dep option\n"
    else
        if [ "$g_depend" -eq 1 ]; then
            build_requirements
        fi
        if [ -n "$g_target" ]; then
            build_target
        fi
    fi
}

############################  MAIN()
main "$@"
