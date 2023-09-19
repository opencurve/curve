#!/usr/bin/env bash

############################  BUILD BASIC FUNCTIONS
get_version() {
    #get tag version
    tag_version=`git status | grep -w "HEAD detached at" | awk '{print $NF}' | awk -F"v" '{print $2}'`

    # get git commit version
    commit_id=`git show --abbrev-commit HEAD|head -n 1|awk '{print $2}'`
    if [ $g_release -eq 1 ]
    then
        debug="+release"
    else
        debug="+debug"
    fi
    if [ -z ${tag_version} ]
    then
        echo "not found version info"
        curve_version=${commit_id}${debug}
    else
        curve_version=${tag_version}+${commit_id}${debug}
    fi
    echo "version: ${curve_version}"
}

usage () {
    cat << _EOC_
Usage:
    build.sh --stor=bs/fs --list
    build.sh --stor=bs/fs --only=target
Examples:
    build.sh --stor=bs --only=//src/chunkserver:chunkserver
    build.sh --stor=bs --only=//src/chunkserver/datastore:chunkserver_datastore
    build.sh --stor=fs --only=src/*
    build.sh --stor=fs --only=//curvefs/src/metaserver:curvefs_metaserver
    build.sh --stor=fs --only=test/*
    build.sh --stor=bs --only=test/chunkserver
_EOC_
}

############################ BUILD FUNCTIONS

list_target() {
    if [ "$g_stor" == "bs" ]; then
        git submodule update --init -- nbd
        if [ $? -ne 0 ]
        then
            echo "submodule init failed"
            exit
        fi
        print_title " SOURCE TARGETS "
        bazel query 'kind("cc_binary", //src/...)'
        bazel query 'kind("cc_binary", //tools/...)'
        bazel query 'kind("cc_binary", //nebd/src/...)'
        bazel query 'kind("cc_binary", //nbd/src/...)'

        bazel query 'kind("cc_library", //include/...)'
        bazel query 'kind("cc_library", //src/...)'
        bazel query 'kind("cc_library", //nebd/src/...)'
        bazel query 'kind("cc_library", //nbd/src/...)'

        print_title " TEST TARGETS "
        bazel query 'kind("cc_(test|binary)", //test/...)'
        bazel query 'kind("cc_(test|binary)", //nebd/test/...)'
        bazel query 'kind("cc_(test|binary)", //nbd/test/...)'

        bazel query 'kind("cc_library", //test/...)'
        bazel query 'kind("cc_library", //nebd/test/...)'
        bazel query 'kind("cc_library", //nbd/test/...)'
    elif [ "$g_stor" == "fs" ]; then
        print_title "SOURCE TARGETS"
        bazel query 'kind("cc_binary", //curvefs/src/...)'
        bazel query 'kind("cc_library", //curvefs/src/...)'

        print_title " TEST TARGETS "
        bazel query 'kind("cc_(test|binary)", //curvefs/test/...)'
        bazel query 'kind("cc_library", //curvefs/test/...)'
    fi
}


get_options() {
    local args=`getopt -o ldorh --long stor:,list,dep:,only:,os:,release:,ci:,build_rocksdb: -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            -s|--stor)
                g_stor=$2
                shift 2
                ;;
            -l|--list)
                g_list=1
                shift 1
                ;;
            -d|--dep)
                g_depend=$2
                shift 2
                ;;
            -o|--only)
                g_target=$2
                shift 2
                ;;
            -r|--release)
                g_release=$2
                shift 2
                ;;
            -c|--ci)
                g_ci=$2
                shift 2
                ;;
            --os)
                g_os=$2
                shift 2
                ;;
            --build_rocksdb)
                g_build_rocksdb=$2
                shift 2
                ;;
            -h)
                usage
                exit 1
                ;;
            --)
                shift
                break
                ;;
            *)
                exit 1
                ;;
        esac
    done
}

build_target() {
    if [ "$g_stor" == "bs" ]; then
        git submodule update --init -- nbd
        if [ $? -ne 0 ]
        then
            echo "submodule init failed"
            exit
        fi
    fi
    local targets
    declare -A result
    if [ $g_release -eq 1 ]; then
        g_build_opts+=("--compilation_mode=opt --copt -g")
        echo "release" > .BUILD_MODE
    else
        g_build_opts+=("--compilation_mode=dbg")
        echo "debug" > .BUILD_MODE
    fi
    g_build_opts+=("--copt -DCURVEVERSION=${curve_version}")

    if [ `gcc -dumpversion | awk -F'.' '{print $1}'` -gt 6 ]; then
        g_build_opts+=("--config=gcc7-later")
    fi

    local target_array
    if [ "$g_stor" == "bs" ]; then
        if [[ "$g_target" = "*" ]]; then
            target_array=("-- //... -//curvefs/...")
        else
            target_array=($(bazel query 'kind("cc_(test|binary|library)", //... -//curvefs/...)' | grep -E "$g_target"))
        fi
    elif [ "$g_stor" == "fs" ]; then
        if [[ "$g_target" = "*" ]]; then
            target_array=("-- //curvefs/...")
        else
            target_array=($(bazel query 'kind("cc_(test|binary|library)", //curvefs/...)' | grep -E "$g_target"))
        fi
    fi

    if [ $g_ci -eq 1 ]; then
        g_build_opts+=("--collect_code_coverage")
        target_array=("...")
    fi

    for target in "${target_array[@]}"
    do
        bazel build ${g_build_opts[@]} $target
        local ret="$?"
        targets+=("$target")
        result["$target"]=$ret
        if [ "$ret" -ne 0 ]; then
            break
        fi
    done

    echo ""
    print_title " BUILD SUMMARY "
    for target in "${targets[@]}"
    do
        if [ "${result[$target]}" -eq 0 ]; then
            success "$target ${version}\n"
        else
            die "$target ${version}\n"
        fi
    done

    # build tools-v2
    g_toolsv2_root="tools-v2"
    if [ $g_release -eq 1 ]
    then
        (cd ${g_toolsv2_root} && make build version=${curve_version})
    else
        (cd ${g_toolsv2_root} && make debug version=${curve_version})
    fi
}


build_requirements() {
    if [[ "$g_stor" == "fs" || $g_ci -eq 1 ]]; then
        kernel_version=`uname -r | awk -F . '{print $1 * 1000 + $2}'`
        if [ $kernel_version -gt 5001 ]; then
            g_build_opts+=("--define IO_URING_SUPPORT=1")
        fi
        g_rocksdb_root="${PWD}/thirdparties/rocksdb"
        (cd ${g_rocksdb_root} && make build from_source=${g_build_rocksdb} && make install prefix=${g_rocksdb_root})
        g_memcache_root="${PWD}/thirdparties/memcache"
        (cd ${g_memcache_root} && make build)
    fi
    g_etcdclient_root="thirdparties/etcdclient"
    (cd ${g_etcdclient_root} && make clean && make all)
}

