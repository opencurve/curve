#!/usr/bin/env bash

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
g_os="debian11"

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

print_title() {
    local delimiter=$(printf '=%.0s' {1..20})
    msg "$delimiter [$1] $delimiter\n"
}

############################ FUNCTIONS

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

main() {
    get_options "$@"

    docker run --rm -w /curve -v $(pwd):/curve --rm --user $(id -u ${USER}):$(id -g ${USER}) -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro -v ${HOME}:${HOME} -v $(pwd):/curve --privileged opencurvedocker/curve-base:build-$g_os bash util/build_in_image.sh "$@"
}

############################  MAIN()
main "$@"
