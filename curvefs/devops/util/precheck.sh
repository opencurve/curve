#!/usr/bin/env bash

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
project=""

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

now() {
    printf `date '+%Y-%m-%d %H:%M:%S'`
}

program_must_exist() {
    local ret='0'
    command -v $1 >/dev/null 2>&1 || { local ret='1'; }

    if [ "$ret" -ne 0 ]; then
        die "You must have '$1' installed to continue.\n"
    fi
}

############################  CHECK POINTS
check_common() {
    exit 0
}

check_etcd() {
    exit 0
}

check_mds() {
    exit 0
}

check_metaserver() {
    exit 0
}

check_space() {
    exit 0
}

check_client() {
    exit 0
}

get_options() {
    while getopts "ht:" opts
    do
        case $opts in
            t)
                project=$OPTARG
                ;;
            h)
                usage
                exit 0
                ;;
            \?)
                usage
                exit 1
                ;;
        esac
    done
}

usage () {
    cat << _EOC_
Usage:
    precheck -t project

Examples:
    precheck -t metaserver
_EOC_
}

main() {
    get_options "$@"

    if [ $project == "" ]; then
        die "You must specify project name"
    fi

    check_common
    case $project in
        etcd)
            check_etcd
            ;;
        mds)
            check_mds
            ;;
        metaserver)
            check_metaserver
            ;;
        space)
            check_space
            ;;
        client)
            check_client
            ;;
    esac
}

############################  MAIN()
main "$@"
