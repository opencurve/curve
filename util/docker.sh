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
    local args=`getopt -o ldorh --long os: -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            --os)
                g_os=$2
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

    sudo docker run -it --rm -w $(pwd) -v $(pwd):$(pwd) -v ${HOME}:${HOME}  --user $(id -u ${USER}):$(id -g ${USER}) -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro -v /etc/sudoers.d/:/etc/sudoers.d/ -v /etc/sudoers:/etc/sudoers:ro -v /etc/shadow:/etc/shadow:ro -v /var/run/docker.sock:/var/run/docker.sock -v /root/.docker:/root/.docker --ulimit core=-1 --privileged opencurvedocker/curve-base:build-$g_os bash
}

############################  MAIN()
main "$@"
