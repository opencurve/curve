#!/usr/bin/env bash

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
``
g_os="debian11"
g_ci=0
source "$(dirname "${BASH_SOURCE}")/docker_opts.sh"

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
    local args=`getopt -o ldorh --long os:,ci: -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            --os)
                g_os=$2
                shift 2
                ;;
            -c|--ci)
                g_ci=$2
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

    if [ $g_ci -eq 0 ]; then
        g_docker_opts+=("--rm")
        g_docker_opts+=("-v $(pwd):$(pwd)")
        g_docker_opts+=("-w $(pwd)")
    else
        g_docker_opts+=("-v $(pwd):/var/lib/jenkins/workspace/curve/curve_multijob/")
        g_docker_opts+=("-w /var/lib/jenkins/workspace/curve/curve_multijob/")
    fi

    sudo docker run -it ${g_docker_opts[@]} opencurvedocker/curve-base:build-$g_os bash
}

############################  MAIN()
main "$@"
