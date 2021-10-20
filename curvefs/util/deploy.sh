#!/usr/bin/env bash

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
g_hosts=""
g_only=""
g_tags=""
g_projects=(etcd mds metaserver tools client)
g_devops_dir="$PWD/devops"

g_color_yellow=`printf '\033[33m'`
g_color_red=`printf '\033[31m'`
g_color_normal=`printf '\033[0m'`

############################  BASIC FUNCTIONS
msg() {
    printf '%b' "$1" >&2
}

success() {
    msg "$g_color_yellow[✔]$g_color_normal ${1}${2}"
}

die() {
    msg "$g_color_red[✘]$g_color_normal ${1}${2}"
    exit 1
}

program_must_exist() {
    command -v $1 >/dev/null 2>&1

    if [ $? -ne 0 ]; then
        die "You must have '$1' installed to continue\n"
    fi
}

############################  FUNCTIONS
usage () {
    cat << _EOC_
Usage:
    deploy.sh --host=HOST --only=ONLY --tags=TAGS...

Examples:
    deploy.sh --host=* --only=* --tags=core,config
    deploy.sh --host=mds --only=* --tags=status
    deploy.sh --host=* --only=mds:metaserver --tags=config,restart
_EOC_
}

get_options() {
    local long_opts="hosts:,only:,tags:"
    local args=`getopt -o eoth --long $long_opts -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            -e|--hosts)
                g_hosts=$2
                shift 2
                ;;
            -o|--only)
                g_only=$2
                shift 2
                ;;
            -t|--tags)
                g_tags=$2
                shift 2
                ;;
            -h|--help)
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

echo_begin() {
    local delimiter=$1
	printf "$g_color_yellow${delimiter}%.0s" $(seq 1 $(tput cols))
	printf "$g_color_normal\n"
}

echo_end() {
    local delimiter=$1
	printf "$g_color_yellow${delimiter}%.0s" $(seq 1 $(tput cols))
	printf "$g_color_normal\n"
}

exec_ansible() {
    cd $g_devops_dir
    for project in ${g_projects[@]};
    do
        local hosts=$project
        if [[ $g_only != "*" && $g_only != $project ]]; then
            continue
        elif [ "$g_hosts" != "*" ]; then
            hosts=$g_hosts
        fi

        echo_begin "-"

        cmd="ansible-playbook $project.yml -e hosts=$hosts -t $g_tags"
        echo "$cmd"
        eval $cmd
        if [ $? -ne 0 ]; then
            die "$cmd\nSee above message for help\n"
        else
            success "$cmd\n"
        fi

        echo_end "-"
    done
}

main() {
    get_options "$@"

    if [ $1 == "begin" ]; then
        echo_begin ">"
    elif [ $1 == "end" ]; then
        echo_end "<"
    elif [[ $g_hosts == "" || $g_only == "" || $g_tags == "" ]]; then
        usage
        exit 1
    fi

    exec_ansible
}

############################  MAIN()
main "$@"
