#!/usr/bin/env bash

# Copyright (C) 2023 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
g_type="$1"

############################ FUNCTIONS
precheck() {
    if [ -z "$(which xq)" ]; then
        die "xq not found, please install it.\n"
    fi

    if [ -z "$(which cpplint)" ]; then
        die "cpplint not found, please install it.\n"
    fi

    if [[ ${g_type} != "bs" && ${g_type} != "fs" ]]; then
        die "please specify storage type: bs or fs\n"
    fi
}

get_files() {
    if [ "$g_type" = "bs" ]; then
        find src test -name '*.h' -or -name '*.cpp'
    elif [ "$g_type" = "fs" ]; then
        find curvefs/src curvefs/test -name '*.h' -or -name '*.cpp'
    else
        die "please specify storage type: bs or fs\n"
    fi
}

run_check() {
    cpplint \
    --linelength=80 \
    --counting=detailed \
    --output=junit \
    --filter=-build/c++11 \
    --quiet $( get_files ) 2>&1 \
    | xq
}

############################  MAIN()
main() {
    source "util/basic.sh"
    precheck
    run_check
}

main "$@"
