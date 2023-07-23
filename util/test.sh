#!/usr/bin/env bash

# Copyright (C) 2023 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
g_type=""
g_only=""
g_exclude="NOTHING"
g_sequence=$(date +"%Y-%m-%d_%H:%M:%S")
g_prefix=".test/${g_sequence}"
g_summary_log="${g_prefix}/summary.log"
g_pass_footer="__PASSED__"
g_fail_footer="__FAILED__"
g_total=0
g_failures=0
g_successes=0
g_failed_cases=()
g_running_cases=()

############################ FUNCTIONS
parse_option() {
    g_type="$1"
    g_only="$2"

    if [ "$g_type" != "fs" ]; then
        die "now we only support run CurveFS tests.\n"
    fi
}

# curvefs/test/common:curvefs-common-test
# curvefs/test/client/volume:curvefs_client_volume_test
# ...
get_files() {
    make list stor=fs 2>/dev/null \
    | grep -E "${g_only}" \
    | grep -v "${g_exclude}" \
    | grep -v "curvefs/test/tools:" \
    | grep -Eo 'curvefs/test/(.+)' \
    | sed 's/:/\//g'
}

run_test() {
    binary="$1"
    output="$2"
    if [ ! -f "${binary}" ]; then
        echo "${g_fail_footer}" >> "${output}"
        return
    fi

    $binary >"${output}" 2>&1
    if [ $? -eq 0 ]; then
        echo "${g_pass_footer}" >> "${output}"
    else
        echo "${g_fail_footer}" >> "${output}"
    fi
}

run_tests() {
    mkdir -p "${g_prefix}"
    files=$(get_files)
    arr=(${files})
    g_total=${#arr[@]}
    for filepath in ${files}; do
        output_dir="${g_prefix}/$(dirname ${filepath})"
        mkdir -p "${output_dir}"
        output_file="${output_dir}/$(basename ${filepath})"
        run_test "$(pwd)/bazel-bin/${filepath}" "${output_file}" &
    done
}

cout() {
    msg "\33[32m${1}\33[0m$2"
}

error() {
    msg "\33[31m${1}\33[0m${2}"
}

warm() {
    msg "\33[33m${1}\33[0m${2}"
}

display_progress() {
    while true; do
        nrun=0
        g_failures=0
        g_successes=0
        g_failed_cases=()
        g_running_cases=()
        for output_file in $(find "${g_prefix}" -type f); do
            footer=$(tail -n 1 "${output_file}" 2>/dev/null)
            if [ "${footer}" = "${g_pass_footer}" ]; then
                g_successes=$((g_successes+1))
                nrun=$((nrun+1))
            elif [ "${footer}" = "${g_fail_footer}" ]; then
                g_failures=$((g_failures+1))
                g_failed_cases+=(${output_file})
                nrun=$((nrun+1))
            else
                g_running_cases+=(${output_file})
            fi
        done

        warm "testing/(${g_sequence}): " "(${g_failures}/${g_successes}/${nrun}/${g_total})\n"

        if [ ${nrun} -eq ${g_total} ]; then
            break
        else
            sleep 3
        fi
    done
}

_summary() {
    cout "\n"
    cout "[==========]\n"
    cout "[   TOTAL  ]" ": ${g_total} tests\n"
    cout "[  PASSED  ]" ": ${g_successes} tests\n"
    cout "[  FAILED  ]" ": ${g_failures} tests\n"
    cout "\n"
    for case in "${g_failed_cases[@]}"; do
        error "FAILED: " "${case}\n"
    done

    for case in "${g_running_cases[@]}"; do
        warm "RUNNING: " "${case}\n"
    done
}

summary() {
    _summary 2>&1 | tee "${g_summary_log}"
    exit 0
}

############################  MAIN()
main() {
    trap 'summary' SIGINT SIGTERM

    source "util/basic.sh"
    parse_option "$@"
    run_tests
    display_progress
    summary
}

main "$@"
