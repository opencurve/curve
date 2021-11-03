#!/usr/bin/env bash

# Copyright (C) 2021 Jingli Chen (Wine93), NetEase Inc.

# metaserevr
# ├── conf
# │   └── metaserver.conf
# ├── logs
# │   └── metaserevr.pid
# ├── sbin
# │   ├── servicectl
# │   └── curvefs-metaserver
# └── lib
#     └── metaserver_lib.so

############################  GLOBAL VARIABLES
g_prefix=""
g_only=""
g_project_name=""
g_etcd_version="v3.4.10"
g_util_dir="$(dirname $(realpath $0))"
g_curvefs_dir="$(dirname $g_util_dir)"
g_curve_dir="$(dirname $g_curvefs_dir)"

g_color_yellow=`printf '\033[33m'`
g_color_red=`printf '\033[31m'`
g_color_normal=`printf '\033[0m'`

############################  BASIC FUNCTIONS
msg() {
    printf '%b' "$1" >&2
}

success() {
    msg "$g_color_yellow[✔]$g_color_normal [$g_project_name] ${1}${2}"
}

die() {
    msg "$g_color_red[✘]$g_color_normal [$g_project_name] ${1}${2}"
    exit 1
}

program_must_exist() {
    command -v $1 >/dev/null 2>&1

    if [ $? -ne 0 ]; then
        die "You must have '$1' installed to continue\n"
    fi
}

############################ FUNCTIONS
usage () {
    cat << _EOC_
Usage:
    install.sh --prefix=PREFIX --only=TARGET

Examples:
    install.sh --prefix=/usr/local/curvefs --only=*
    install.sh --prefix=/usr/local/curvefs --only=metaserver
    install.sh --prefix=/usr/local/curvefs --only=etcd --etcd_version="v3.4.0"
_EOC_
}

get_options() {
    local long_opts="prefix:,only:,etcd_version:,help"
    local args=`getopt -o povh --long $long_opts -n "$0" -- "$@"`
    eval set -- "${args}"
    while true
    do
        case "$1" in
            -p|--prefix)
                g_prefix=$2
                shift 2
                ;;
            -o|--only)
                g_only=$2
                shift 2
                ;;
            -v|--etcd_version)
                g_etcd_version=$2
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

create_project_dir() {
    mkdir -p $1/{conf,logs,sbin,lib,data}
    if [ $? -eq 0 ]; then
        success "create project directory $1 success\n"
    else
        die "create directory $1 failed\n"
    fi
}

copy_file() {
    cp -f "$1" "$2"
    if [ $? -eq 0 ]; then
        success "copy file $1 to $2 success\n"
    else
        die "copy file $1 to $2 failed\n"
    fi
}

get_targets() {
    bazel query 'kind("cc_binary", //curvefs/src/...)' | grep -E "$g_only"
}

gen_servicectl() {
    local src="$g_util_dir/servicectl.sh"
    local dst="$4/servicectl"
    sed -e "s|__PROJECT__|$1|g" \
        -e "s|__BIN_FILENAME__|$2|g" \
        -e "s|__START_ARGS__|$3|g" \
        $src > $dst \
    && chmod a+x $dst

    if [ $? -eq 0 ]; then
        success "generate servicectl success\n"
    else
        die "generate servicectl failed\n"
    fi
}

install_curvefs() {
    for target in `get_targets`
    do
        local regex_target="//(curvefs/src/([^:]+)):(.+)"
        if [[ ! $target =~ $regex_target ]]; then
            die "unknown target: $target\n"
        fi

        local project_name="${BASH_REMATCH[2]}"  # ex: metaserver
        g_project_name=$project_name
        local project_prefix="$g_prefix/$project_name"  # ex: /usr/local/curvefs/metaserver

        local project_bin_filename=${BASH_REMATCH[3]}  # ex: curvefs_metaserver
        local bazel_dir="$g_curve_dir/bazel-bin"
        local binary="$bazel_dir/${BASH_REMATCH[1]}/$project_bin_filename"

        # The below actions:
        #   1) create project directory
        #   2) copy binary to project directory
        #   3) generate servicectl script into project directory.
        create_project_dir $project_prefix
        copy_file "$binary" "$project_prefix/sbin"
        gen_servicectl \
            $project_name \
            $project_bin_filename \
            '--confPath=$g_conf_file' \
            "$project_prefix/sbin"

        success "install $project_name success\n"
    done
}

download_etcd() {
    local now=`date +"%s%6N"`
    local nos_url="https://curve-build.nos-eastchina1.126.net"
    local src="${nos_url}/etcd-${g_etcd_version}-linux-amd64.tar.gz"
    local tmpfile="/tmp/$now-etcd-${g_etcd_version}-linux-amd64.tar.gz"
    local dst="$1"

    msg "download etcd: $src to $dst\n"

    # download etcd tarball and decompress to dst directory
    mkdir -p $dst &&
        curl -L $src -o $tmpfile &&
        tar -zxvf $tmpfile -C $dst --strip-components=1 >/dev/null 2>&1

    local ret=$?
    rm -rf $tmpfile
    if [ $ret -ne 0 ]; then
        die "download etcd-$g_etcd_version failed\n"
    else
        success "download etcd-$g_etcd_version success\n"
    fi
}

install_etcd() {
    local project_name="etcd"
    g_project_name=$project_name

    # The below actions:
    #   1) download etcd tarball from github
    #   2) create project directory
    #   3) copy binary to project directory
    #   4) generate servicectl script into project directory.
    local now=`date +"%s%6N"`
    local dst="/tmp/$now/etcd-$g_etcd_version"
    download_etcd $dst
    local project_prefix="$g_prefix/etcd"
    create_project_dir $project_prefix
    copy_file "$dst/etcd" "$project_prefix/sbin"
    copy_file "$dst/etcdctl" "$project_prefix/sbin"
    gen_servicectl \
        $project_name \
        "etcd" \
        '--config-file=$g_project_prefix/conf/etcd.conf' \
        "$project_prefix/sbin"

    rm -rf "$dst"
    success "install $project_name success\n"
}

main() {
    get_options "$@"

    if [[ $g_prefix == "" || $g_only == "" ]]; then
        usage
        exit 1
    elif [ "$g_only" == "etcd" ]; then
        install_etcd
    else
        install_curvefs
    fi
}

############################  MAIN()
main "$@"
