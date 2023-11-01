#!/usr/bin/env bash

# Copyright (C) 2023 Jingli Chen (Wine93), NetEase Inc.

############################  GLOBAL VARIABLES
g_obm_cfg=".obm.cfg"
g_worker_dir="/curve"
g_container_name="curve-build-playground.master"
g_container_image="opencurvedocker/curve-build:ubuntu22"
g_init_script=$(cat << EOF
useradd -m -s /bin/bash -N -u $UID $USER
echo "${USER} ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers
chmod 0440 /etc/sudoers
chmod g+w /etc/passwd
echo 'alias ls="ls --color"' >> /home/${USER}/.bashrc
EOF
)

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

############################  FUNCTIONS
parse_cfg() {
    local args=`getopt -o v: --long version: -n "playground.sh" -- "$@"`
    eval set -- "${args}"
    if [ ! -f "${g_obm_cfg}" ]; then
        die "${g_obm_cfg} not found\n"
    fi
    g_container_name=$(cat < "${g_obm_cfg}" | grep -oP '(?<=container_name: ).*')
    g_container_image=$(cat < "${g_obm_cfg}" | grep -oP '(?<=container_image: ).*')
}

create_container() {
    id=$(docker ps --all --format "{{.ID}}" --filter name=${g_container_name})
    if [ -n "${id}" ]; then
        return
    fi

    docker run -v "$(pwd)":${g_worker_dir} \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -dt \
        --cap-add=SYS_PTRACE \
        --security-opt seccomp=unconfined \
        --restart always \
        --env "UID=$(id -u)" \
        --env "USER=${USER}" \
        --env "TZ=Asia/Shanghai" \
        --hostname "${g_container_name}" \
        --name "${g_container_name}" \
        --workdir ${g_worker_dir} \
        "${g_container_image}"

    docker exec "${g_container_name}" bash -c "${g_init_script}"
    success "create ${g_container_name} (${g_container_image}) success :)\n"
}

enter_container() {
    docker exec \
        -u "$(id -u):$(id -g)" \
        -it \
        --env "TERM=xterm-256color" \
        "${g_container_name}" /bin/bash
}

main() {
    parse_cfg "$@"
    create_container
    enter_container
}

############################  MAIN()
main "$@"
