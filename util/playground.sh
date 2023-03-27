#!/usr/bin/env bash

# Copyright (C) 2023 Jingli Chen (Wine93), NetEase Inc.

g_worker_dir="/curve"
g_container_name="curve-build-playground"
g_container_image="opencurvedocker/curve-base:build-debian9"
g_init_script=$(cat << EOF
apt-get -y install rsync golang > /dev/null
mkdir -p ~/.local/gopath
rsync -av --progress \
    ${g_worker_dir}/src/ \
    ${g_worker_dir}/build/ \
    --exclude .git \
    --exclude bazel-* \
    --exclude external \
    > /dev/null
EOF
)

id=$(docker ps --all --format "{{.ID}}" --filter name=${g_container_name})
if [ -z "${id}" ]; then
    docker run -v "$(pwd)":${g_worker_dir}/src \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -dt \
        --name ${g_container_name} \
        --workdir ${g_worker_dir}/build \
        ${g_container_image}
    docker exec ${g_container_name} bash -c "${g_init_script}"
fi

docker exec ${g_container_name} cp -r /curve/src/curvefs /curve/build/
docker exec ${g_container_name} cp -r /curve/src/WORKSPACE /curve/build/
docker exec ${g_container_name} cp -r /curve/src/thirdparties/*.BUILD /curve/build/thirdparties
docker exec -it ${g_container_name} /bin/bash
