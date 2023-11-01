#!/usr/bin/env bash

g_bazelisk_url="https://github.com/bazelbuild/bazelisk/releases/download/v1.18.0/bazelisk-linux-amd64"
g_protoc_url="https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-x86_64.zip"

cat << EOF > /etc/apt/sources.list
deb http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-proposed main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy-proposed main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse
EOF

apt-get clean
apt-get -y update
apt-get -y install --no-install-recommends \
    bison \
    build-essential \
    cmake \
    default-jdk \
    flex \
    git \
    golang \
    libcurl4-gnutls-dev \
    libfiu-dev \
    libfuse3-dev \
    libhashkit-dev \
    liblz4-dev \
    libsnappy-dev \
    libssl-dev \
    libz-dev \
    make \
    maven \
    musl \
    musl-dev \
    musl-tools \
    python3-pip \
    sudo \
    tree \
    unzip \
    uuid-dev \
    vim \
    wget
apt-get autoremove -y

wget "${g_bazelisk_url}" -O /usr/bin/bazel
chmod a+x /usr/bin/bazel

g_protoc_zip="/tmp/protoc.zip"
wget "${g_protoc_url}" -O ${g_protoc_zip}
unzip ${g_protoc_zip} "bin/protoc" -d /usr
rm -f ${g_protoc_zip}

cat << EOF >> ~/.bashrc
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export GOPATH=${HOME}/go
export PATH=\$JAVA_HOME/bin:\$PATH
EOF
