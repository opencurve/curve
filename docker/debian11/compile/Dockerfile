FROM debian:11
ENV TZ=Asia/Shanghai \
    LANG=C.UTF-8     \
    LC_ALL=C.UTF-8 \
    GITHUB_PROXY=https://ghproxy.com/ \
    VERSION_ETCD=v3.4.0 \
    DOWNLOAD_ETCD_URL=${GITHUB_PROXY}https://github.com/etcd-io/etcd/releases/download \
    GOPROXY=https://goproxy.io,direct \
    PROTOC_VERSION=21.8 \
    GO_INSTALL_VERSION=1.19.6 \
    PROTOC_GEN_GO_VERSION=v1.28 \
    PROTOC_GEN_GO_GRPC_VERSION=v1.2 \
    BAZELISK_BASE_URL=https://repo.huaweicloud.com/bazel
RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|mirrors.ustc.edu.cn/debian-security|g' /etc/apt/sources.list && \
    apt clean && \
    apt -y update && \
    apt -y install \
        g++ \
        wget \
        clang \
        git \
        libnl-genl-3-dev \
        libssl-dev \
        uuid-dev \
        libcurl4-gnutls-dev \
        libfiu-dev \
        openjdk-11-jdk \
        sudo \
        python3-dev \
        python3-setuptools \
        python3-wheel \
        zlib1g-dev \
        libsnappy-dev \
        make \
        liblz4-dev \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        curl \
        apt-transport-https \
        git \
        libbison-dev \
        flex \
        libhashkit-dev \
        libevent-dev \
        procps \
        lsof \
        gdb \
        unzip \
        musl-tools \
        libunwind8-dev \
        cmake \
        fuse3 \
        libfuse3-dev \
        libnl-3-200\ 
        libnl-3-dev \
        libnl-genl-3-200 \
        libnl-genl-3-dev  \
        libjemalloc2 \
        file \
        lcov \
        bc \
        gcovr && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | \
    gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
    "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt update && \
    apt install -y \
        docker-ce \
        docker-ce-cli \
        containerd.io \
        docker-buildx-plugin \
        docker-compose-plugin
RUN wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazelisk/releases/download/v1.11.0/bazelisk-linux-amd64 && \
    mv bazelisk-linux-amd64 /usr/bin/bazelisk && \
    ln -sf /usr/bin/bazelisk /usr/bin/bazel && \
    wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazel/releases/download/4.2.2/bazel-4.2.2-linux-x86_64 && \
    mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin && \
    mv bazel-4.2.2-linux-x86_64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel && \
    wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazel/releases/download/0.17.2/bazel-0.17.2-linux-x86_64 && \
    mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin && \
    mv bazel-0.17.2-linux-x86_64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin/bazel && \
    chmod +x /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel \
        /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin/bazel \
        /usr/bin/bazel \
        /usr/bin/bazelisk && \
    mkdir -p /usr/local/etcd && \
    curl -L ${DOWNLOAD_ETCD_URL}/${VERSION_ETCD}/etcd-${VERSION_ETCD}-linux-amd64.tar.gz -o /usr/local/etcd-${VERSION_ETCD}-linux-amd64.tar.gz && \
    tar xzvf /usr/local/etcd-${VERSION_ETCD}-linux-amd64.tar.gz -C /usr/local/etcd --strip-components=1 && \
    rm -f /usr/local/etcd-${VERSION_ETCD}-linux-amd64.tar.gz && \
    ln -sf /usr/local/etcd/etcd /usr/bin/etcd && \
    ln -sf /usr/local/etcd/etcdctl /usr/bin/etcdctl && \
    wget http://www.memcached.org/files/memcached-1.6.17.tar.gz && \
    tar xvf memcached-1.6.17.tar.gz && \
    cd memcached-1.6.17/ && \
    ./configure && \
    make && \
    make install && \
    cd / && \
    rm -rf memcached-1.6.17/ memcached-1.6.17.tar.gz && \
    wget https://golang.google.cn/dl/go${GO_INSTALL_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_INSTALL_VERSION}.linux-amd64.tar.gz && \
    rm go${GO_INSTALL_VERSION}.linux-amd64.tar.gz && \
    export PATH=$PATH:/usr/local/go/bin && \
    export GOPATH=/usr/local/go && \
    go env -w GO111MODULE=on &&  \
    go env -w GOPROXY=https://goproxy.cn,direct && \
    wget ${GITHUB_PROXY}https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip "bin/protoc" -d /usr/ && \
    rm protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION} && \
    wget -P /usr/bin https://dl.minio.org.cn/server/minio/release/linux-amd64/minio && \
    chmod +x /usr/bin/minio && \
    mkdir -p /data/minio/ /data/log/curve/ /etc/nebd /etc/curve/ && \
    chmod 0777 /data/log/curve/ /data/minio/ /etc/nebd /etc/curve/

# install clang-format-14
RUN echo "deb http://mirrors.tuna.tsinghua.edu.cn/llvm-apt/bullseye/ llvm-toolchain-bullseye-14 main" > /etc/apt/sources.list.d/llvm.list && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add - && \
    apt-get update && apt-get install clang-format-14 -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

ENV PATH=$PATH:/usr/local/go/bin
