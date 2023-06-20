FROM opencurve/curve-base-openeuler:latest

ENV GITHUB_PROXY=https://ghproxy.com/
ENV ETCD_VERSION=v3.4.0 \
    DOWNLOAD_ETCD_URL=${GITHUB_PROXY}https://github.com/etcd-io/etcd/releases/download \
    GOPROXY=https://goproxy.io,direct \
    PROTOC_VERSION=21.8 \
    GO_VERSION=1.19.6 \
    PROTOC_GEN_GO_VERSION=v1.28 \
    PROTOC_GEN_GO_GRPC_VERSION=v1.2
COPY uuid/* /root/curve_compile_openEuler/uuid/
COPY curve_base_openEuler.tar.gz /root/curve_compile_openEuler/curve_base_openEuler.tar.gz
COPY libfiu-master.zip /root/curve_compile_openEuler/
RUN yum -y update \
    && yum -y install \
    g++ \
    wget \
    clang \
    git \
    libnl3-devel\
    openssl\
    uuid-devel\
    curl\
    libcurl-devel\
    zlib\
    python3\
    snappy\
    flex\
    bison\
    lsof\
    gdb\
    unzip\
    musl-devel\
    libevent-devel\
    ca-certificates\
    gnupg2\
    sudo\
    openeuler-lsb\
    openssl-devel\
    musl-gcc\
    make\
    patch\
    cmake\
    protobuf \
    container-selinux \
    iptables \
    libcgroup \
    device-mapper \
    xz \
    fuse-overlayfs \
    slirp4netns \
    libmemcached-devel \
    && mkdir curve_install_docker \
    && cd curve_install_docker \
    && wget https://download.docker.com/linux/centos/8/aarch64/stable/Packages/containerd.io-1.6.20-3.1.el8.aarch64.rpm \
    && wget https://download.docker.com/linux/centos/8/aarch64/stable/Packages/docker-buildx-plugin-0.10.4-1.el8.aarch64.rpm \
    && wget https://download.docker.com/linux/centos/8/aarch64/stable/Packages/docker-ce-23.0.4-1.el8.aarch64.rpm \
    && wget https://download.docker.com/linux/centos/8/aarch64/stable/Packages/docker-ce-cli-23.0.4-1.el8.aarch64.rpm \
    && wget https://download.docker.com/linux/centos/8/aarch64/stable/Packages/docker-compose-plugin-2.17.2-1.el8.aarch64.rpm \
    && wget https://download.docker.com/linux/centos/8/aarch64/stable/Packages/docker-ce-rootless-extras-23.0.3-1.el8.aarch64.rpm \
    && rpm -ivh containerd.io-1.6.20-3.1.el8.aarch64.rpm \
    && rpm -ivh docker* \
    && cd .. \
    && rm -rf curve_install_docker\
    && wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazelisk/releases/download/v1.11.0/bazelisk-linux-arm64 \
    && mv bazelisk-linux-arm64 /usr/bin/bazelisk \
    && ln -sf /usr/bin/bazelisk /usr/bin/bazel \
    && wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazel/releases/download/4.2.2/bazel-4.2.2-linux-arm64 \
    && mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-arm64/bin \
    && mv bazel-4.2.2-linux-arm64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-arm64/bin/bazel \
    && chmod +x /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-arm64/bin/bazel /usr/bin/bazel /usr/bin/bazelisk \
    && wget ${GITHUB_PROXY}https://github.com/Kitware/CMake/releases/download/v3.26.3/cmake-3.26.3-linux-aarch64.tar.gz \
    && tar Cxzf /usr/share/ cmake-3.26.3-linux-aarch64.tar.gz \
    && rm cmake-3.26.3-linux-aarch64.tar.gz \
    && wget https://curve-build.nos-eastchina1.126.net/fuse3.tar.gz \
    && tar Cxzf /usr/include/ fuse3.tar.gz && rm fuse3.tar.gz \
    && rm -f /usr/local/etcd-${ETCD_VERSION}-linux-arm64.tar.gz \
    && rm -rf /usr/local/etcd && mkdir -p /usr/local/etcd \
    && curl -L ${DOWNLOAD_ETCD_URL}/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-arm64.tar.gz -o /usr/local/etcd-${ETCD_VERSION}-linux-arm64.tar.gz \
    && tar xzvf /usr/local/etcd-${ETCD_VERSION}-linux-arm64.tar.gz -C /usr/local/etcd --strip-components=1 \
    && rm -f /usr/local/etcd-${ETCD_VERSION}-linux-arm64.tar.gz \
    && ln -sf /usr/local/etcd/etcd /usr/bin/etcd && ln -sf /usr/local/etcd/etcdctl /usr/bin/etcdctl \
    && wget https://curve-build.nos-eastchina1.126.net/memcache/memcached-1.6.17.tar.gz \
    && tar xvf memcached-1.6.17.tar.gz && cd memcached-1.6.17/ \
    && ./configure && make && make install \
    && cd / && rm -rf memcached-1.6.17/ memcached-1.6.17.tar.gz \
#    && wget https://curve-build.nos-eastchina1.126.net/curve-base.tar.gz \
    && mv /root/curve_compile_openEuler/curve_base_openEuler.tar.gz ./ \
    && tar -xzvf curve_base_openEuler.tar.gz \
    && mv fusermount3 /usr/local/bin \
    && mv libetcdclient.so /usr/lib64/ \
    && mv libfuse3.so libfuse3.so.3 libfuse3.so.3.10.5 libnl-3.so.200 libnl-genl-3.so.200 /usr/lib64/ \
    && mv libjemalloc.so libjemalloc.so.2 /usr/local/lib/ \
    && rm curve_base_openEuler.tar.gz \
    && wget https://golang.google.cn/dl/go${GO_VERSION}.linux-arm64.tar.gz \
    && tar -C /usr/local -xzf go${GO_VERSION}.linux-arm64.tar.gz \
    && rm go${GO_VERSION}.linux-arm64.tar.gz \
    && export PATH=$PATH:/usr/local/go/bin \
    && echo "export PATH=\$PATH:/usr/local/go/bin:/root/go/bin" >> /root/.bashrc \
    && go env -w GO111MODULE=on && go env -w GOPROXY=https://goproxy.cn,direct \
    && wget ${GITHUB_PROXY}https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-aarch_64.zip \
    && unzip protoc-${PROTOC_VERSION}-linux-aarch_64.zip "bin/protoc" -d /usr/ \
    && rm protoc-${PROTOC_VERSION}-linux-aarch_64.zip \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION} \
    && cp -r /root/curve_compile_openEuler/uuid /usr/include/ \
    && ln -s /usr/lib64/libuuid.so.1 /usr/lib64/libuuid.so \
    && unzip -d ./ /root/curve_compile_openEuler/libfiu-master.zip \
    && cd libfiu-master/ \
    && make install \
    && cd .. \
    && rm -rf libfiu-master 
  
