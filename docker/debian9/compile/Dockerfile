FROM debian:9

ENV GITHUB_PROXY=https://ghproxy.com/
ENV ETCD_VER=v3.4.0 \
    DOWNLOAD_ETCD_URL=${GITHUB_PROXY}https://github.com/etcd-io/etcd/releases/download
RUN echo "deb http://mirrors.163.com/debian/ stretch main\n" \
    "deb http://mirrors.163.com/debian/ stretch-updates main non-free contrib\n" \
    "deb-src http://mirrors.163.com/debian/ stretch-updates main non-free contrib\n" \
    "deb http://mirrors.163.com/debian-security/ stretch/updates main non-free contrib\n" \
    "deb http://httpredir.debian.org/debian stretch-backports main contrib non-free\n" \
    > /etc/apt/sources.list \
    && apt-get clean \
    && apt-get -y update \
    && apt-get -y install \
    g++ \
    wget \
    clang \
    git \
    libnl-genl-3-dev \
    libssl-dev \
    uuid-dev \
    libcurl4-gnutls-dev \
    libfiu-dev \
    openjdk-8-jdk \
    sudo \
    python-dev \
    python-setuptools \
    python-wheel \
    python3-dev \
    python3-setuptools \
    python3-wheel \
    zlib1g-dev \
    libsnappy-dev \
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
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
    $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce docker-ce-cli containerd.io \
    && wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazelisk/releases/download/v1.11.0/bazelisk-linux-amd64 \
    && mv bazelisk-linux-amd64 /usr/bin/bazelisk \
    && ln -sf /usr/bin/bazelisk /usr/bin/bazel \
    && wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazel/releases/download/4.2.2/bazel-4.2.2-linux-x86_64 \
    && mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin \
    && mv bazel-4.2.2-linux-x86_64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel \
    && wget ${GITHUB_PROXY}https://github.com/bazelbuild/bazel/releases/download/0.17.2/bazel-0.17.2-linux-x86_64 \
    && mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin \
    && mv bazel-0.17.2-linux-x86_64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin/bazel \
    && chmod +x /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin/bazel /usr/bin/bazel /usr/bin/bazelisk \
    && wget ${GITHUB_PROXY}/https://github.com/Kitware/CMake/releases/download/v3.13.4/cmake-3.13.4-Linux-x86_64.tar.gz \
    && tar Cxzf /usr/share/ cmake-3.13.4-Linux-x86_64.tar.gz \
    && rm cmake-3.13.4-Linux-x86_64.tar.gz \
    && ln -s /usr/share/cmake-3.13.4-Linux-x86_64/bin/cmake /usr/bin/cmake \
    && wget https://curve-build.nos-eastchina1.126.net/fuse3.tar.gz \
    && tar Cxzf /usr/include/ fuse3.tar.gz && rm fuse3.tar.gz \
    && rm -f /usr/local/etcd-${ETCD_VER}-linux-amd64.tar.gz \
    && rm -rf /usr/local/etcd && mkdir -p /usr/local/etcd \
    && curl -L ${DOWNLOAD_ETCD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o /usr/local/etcd-${ETCD_VER}-linux-amd64.tar.gz \
    && tar xzvf /usr/local/etcd-${ETCD_VER}-linux-amd64.tar.gz -C /usr/local/etcd --strip-components=1 \
    && rm -f /usr/local/etcd-${ETCD_VER}-linux-amd64.tar.gz \
    && ln -sf /usr/local/etcd/etcd /usr/bin/etcd && ln -sf /usr/local/etcd/etcdctl /usr/bin/etcdctl \
    && wget https://curve-build.nos-eastchina1.126.net/memcache/memcached-1.6.17.tar.gz \
    && tar xvf memcached-1.6.17.tar.gz && cd memcached-1.6.17/ \
    && ./configure && make && make install \
    && cd / && rm -rf memcached-1.6.17/ memcached-1.6.17.tar.gz \
    && wget https://curve-build.nos-eastchina1.126.net/curve-base.tar.gz \
    && tar -xzf curve-base.tar.gz \
    && mv fusermount3 /usr/local/bin \
    && mv libetcdclient.so /usr/lib/ \
    && mv libfuse3.so libfuse3.so.3 libfuse3.so.3.10.5 libnl-3.so.200 libnl-genl-3.so.200 /usr/lib/x86_64-linux-gnu/ \
    && mv libjemalloc.so libjemalloc.so.1 libjemalloc.so.2 /usr/local/lib/ \
    && rm curve-base.tar.gz
