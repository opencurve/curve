FROM openeuler/openeuler:22.03-lts-sp2
ENV GITHUB_PROXY=https://ghproxy.com/
ENV ETCD_VERSION=v3.4.0 \
    DOWNLOAD_ETCD_URL=${GITHUB_PROXY}https://github.com/etcd-io/etcd/releases/download \
    GOPROXY=https://goproxy.io,direct \
    PROTOC_VERSION=21.8 \
    GO_VERSION=1.19.6 \
    PROTOC_GEN_GO_VERSION=v1.28 \
    PROTOC_GEN_GO_GRPC_VERSION=v1.2
RUN yum update -y\
    && yum install -y gcc gcc-c++ bison bison-devel flex flex-devel cmake make wget curl git protobuf-compiler patch libevent-devel\
    && yum install -y unzip which zlib zlib-devel openssl openssl-devel libnl3 libnl3-devel libuuid libuuid-devel libcurl-devel boost boost-devel wget cmake tar  \
    && yum install -y libunwind libunwind-devel \
    && wget https://github.com/bazelbuild/bazel/releases/download/4.2.2/bazel-4.2.2-installer-linux-x86_64.sh && bash bazel-4.2.2-installer-linux-x86_64.sh \
    && wget https://curve-build.nos-eastchina1.126.net/libfiu-1.00.tar.gz && tar -zxvf libfiu-1.00.tar.gz && cd libfiu-1.00 && make libfiu && make libfiu_install && cd .. && rm -rf libfiu-1.00 \
    && wget https://musl.libc.org/releases/musl-1.2.4.tar.gz && tar xvzf musl-1.2.4.tar.gz && cd musl-1.2.4 && ./configure && make install -j  && cd  .. &&rm -rf musl-1.2.4 musl-1.2.4.tar.gz && echo "export PATH=\$PATH:/usr/local/musl/bin/" >> ~/.bashrc \
    &&wget https://golang.google.cn/dl/go1.20.6.linux-amd64.tar.gz && tar xzvf go1.20.6.linux-amd64.tar.gz && echo "export PATH=\$PATH:/root/go/bin" >> ~/.bashrc \
   && wget https://curve-build.nos-eastchina1.126.net/memcache/memcached-1.6.17.tar.gz \
    && tar xvf memcached-1.6.17.tar.gz && cd memcached-1.6.17/ \
    && ./configure && make -j&& make install \
    && cd .. && rm -rf memcached-1.6.17 memcached-1.6.17.tar.gz \
    && yum install -y fuse3 fuse3-devel lz4-devel snappy-devel\
   && echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/usr/local/lib" >> ~/.bashrc \
   && source ~/.bashrc 