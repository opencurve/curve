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
    && yum install -y jemalloc libevent fuse3 snappy zlib openssl libnl3 libuuid libcurl boost libunwind musl-libc nginx wget make gcc gcc-c++ util-linux\
    && wget https://curve-build.nos-eastchina1.126.net/libfiu-1.00.tar.gz \
    && tar -zxvf libfiu-1.00.tar.gz && cd libfiu-1.00 && make libfiu \
    && cp libfiu/libfiu.so.1.00 /usr/lib && ln -s libfiu.so.1.00 libfiu.so\
    && cp libfiu/*.h /usr/include \
    && cd .. && rm -rf libfiu-1.00 \
    && ln -s /usr/lib64/libjemalloc.so.2 /usr/local/lib/libjemalloc.so
ONBUILD COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
