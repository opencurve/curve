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
    && make libfiu_install \
    && cd .. && rm -rf libfiu-1.00 \
    && ln -s /usr/lib64/libjemalloc.so.2 /usr/local/lib/libjemalloc.so \
    && wget https://www.python.org/ftp/python/2.7.18/Python-2.7.18.tgz && \
    tar xvf Python-2.7.18.tgz && \
    cd Python-2.7.18 &&\
    ./configure --prefix=/usr/ && \
    make all -j &&\
    make altinstall &&\
    ln -s /usr/bin/python2.7 /usr/bin/python && \
    ln -s /usr/bin/python2.7 /usr/bin/python2 \
    && wget https://bootstrap.pypa.io/pip/2.7/get-pip.py && python2 get-pip.py \
    && python2 -m pip install wheel setuptools
ONBUILD COPY libetcdclient.so /usr/lib/
ONBUILD COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
