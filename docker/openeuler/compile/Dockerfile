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
    && yum install -y gcc gcc-c++ bison bison-devel python3-devel python3-pip flex flex-devel cmake make wget curl git protobuf-compiler patch libevent-devel musl-gcc\
    && yum install -y unzip which zlib zlib-devel openssl openssl-devel libnl3 libnl3-devel libuuid libuuid-devel libcurl-devel boost boost-devel wget cmake tar  \
    && yum install -y libunwind libunwind-devel
RUN wget https://github.com/bazelbuild/bazel/releases/download/4.2.2/bazel-4.2.2-installer-linux-x86_64.sh && bash bazel-4.2.2-installer-linux-x86_64.sh && rm bazel-4.2.2-installer-linux-x86_64.sh -f && cd /usr/local/ &&  wget https://golang.google.cn/dl/go1.20.6.linux-amd64.tar.gz && tar xzvf go1.20.6.linux-amd64.tar.gz && rm go1.20.6.linux-amd64.tar.gz -rf 
    
RUN wget https://curve-build.nos-eastchina1.126.net/libfiu-1.00.tar.gz && tar -zxvf libfiu-1.00.tar.gz && cd libfiu-1.00 && make libfiu && cp libfiu/*.h /usr/include && cp libfiu/libfiu.so.1.00 /usr/lib && ln -s /usr/lib/libfiu.so.1.00 /usr/lib/libfiu.so && cd .. &&rm -rf libfiu-1.00*
RUN wget https://curve-build.nos-eastchina1.126.net/memcache/memcached-1.6.17.tar.gz \
    && tar xvf memcached-1.6.17.tar.gz && cd memcached-1.6.17/ \
    && ./configure && make -j&& make install \
    && cd .. && rm -rf memcached-1.6.17 memcached-1.6.17.tar.gz \
    && yum install -y fuse3 fuse3-devel lz4-devel snappy-devel


RUN wget https://www.python.org/ftp/python/2.7.18/Python-2.7.18.tgz && tar xvf Python-2.7.18.tgz && \
    cd Python-2.7.18 &&\
    ./configure --prefix=/usr/ && \
    make all -j $(nproc)&&\
    make altinstall &&\
    ln -s /usr/bin/python2.7 /usr/bin/python && \
    ln -s /usr/bin/python2.7 /usr/bin/python2 && \
    python3 -m pip install -i https://pypi.tuna.tsinghua.edu.cn/simple  wheel
RUN wget https://curve-build.nos-eastchina1.126.net/memcache/libmemcached-1.1.2.tar.gz && tar -zxf libmemcached-1.1.2.tar.gz && cd libmemcached-1.1.2 && mkdir build-libmemcached && cd build-libmemcached && cmake .. && make libmemcached -j && make install
RUN export GOPATH=/usr/local/go && export PATH=$PATH:$GOPATH/bin
RUN echo -e "export GOPATH=/usr/local/go\nexport PATH=\$PATH:\$GOPATH/bin" >> ~/.bashrc
ENV GOPATH=/usr/local/go \
    PATH=$PATH:$GOPATH/bin
