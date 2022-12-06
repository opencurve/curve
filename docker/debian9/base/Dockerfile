FROM opencurvedocker/curve-base:debian9

RUN echo "deb http://mirrors.163.com/debian/ stretch main\n" \
         "deb http://mirrors.163.com/debian/ stretch-updates main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ stretch-updates main non-free contrib\n" \
         "deb http://mirrors.163.com/debian-security/ stretch/updates main non-free contrib\n" \
         "deb http://httpredir.debian.org/debian stretch-backports main contrib non-free\n" \
    > /etc/apt/sources.list \
    && apt-get clean \
    && apt-get -y update \
    && apt-get -y install \
        gcc \
        gdb \
        make \
        openssl \
        net-tools \
        libcurl3-gnutls \
        podlators-perl \
        linux-perf \
        vim \
        curl \
        cron \
        procps \
        lsof \
        nginx \
        less \
        zlib1g \
        libsnappy1v5 \
        liblz4-1 \
        python \
        wget \
        python3 \
        libevent-dev \
    && wget https://curve-build.nos-eastchina1.126.net/memcache/memcached-1.6.17.tar.gz \
    && tar xvf memcached-1.6.17.tar.gz && cd memcached-1.6.17/ \
    && ./configure && make && make install \
    && cd / && rm -rf memcached-1.6.17/ memcached-1.6.17.tar.gz \
    && mkdir -p /memcached/data \
    && wget https://curve-build.nos-eastchina1.126.net/curve-base.tar.gz \
    && tar -xzvf curve-base.tar.gz \
    && mv fusermount3 /usr/local/bin \
    && mv libetcdclient.so /usr/lib/ \
    && mv libfuse3.so libfuse3.so.3 libfuse3.so.3.10.5 libnl-3.so.200 libnl-genl-3.so.200 /usr/lib/x86_64-linux-gnu/ \
    && mv libjemalloc.so libjemalloc.so.1 libjemalloc.so.2 /usr/local/lib/ \
    && rm curve-base.tar.gz
