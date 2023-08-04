FROM debian:11
ENV TZ=Asia/Shanghai \
    LANG=C.UTF-8     \
    LC_ALL=C.UTF-8
RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|mirrors.ustc.edu.cn/debian-security|g' /etc/apt/sources.list && \
    apt-get clean && \
    apt-get -y update && \
    apt-get -y install \
        gcc \
        gdb \
        make \
        openssl \
        net-tools \
        libcurl3-gnutls \
        perl \
        linux-perf \
        vim \
        curl \
        cron \
        procps \
        lsof \
        nginx \
        less \
        fuse3 \
        libnl-3-200 \
        libnl-genl-3-200 \
        libjemalloc2 \
        libjemalloc-dev \
        libsnappy1v5 \
        liblz4-1 \
        zlib1g \
        python \
        wget \
        python3 \
        kmod && \
    ln -s /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 /usr/local/lib/libjemalloc.so
ONBUILD COPY entrypoint.sh /
ONBUILD COPY libetcdclient.so /usr/lib/
ENTRYPOINT ["/entrypoint.sh"]
