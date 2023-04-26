FROM debian:11
ENV TZ=Asia/Shanghai
RUN echo "deb http://mirrors.163.com/debian/ bullseye main non-free contrib\n" \
         "deb http://mirrors.163.com/debian/ bullseye-updates main non-free contrib\n" \
         "deb http://mirrors.163.com/debian/ bullseye-backports main non-free contrib\n" \
         "deb http://mirrors.163.com/debian-security/ stable-security main non-free contrib\n" \
         "deb http://mirrors.aliyun.com/debian/ bullseye main\n" \
         "deb-src http://mirrors.163.com/debian/ bullseye main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ bullseye-updates main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ bullseye-backports main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian-security/ stable-security main non-free contrib\n" \
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
        libsnappy1v5 \
        liblz4-1 \
        zlib1g \
        python \
        wget \
        python3 \
    && wget https://curve-build.nos-eastchina1.126.net/curve-base.tar.gz \
    && tar -xzvf curve-base.tar.gz \
    && mv fusermount3 /usr/local/bin \
    # && mv libetcdclient.so /usr/lib/ \
    # && mv libfuse3.so libfuse3.so.3 libfuse3.so.3.10.5 libnl-3.so.200 libnl-genl-3.so.200 /usr/lib/x86_64-linux-gnu/ \
    # && mv libjemalloc.so libjemalloc.so.1 libjemalloc.so.2 /usr/local/lib/ \
    && rm curve-base.tar.gz \
    && mkdir -p /core \
    && ln -sf /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 /usr/local/lib/libjemalloc.so

#  COPY libetcdclient.so /usr/lib/
