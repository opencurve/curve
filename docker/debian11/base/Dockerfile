FROM debian:11
ENV TZ=Asia/Shanghai
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
    && wget https://curve-build.nos-eastchina1.126.net/curve-base.tar.gz \
    && tar -xzvf curve-base.tar.gz \
    && mv fusermount3 /usr/local/bin \
    && rm curve-base.tar.gz \
    && mkdir -p /core
