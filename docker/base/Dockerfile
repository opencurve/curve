FROM debian:9

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
        libconfig-general-perl \
        libibverbs1 \
        libnl-3-200 \
        libnl-genl-3-200 \
        libnl-route-3-200 \
        librdmacm1 \
        podlators-perl \
        linux-perf \
        vim \
        curl \
        cron \
        procps \
        lsof \
        nginx \
        less \
        tree

COPY libetcdclient.so libjemalloc.so libjemalloc.so.1 libjemalloc.so.2 /usr/lib/
COPY libjemalloc.so libjemalloc.so.1 libjemalloc.so.2 /usr/local/lib/
