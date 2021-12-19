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
        podlators-perl \
        linux-perf \
        vim \
        curl \
        cron \
        procps

COPY fusermount3 /usr/local/bin
COPY libetcdclient.so /usr/lib/
COPY libfuse3.so libfuse3.so.3 libfuse3.so.3.10.5 /usr/lib/x86_64-linux-gnu/
COPY libjemalloc.so libjemalloc.so.1 libjemalloc.so.2 /usr/local/lib/
