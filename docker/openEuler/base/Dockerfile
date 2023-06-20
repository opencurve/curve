FROM --platform=$BUILDPLATFORM openeuler/openeuler:latest
ENV TZ=Asia/Shanghai
# RUN echo "deb http://mirrors.163.com/debian/ stretch main\n" \
#         "deb http://mirrors.163.com/debian/ stretch-updates main non-free contrib\n" \
#         "deb-src http://mirrors.163.com/debian/ stretch-updates main non-free contrib\n" \
#         "deb http://mirrors.163.com/debian-security/ stretch/updates main non-free contrib\n" \
#         "deb http://httpredir.debian.org/debian stretch-backports main contrib non-free\n" \
#    > /etc/apt/sources.list \
#      yum clean all \

COPY tsinghua.repo /etc/yum.repos.d/tsinghua.repo 
COPY curve_base_openEuler.tar.gz /root/curve_base_openEuler.tar.gz
RUN mv /etc/yum.repos.d/openEuler.repo /etc/yum.repos.d/openEuler.repo.bak \
    && yum -y update \
    && yum -y install \
        gcc \
        gdb \
        make \
        openssl \
        net-tools \
        libcurl \
        perl-podlators \
        perf \
        crontabs \
        zlib-devel \
        snappy-devel \
        lz4-devel\
        vim \
        curl \
        procps \
        lsof \
        nginx \
        less \
        python \
        wget \
        python3 \
        jemalloc-devel \
        util-linux \
#    && wget https://curve-build.nos-eastchina1.126.net/curve-base.tar.gz \
    && mv /root/curve_base_openEuler.tar.gz ./ \
    && tar -xzvf curve_base_openEuler.tar.gz \
    && mv fusermount3 /usr/local/bin \
    && mv libetcdclient.so /usr/lib64/ \
    && mv libfuse3.so libfuse3.so.3 libfuse3.so.3.10.5 libnl-3.so.200 libnl-genl-3.so.200 /usr/lib64/ \
    && mv libjemalloc.so libjemalloc.so.2 /usr/local/lib/ \
    && rm curve_base_openEuler.tar.gz \
    && mkdir -p /core
