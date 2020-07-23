FROM centos

WORKDIR /curve-tmp
RUN cp -a /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak && \
    curl -o /etc/yum.repos.d/CentOS-Base.repo https://mirrors.aliyun.com/repo/Centos-8.repo && \
    yum clean all && yum makecache

RUN yum groupinstall -y "Development Tools"
RUN yum install -y unzip which zlib zlib-devel openssl openssl-devel libnl3 libnl3-devel libuuid libuuid-devel libcurl-devel boost boost-devel wget cmake epel-release python2-pip python2-wheel python2-devel && \
    yum install -y libunwind libunwind-devel

# install libfiu
RUN wget https://curve-build.nos-eastchina1.126.net/libfiu-1.00.tar.gz && \
    tar -zxvf libfiu-1.00.tar.gz && \
    cd libfiu-1.00 && make libfiu && make libfiu_install

# install bazel, later using download
RUN wget https://curve-build.nos-eastchina1.126.net/bazel-0.17.2-installer-linux-x86_64.sh && \
    bash bazel-0.17.2-installer-linux-x86_64.sh

WORKDIR /
RUN rm -rf /curve-tmp

