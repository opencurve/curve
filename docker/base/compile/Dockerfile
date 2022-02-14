FROM debian:10

RUN echo "deb http://mirrors.163.com/debian/ buster main non-free contrib\n" \
         "deb http://mirrors.163.com/debian/ buster-updates main non-free contrib\n" \
         "deb http://mirrors.163.com/debian/ buster-backports main non-free contrib\n" \
         "deb http://mirrors.163.com/debian-security/ buster/updates main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ buster main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ buster-updates main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ buster-backports main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian-security/ buster/updates main non-free contrib\n" \
    > /etc/apt/sources.list \
    && apt-get clean \
    && apt-get -y update \
    && apt-get -y install \
        apt-transport-https \
        curl \
        gnupg \
        git \
        gcc \
        g++ \
        clang \
        libssl-dev \
        libnl-genl-3-dev \
        libcurl4-gnutls-dev \
        uuid-dev \
        libfiu-dev \
        libfuse3-dev \
    && curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg \
    && mv bazel.gpg /etc/apt/trusted.gpg.d/ \
    && echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list \
    && apt-get -y update \
    && apt-get -y install \
        bazel-4.2.2 \
    && ln -s /usr/bin/bazel-4.2.2 /usr/bin/bazel
