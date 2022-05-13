FROM opencurvedocker/curve-base:debian9

RUN apt-get clean \
    && apt-get -y update \
    && apt-get -y install \
        g++ \
        wget \
        clang \
        git \
        libnl-genl-3-dev \
        libssl-dev \
        uuid-dev \
        zlib1g-dev \
        libcurl4-gnutls-dev \
        libfiu-dev \
        openjdk-8-jdk \
        sudo \
        python-dev \
        python-setuptools \
        python-wheel \
        python3-dev \
        python3-setuptools \
        python3-wheel \
    && wget https://github.com/bazelbuild/bazelisk/releases/download/v1.11.0/bazelisk-linux-amd64 \
    && mv bazelisk-linux-amd64 /usr/bin/bazelisk \
    && ln -s /usr/bin/bazelisk /usr/bin/bazel \
    && wget https://github.com/bazelbuild/bazel/releases/download/4.2.2/bazel-4.2.2-linux-x86_64 \
    && mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin \
    && mv bazel-4.2.2-linux-x86_64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel \
    && chmod +x /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel /usr/bin/bazel /usr/bin/bazelisk
COPY fuse3/* /usr/include/fuse3/
