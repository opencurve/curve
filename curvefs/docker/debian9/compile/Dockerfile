FROM opencurvedocker/curve-base:build-debian9 

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
        zlib1g-dev \
        libsnappy-dev \
        liblz4-dev \
        cmake \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        curl \
        apt-transport-https \
        git \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && echo  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce docker-ce-cli containerd.io \
    && wget https://github.com/bazelbuild/bazelisk/releases/download/v1.11.0/bazelisk-linux-amd64 \
    && mv bazelisk-linux-amd64 /usr/bin/bazelisk \
    && ln -sf /usr/bin/bazelisk /usr/bin/bazel \
    && wget https://github.com/bazelbuild/bazel/releases/download/4.2.2/bazel-4.2.2-linux-x86_64 \
    && mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin \
    && mv bazel-4.2.2-linux-x86_64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel \
    && wget https://github.com/bazelbuild/bazel/releases/download/0.17.2/bazel-0.17.2-linux-x86_64 \
    && mkdir -p /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin \
    && mv bazel-0.17.2-linux-x86_64 /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin/bazel \
    && chmod +x /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel /root/.cache/bazelisk/downloads/bazelbuild/bazel-0.17.2-linux-x86_64/bin/bazel /usr/bin/bazel /usr/bin/bazelisk
COPY fuse3/* /usr/include/fuse3/
