FROM opencurvedocker/curve-base:build-debian11 

RUN echo "deb http://mirrors.163.com/debian/ bullseye main non-free contrib\n" \
         "deb http://mirrors.163.com/debian/ bullseye-updates main non-free contrib\n" \
         "deb http://mirrors.163.com/debian/ bullseye-backports main non-free contrib\n" \
         "deb http://mirrors.163.com/debian-security/ stable-security main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ bullseye main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ bullseye-updates main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian/ bullseye-backports main non-free contrib\n" \
         "deb-src http://mirrors.163.com/debian-security/ stable-security main non-free contrib\n" \
    > /etc/apt/sources.list \
    && apt-get clean \
    && apt-get -y update \
    && apt-get -y install \
        wget \
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
        zlib1g-dev \
        make \
        openjdk-11-jdk \
        sudo \
        libsnappy-dev \
        liblz4-dev \
        cmake \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        curl \
        git \
    && mkdir -p /etc/apt/keyrings \
    # && curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
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
    && chmod +x /root/.cache/bazelisk/downloads/bazelbuild/bazel-4.2.2-linux-x86_64/bin/bazel /usr/bin/bazel /usr/bin/bazelisk
