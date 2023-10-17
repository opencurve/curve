FROM opencurvedocker/curve-base:build-debian11 AS curve-sdk
ENV GITHUB_PROXY=https://ghproxy.com/
RUN git clone ${GITHUB_PROXY}https://github.com/opencurve/curve && \
    cd /curve && \
    git checkout release1.5 && \
    bash replace-curve-repo.sh && \
    bash mk-tar.sh && \
    mv curve_*.tar.gz curve_sdk.tar.gz

FROM opencurvedocker/curve-base:build-debian11 AS curve-spdk-tgt
ENV GITHUB_PROXY=https://ghproxy.com/
COPY --from=curve-sdk /curve/curve_sdk.tar.gz /
RUN tar -zxvf curve_sdk.tar.gz && \
    rm curve_sdk.tar.gz && \
    cd /curve/curve-sdk && \
    cp -f lib/* /usr/lib && \
    cp -f bin/* /usr/bin && \
    mkdir -p /usr/curvefs && \
    cp -f curvefs/* /usr/curvefs && \
    cp -f include/* /usr/include && \
    ldconfig && \
    apt update && \
    apt install -y meson \
        python3-pyelftools \
        uuid-dev \
        libaio-dev \
        libcunit1-dev \
        libblkid-dev \
        libnuma-dev \
        libisal-dev \
        isal \
        libpcap-dev \
        apt-file \
        libbsd-dev \
        pkg-config \
        libssl-dev \
        libncurses5-dev \
        libncursesw5-dev
RUN cd / && \
    git clone ${GITHUB_PROXY}https://github.com/opencurve/dpdk && \
    cd /dpdk && \
    git checkout v22.03 && \
    meson build --prefix=/usr/local/dpdk --libdir=/usr/local/dpdk/lib --buildtype release --optimization=2 && \
    cd build && \
    ninja && \
    meson install
RUN cd / && \
    git clone ${GITHUB_PROXY}https://github.com/opencurve/spdk && \
    cd /spdk && \
    git checkout cbd_for_v22.05.x && \
    sed -i "s;https://github.com;${GITHUB_PROXY}https://github.com;g" .gitmodules && \
    git submodule update --init && \
    ./configure --with-dpdk=/usr/local/dpdk --prefix=/usr/local/spdk --with-cbd --with-ocf --disable-tests && \
    make && \
    make install && \
    cp -r scripts/ /usr/local/spdk && \
    cp -r python/ /usr/local/spdk



