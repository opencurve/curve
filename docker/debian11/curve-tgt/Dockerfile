FROM opencurvedocker/curve-base:build-debian11 AS curve-sdk
ENV GITHUB_PROXY=https://ghproxy.com/
RUN git clone ${GITHUB_PROXY}https://github.com/opencurve/curve \
    && cd /curve \
    && bash replace-curve-repo.sh \
    && make dep stor=fs \
    && bash mk-tar.sh \
    && mv curve_*.tar.gz curve_sdk.tar.gz

FROM opencurvedocker/curve-base:build-debian11 AS curve-tgt
ENV GITHUB_PROXY=https://ghproxy.com/
COPY --from=curve-sdk /curve/curve_sdk.tar.gz /
RUN tar -zxvf curve_sdk.tar.gz \
    && rm curve_sdk.tar.gz \
    && cd /curve/curve-sdk \
    && cp -f lib/* /usr/lib \
    && cp -f bin/* /usr/bin \
    && mkdir -p /usr/curvefs \
    && cp -f curvefs/* /usr/curvefs \
    && cp -f include/* /usr/include \
    && ldconfig \
    && apt update \
    && apt install -y devscripts \
    librdmacm-dev \
    libibverbs-dev \
    xsltproc \
    docbook-xsl \
    && cd / \
    && git clone --branch curve ${GITHUB_PROXY}https://github.com/opencurve/curve-tgt \
    && cd curve-tgt \
    && make programs
