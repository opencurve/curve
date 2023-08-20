FROM opencurvedocker/curve-base:build-openeuler AS curve-sdk
ENV GITHUB_PROXY=https://ghproxy.com/ 
RUN echo $PATH

RUN git clone https://github.com/opencurve/curve && \
     cd /curve && git checkout a9db5df70898dad53c76369ed0f732477f3e4c4f  \
     && cp thirdparties/brpc/fix-gcc11.patch / \
     && git checkout a14bcd3ecdfcedd9398cbe74edf17b8173e417a0  \
     && mv /fix-gcc11.patch thirdparties/brpc/fix-gcc11.patch  \
     &&  sed -i "s;//:thirdparties/brpc/brpc.patch;//:thirdparties/brpc/brpc.patch\",\"//:thirdparties/brpc/fix-gcc11.patch;g" WORKSPACE 
RUN cd /curve && sed -i "s;https://gitee.com/mirrors/etcd;https://github.com/etcd-io/etcd;g" thirdparties/etcdclient/Makefile &&   make dep stor=bs
RUN cd /curve && make dep stor=fs 
RUN wget https://bootstrap.pypa.io/pip/2.7/get-pip.py && python2 get-pip.py && python2 -m pip install wheel setuptools
RUN cd /curve &&  bash -x mk-tar.sh
RUN cd /curve &&  mv curve_*.tar.gz curve_sdk.tar.gz
RUN cd /curve && ls ./curvefs_python/tmplib/
FROM opencurvedocker/curve-base:build-openeuler AS curve-tgt
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
    cd / && \
    git clone --branch curve ${GITHUB_PROXY}https://github.com/opencurve/curve-tgt && \
    cd curve-tgt && \
    make programs
RUN cd /
