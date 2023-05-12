FROM opencurvedocker/curve-base:debian11
ENV TZ=Asia/Shanghai
COPY --from=opencurvedocker/curve-base:curve-tgt-debian11-release1.5 /curve-tgt/  /curve-tgt/
COPY --from=opencurvedocker/curve-base:curve-tgt-debian11-release1.5 /curve/curve-sdk /curve-tgt/curve-sdk
RUN cd /curve-tgt/curve-sdk && \
    cp -f lib/* /usr/lib && \
    cp -f bin/* /usr/bin && \
    mkdir -p /usr/curvefs && \
    cp -f curvefs/* /usr/curvefs && \
    cp -f include/* /usr/include && \
    ldconfig && \
    cd /curve-tgt/ && \
    make install-programs && \
    rm -rf /curve-tgt
COPY curvebs /curvebs
RUN mkdir -p /etc/curve /etc/nebd /curve/init.d/ && \
    chmod a+x /entrypoint.sh && \
    cp /curvebs/nbd/sbin/curve-nbd /usr/bin/ && \
    cp /curvebs/tools/sbin/curve_ops_tool /usr/bin/ && \
    cp /curvebs/tools-v2/sbin/curve /usr/bin/
