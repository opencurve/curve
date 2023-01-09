FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvebs /etc/curve /etc/nebd /core
COPY curvebs /curvebs
RUN mkdir -p /etc/curve /etc/nebd \
    && chmod a+x /entrypoint.sh \
    && cp curvebs/tools/sbin/curve_ops_tool /usr/bin \
    && cp curvebs/nbd/sbin/curve-nbd /usr/bin/ \
    && cp curvebs/tools-v2/sbin/curve /usr/bin/
ENTRYPOINT ["/entrypoint.sh"]
