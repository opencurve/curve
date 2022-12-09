FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvebs /etc/curve /etc/nebd /core
COPY curvebs /curvebs
COPY entrypoint.sh /
COPY curvebs/tools/sbin/curve_ops_tool curvebs/nbd/sbin/curve-nbd /usr/bin/
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
