FROM opencurvedocker/curve-base:debian10
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvebs /etc/curve /etc/nebd /core
COPY curvebs /curvebs
COPY entrypoint.sh /
COPY curvebs/tools/sbin/curve_ops_tool curvebs/nbd/sbin/curve-nbd /usr/bin/
COPY libaws-cpp-sdk-core.so /usr/lib/
COPY libaws-cpp-sdk-s3-crt.so /usr/lib/
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
