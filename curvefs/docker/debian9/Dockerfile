FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvefs /etc/curvefs /core /etc/curve
COPY curvefs /curvefs
COPY entrypoint.sh /
COPY curvefs/tools/sbin/curvefs_tool /usr/bin
COPY libaws-cpp-sdk-core.so /usr/lib/
COPY libaws-cpp-sdk-s3-crt.so /usr/lib/
COPY libmemcached.so /usr/lib/
COPY libmemcached.so.11 /usr/lib/
COPY libhashkit.so.2 /usr/lib
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
