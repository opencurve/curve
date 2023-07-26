FROM --platform=linux/arm64 opencurve/curve-base-openeuler:latest
COPY entrypoint.sh /
COPY curvefs /curvefs
COPY libmemcached.so libmemcached.so.11 libhashkit.so.2 /usr/lib64/
RUN mkdir -p /etc/curvefs /core /etc/curve && chmod a+x /entrypoint.sh \
    && cp /curvefs/tools/sbin/curvefs_tool /usr/bin \
    && cp curvefs/tools-v2/sbin/curve /usr/bin/
ENV ETCD_UNSUPPORTED_ARCH=arm64 
ENTRYPOINT ["/entrypoint.sh"]
