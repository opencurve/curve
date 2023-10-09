FROM opencurvedocker/curve-base:openeuler
COPY libmemcached.so libmemcached.so.11 libhashkit.so.2  libfuse3.so.3.10.5 libsnappy.so.1.1.9  libetcdclient.so /usr/lib/
COPY curvefs /curvefs
RUN mkdir -p /etc/curvefs /core /etc/curve && chmod a+x /entrypoint.sh \
    && cp /curvefs/tools/sbin/curvefs_tool /usr/bin \
    && cp /curvefs/tools-v2/sbin/curve /usr/bin/ \
    && ln -s /usr/lib/libfuse3.so.3.10.5 /usr/lib/libfuse3.so.3 \
    && ln -s /usr/lib64/libsnappy.so.1.1.9 /usr/lib64/libsnappy.so.1 \
    && ldconfig
