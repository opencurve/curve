FROM opencurvedocker/curve-base:debian11
COPY libmemcached.so libmemcached.so.11 libhashkit.so.2 libetcdclient.so /usr/lib/
COPY curvefs /curvefs
RUN mkdir -p /etc/curvefs /core /etc/curve && chmod a+x /entrypoint.sh \
    && cp /curvefs/tools/sbin/curvefs_tool /usr/bin \
    && cp /curvefs/etcd/sbin/etcdctl /usr/bin/ \
    && cp /curvefs/tools-v2/sbin/curve /usr/bin/
