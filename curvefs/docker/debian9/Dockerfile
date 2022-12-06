FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvefs /etc/curvefs /core /etc/curve
COPY curvefs/client /curvefs/client
COPY curvefs/conf /curvefs/conf
COPY curvefs/etcd /curvefs/etcd
COPY curvefs/mds /curvefs/mds
COPY curvefs/metaserver /curvefs/metaserver
COPY curvefs/monitor /curvefs/monitor
COPY curvefs/tools /curvefs/tools
COPY entrypoint.sh /
COPY curvefs/tools/sbin/curvefs_tool /usr/bin
COPY libmemcached.so libmemcached.so.11 libhashkit.so.2 /usr/lib/
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
