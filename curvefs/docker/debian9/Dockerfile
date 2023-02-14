FROM opencurvedocker/curve-base:debian9
COPY entrypoint.sh /
COPY curvefs /curvefs
COPY libmemcached.so libmemcached.so.11 libhashkit.so.2 /usr/lib/
RUN mkdir -p /etc/curvefs /core /etc/curve && chmod a+x /entrypoint.sh \
    && cp /curvefs/tools/sbin/curvefs_tool /usr/bin
ENTRYPOINT ["/entrypoint.sh"]
