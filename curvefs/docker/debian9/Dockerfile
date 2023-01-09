FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
COPY entrypoint.sh /
RUN mkdir -p /curvefs /etc/curvefs /core /etc/curve
COPY curvefs /curvefs
RUN mkdir -p /etc/curvefs /core /etc/curve && chmod a+x /entrypoint.sh \
    && cp /curvefs/tools/sbin/curvefs_tool /usr/bin \
    && cp /curvefs/tools-v2/sbin/curve /usr/bin/
ENTRYPOINT ["/entrypoint.sh"]
