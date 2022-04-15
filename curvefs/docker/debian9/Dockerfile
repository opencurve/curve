FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvefs /etc/curvefs /core /etc/curve
COPY curvefs /curvefs
COPY entrypoint.sh /
COPY curvefs/tools/sbin/curvefs_tool /usr/bin
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
