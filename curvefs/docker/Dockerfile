FROM opencurvedocker/curve-base:debian9-rc1
ENV TZ=Asia/Shanghai
RUN mkdir -p /usr/local/curvefs /etc/curvefs
COPY curvefs /usr/local/curvefs
COPY entrypoint.sh /
COPY curvefs/tools/sbin/curvefs_tool /usr/bin
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
