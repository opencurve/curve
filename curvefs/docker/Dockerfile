FROM opencurvedocker/curve-base:debian9
RUN mkdir -p /usr/local/curvefs
RUN mkdir -p /etc/curvefs
COPY curvefs /usr/local/curvefs
COPY entrypoint.sh /
COPY curvefs/tools/sbin/curvefs_tool /usr/bin
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
