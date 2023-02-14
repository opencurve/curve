FROM opencurvedocker/curve-base:debian9
COPY entrypoint.sh /
COPY curvebs /curvebs
RUN mkdir -p /etc/curve /etc/nebd \
    && chmod a+x /entrypoint.sh \
    && cp curvebs/tools/sbin/curve_ops_tool curvebs/nbd/sbin/curve-nbd \
    && cp curvebs/tools/sbin/curve_ops_tool /usr/bin/
ENTRYPOINT ["/entrypoint.sh"]
