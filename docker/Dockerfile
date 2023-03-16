FROM opencurvedocker/curvebs-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvebs /etc/curve /etc/nebd /core /curve/init.d/
COPY curvebs /curvebs
COPY entrypoint.sh /
COPY curvebs/tools/sbin/curve_ops_tool curvebs/nbd/sbin/curve-nbd /usr/bin/
RUN ([ -f /curvebs/curvebs_tgt.deb ] && dpkg -i /curvebs/curvebs_tgt.deb && rm /curvebs/curvebs_tgt.deb) \
    && mv /curvebs/lib /usr/lib/nebd \
    && chmod a+x /entrypoint.sh \
    && apt update && apt install -y kmod
ENTRYPOINT ["/entrypoint.sh"]