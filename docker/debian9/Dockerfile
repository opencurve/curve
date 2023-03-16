FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvebs /etc/curve /etc/nebd /core
COPY curvebs /curvebs
<<<<<<< HEAD
COPY entrypoint.sh /
COPY curvebs/tools/sbin/curve_ops_tool curvebs/nbd/sbin/curve-nbd /usr/bin/
RUN chmod a+x /entrypoint.sh
=======
RUN mkdir -p /etc/curve /etc/nebd /curve/init.d/ \
    && chmod a+x /entrypoint.sh \
    && cp curvebs/nbd/sbin/curve-nbd /usr/bin/ \
    && cp curvebs/tools/sbin/curve_ops_tool /usr/bin/ \
    && cp curvebs/tools-v2/sbin/curve /usr/bin/
>>>>>>> ccd55691... [feat]curve: run deamon after start
ENTRYPOINT ["/entrypoint.sh"]
