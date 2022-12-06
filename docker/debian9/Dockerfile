FROM opencurvedocker/curve-base:debian9
ENV TZ=Asia/Shanghai
RUN mkdir -p /curvebs /etc/curve /etc/nebd /core
COPY curvebs/chunkserver /curvebs/chunkserver
COPY curvebs/conf /curvebs/conf
COPY curvebs/etcd /curvebs/etcd
COPY curvebs/mds /curvebs/mds
COPY curvebs/monitor /curvebs/monitor
COPY curvebs/nbd /curvebs/nbd
COPY curvebs/nebd /curvebs/nebd
COPY curvebs/playground /curvebs/playground
COPY curvebs/snapshotclone /curvebs/snapshotclone
COPY curvebs/tools /curvebs/tools
COPY entrypoint.sh /
COPY curvebs/tools/sbin/curve_ops_tool curvebs/nbd/sbin/curve-nbd /usr/bin/
RUN chmod a+x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
