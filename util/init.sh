#!/usr/bin/env bash

g_hadoop_prefix="/home/${USER}/.local/hadoop"
g_hadoop_etc="${g_hadoop_prefix}/etc/hadoop/core-site.xml"

# hadoop
mkdir -p "${g_hadoop_prefix}"
sudo mkdir -p "/data/logs/curvefs"
sudo chown ${USER} -R "/data/logs/curvefs"
wget https://curveadm.nos-eastchina1.126.net/T/hadoop-3.3.6.tar.gz -O /tmp/hadoop-3.3.6.tar.gz
tar -zxvf /tmp/hadoop-3.3.6.tar.gz --strip-components=1 -C "${g_hadoop_prefix}"

cat << EOF >> ~/.bashrc
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
export PATH=${g_hadoop_prefix}/bin:\$PATH
export PATH=\$JAVA_HOME/bin:\$PATH
EOF

cat << EOF > "${g_hadoop_etc}"
<configuration>

<property>
<name>fs.curvefs.impl</name>
<value>io.opencurve.curve.fs.hadoop.CurveFileSystem</value>
</property>

<property>
<name>fs.AbstractFileSystem.curvefs.impl</name>
<value>io.opencurve.curve.fs.hadoop.CurveFs</value>
</property>

<property>
<name>curvefs.name</name>
<value>myfs</value>
</property>

<property>
<name>curvefs.diskCache.diskCacheType</name>
<value>0</value>
</property>

<property>
<name>curvefs.s3.ak</name>
<value>xxxx</value>
</property>

<property>
<name>curvefs.s3.sk</name>
<value>xxxx</value>
</property>

<property>
<name>curvefs.s3.endpoint</name>
<value>127.0.0.1:9000</value>
</property>

<property>
<name>curvefs.s3.bucket_name</name>
<value>xxxx</value>
</property>

<property>
<name>curvefs.mdsOpt.rpcRetryOpt.addrs</name>
<value>10.0.0.1:6700,10.0.0.2:6700,10.0.0.3:6700</value>
</property>

<property>
<name>curvefs.fs.accessLogging</name>
<value>true</value>
</property>

<property>
<name>curvefs.vfs.entryCache.lruSize</name>
<value>2000000</value>
</property>

<property>
<name>curvefs.vfs.attrCache.lruSize</name>
<value>2000000</value>
</property>

</configuration>
EOF