package io.opencurve.curve.fs.flink;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.opencurve.curve.fs.hadoop.CurveFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

public class CurveFileSystemFactory implements FileSystemFactory {
    private org.apache.hadoop.conf.Configuration conf;

    private static final Log LOG = LogFactory.getLog(CurveFileSystemFactory.class);

    private static final String CURVE_FS_CONFIG_PREFIXES = "curvefs.";
    private static final String FLINK_CONFIG_PREFIXES = "fs.";
    public static String SCHEME = "curvefs";

    @Override
    public void configure(org.apache.flink.configuration.Configuration config) {
        conf = new Configuration();

        LOG.info("#### configure");

        if (config != null) {
            for (String key : config.keySet()) {
                if (key.startsWith(CURVE_FS_CONFIG_PREFIXES) || key.startsWith(FLINK_CONFIG_PREFIXES)) {
                    String value = config.getString(key, null);
                    if (value != null) {
                        if (CurveFileSystem.class.getCanonicalName().equals(value.trim())) {
                            SCHEME = key.split("\\.")[1];
                            LOG.info("##### SCHEME KEY=" + key + ",VAL=" + value);
                        }
                        conf.set(key, value);
                        LOG.info("##### ADD KEY=" + key + ",VAL=" + value);
                    }
                }
            }
        }
    }

    @Override
    public String getScheme() {
        return SCHEME;
    }

    @Override
    public FileSystem create(URI uri) throws IOException {
        CurveFileSystem fs = new CurveFileSystem();
        conf.set("curvefs.name", "hadoop-test01");
        conf.set("curvefs.diskCache.diskCacheType", "0");
        conf.set("curvefs.s3.ak", "dd42c79a9e0d4719bc16b790e24f5329");
        conf.set("curvefs.s3.sk", "ee2ddb13eb9e4708843c55eb2d4b5ec7");
        conf.set("curvefs.s3.endpoint", "nos-jd.service.163.org");
        conf.set("curvefs.s3.bucket_name", "curvefs-chuanmei-hadoop-01");
        conf.set("curvefs.mdsOpt.rpcRetryOpt.addrs", "10.166.16.60:6700,10.166.16.61:6700,10.166.16.62:6700");
        conf.set("curvefs.fs.accessLogging", "true");
        conf.set("curvefs.s3.logPrefix", "/tmp");
        conf.set("curvefs.client.common.logDir",  "/tmp");

        LOG.info("#### hardcode for keyset");

        fs.initialize(uri, conf);
        return new HadoopFileSystem(fs);
    }
}
