package io.opencurve.curve.fs.flink;

import io.opencurve.curve.fs.hadoop.CurveFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

public class CurveFileSystemFactory implements FileSystemFactory {
    private org.apache.hadoop.conf.Configuration conf;

    private static final String CURVE_FS_CONFIG_PREFIXES = "curvefs.";
    private static final String FLINK_CONFIG_PREFIXES = "fs.";
    public static String SCHEME = "curvefs";

    @Override
    public void configure(org.apache.flink.configuration.Configuration config) {
        conf = new Configuration();
        if (config != null) {
            for (String key : config.keySet()) {
                if (key.startsWith(CURVE_FS_CONFIG_PREFIXES) || key.startsWith(FLINK_CONFIG_PREFIXES)) {
                    String value = config.getString(key, null);
                    if (value != null) {
                        if (CurveFileSystem.class.getCanonicalName().equals(value.trim())) {
                            SCHEME = key.split("\\.")[1];
                        }
                        conf.set(key, value);
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
        fs.initialize(uri, conf);
        return new HadoopFileSystem(fs);
    }
}
