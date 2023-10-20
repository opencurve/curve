package io.opencurve.curve.fs.flink;

import org.apache.flink.connector.file.table.FileSystemTableFactory;

public class CurveFileSystemTableFactory extends FileSystemTableFactory {
    @Override
    public String factoryIdentifier() {
        return CurveFileSystemFactory.SCHEME;
    }
}
