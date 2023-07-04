package io.opencurve.curve.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class CurveMount {
    // init
    private native long nativeCurveFSCreate();
    private static native int nativeCurveFSMount(long cInstancePtr);
    private static native int nativeCurveFSUmount(long cInstancePtr);
    // dir*
    private static native int nativeCurveFSMkDirs(long cInstancePtr, String path, int mode);
    private static native int nativeCurveFSRmDir(long cInstancePtr, String path);
    private static native String[] nativeCurveFSListDir(long cInstancePtr, String path);
    // file*
    private static native int nativeCurveFSOpen(long cInstancePtr, String path, int flags, int mode);
    private static native long nativeCurveFSLSeek(long cInstancePtr, int fd, long offset, int whence);
    private static native long nativieCurveFSRead(long cInstancePtr, int fd, byte[] buffer, long size, long offset);
    private static native long nativieCurveFSWrite(long cInstancePtr, int fd, byte[] buffer, long size, long offset);
    private static native int nativeCurveFSFSync(long cInstancePtr, int fd);
    private static native int nativeCurveFSClose(long cInstancePtr, int fd);
    private static native int nativeCurveFSUnlink(long cInstancePtr, String path);
    // others
    private static native int nativeCurveFSStatFS(long cInstancePtr, String path, CurveStatVFS statvfs);
    private static native int nativeCurveFSLstat(long cInstancePtr, String path, CurveStat stat);
    private static native int nativeCurveFSFStat(long cInstancePtr, int fd, CurveStat stat);
    private static native int nativeCurveFSSetAttr(long cInstancePtr, String path, CurveStat stat, int mask);
    private static native int nativeCurveFSChmod(long cInstancePtr, String path, int mode);
    private static native int nativeCurveFSRename(long cInstancePtr, String src, String dst);

    public static final int O_RDONLY = 1;
    public static final int O_RDWR = 2;
    public static final int O_APPEND = 4;
    public static final int O_CREAT = 8;
    public static final int O_TRUNC = 16;
    public static final int O_EXCL = 32;
    public static final int O_WRONLY = 64;
    public static final int O_DIRECTORY = 128;

    /*
     * Whence flags for seek().
     *
     * Must be synchronized with JNI if changed.
     */
    public static final int SEEK_SET = 1;
    public static final int SEEK_CUR = 2;
    public static final int SEEK_END = 3;

    /*
     * Attribute flags for setattr().
     *
     * Must be synchronized with JNI if changed.
     */
    public static final int SETATTR_MODE = 1;
    public static final int SETATTR_UID = 2;
    public static final int SETATTR_GID = 4;
    public static final int SETATTR_MTIME = 8;
    public static final int SETATTR_ATIME = 16;

    private long instance_ptr;

    static {
        CurveFSNativeLoader.getInstance().loadLibrary();
    }

    public void mount(String fsname, String fstype, Object option) {
        instance_ptr = nativeCurveFSCreate();
        nativeCurveFSMount(instance_ptr);
    }

    public void unmount() {
       // nativeCurveFSUmount(instance_ptr);
    }

    public void shutdown() throws IOException {
    }

    // directory*
    public void mkdirs(String path, int mode) throws IOException {
        nativeCurveFSMkDirs(instance_ptr, path, mode);
    }

    public void rmdir(String path) throws IOException {
        nativeCurveFSRmDir(instance_ptr, path);
    }

    public String[] listdir(String path) throws IOException {
        return nativeCurveFSListDir(instance_ptr, path);
    }

    // file*
    public int open(String path, int flags, int mode) throws IOException {
        return nativeCurveFSOpen(instance_ptr, path, flags, mode);
    }

    public long lseek(int fd, long offset, int whence) throws IOException {
        return nativeCurveFSLSeek(instance_ptr, fd, offset, whence);
    }

    // FIXME: int -> long
    public int read(int fd, byte[] buf, long size, long offset) throws IOException {
        return (int) nativieCurveFSRead(instance_ptr, fd, buf, size, offset);
    }

    public int write(int fd, byte[] buf, long size, long offset) throws IOException {
        return (int) nativieCurveFSWrite(instance_ptr, fd, buf, size, offset);
    }

    public void fsync(int fd) throws IOException {
        nativeCurveFSFSync(instance_ptr, fd);
    }

    public void close(int fd) throws IOException {
        nativeCurveFSClose(instance_ptr, fd);
    }

    public void unlink(String path) throws IOException {
        nativeCurveFSUnlink(instance_ptr, path);
    }

    // others
    public void statfs(String path, CurveStatVFS statvfs) throws IOException {
        nativeCurveFSStatFS(instance_ptr, path, statvfs);
    }

    public void lstat(String path, CurveStat stat) throws IOException {
        nativeCurveFSLstat(instance_ptr, path, stat);
    }

    public void fstat(int fd, CurveStat stat) throws IOException {
        nativeCurveFSFStat(instance_ptr, fd, stat);
    }

    public void setattr(String path, CurveStat stat, int mask) throws IOException {
        nativeCurveFSSetAttr(instance_ptr, path, stat, mask);
    }

    public void chmod(String path, int mode) throws IOException {
        nativeCurveFSChmod(instance_ptr, path, mode);
    }

    public void rename(String src, String dst) throws IOException {
        nativeCurveFSRename(instance_ptr, src, dst);
    }

    // dummy
    public int get_file_replication(int fd) throws IOException {
        return 1;
    }

    public CurveFileExtent get_file_extent(int fd, long offset) throws IOException{
        return new CurveFileExtent(0, 0, new int[0]);
    }

    public InetAddress get_osd_address(int osd) {
        return new InetSocketAddress("localhost", 9000).getAddress();
    }

    public Bucket[] get_osd_crush_location(int osd) {
        return new Bucket[0];
    }
}
