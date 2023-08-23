package io.opencurve.curve.fs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class CurveMount {
    // init
    private native long nativeCurveFSCreate();
    private native void nativeCurveFSRelease(long cInstancePtr);
    private static native void nativeCurveFSConfSet(long cInstancePtr, String key, String value);
    private static native int nativeCurveFSMount(long cInstancePtr, String fsname);
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

    private long instance_ptr = 0;

    private boolean initialized = false;

    private static void info(String name, String... args) {
        //System.out.println("io.opencurve.curve.fs.hadoop.CurveMount."
        //    + name + "(" + String.join(",", args) + ")");

    }

    static {
        loadLibrary();
    }

    static synchronized void loadLibrary() {
        info("loadLibrary");
        CurveFSNativeLoader.getInstance().loadLibrary();
    }

    protected void finalize() throws Throwable {
        info("finalize");

        if (initialized) {
            try {
            } catch (Exception e) {}
                nativeCurveFSUmount(instance_ptr);
            try {
                nativeCurveFSRelease(instance_ptr);
            } catch (Exception e) {}
        }
        super.finalize();
    }

    public CurveMount() {
        info("CurveMount");
        instance_ptr = nativeCurveFSCreate();
        initialized = true;
    }

    public void mount(String fsname, String fstype, Object option) {
        info("mount");
        nativeCurveFSMount(instance_ptr, fsname);
    }

    public void umount() {
        info("umount");
        nativeCurveFSUmount(instance_ptr);
    }

    public void shutdown() throws IOException {
        info("shutdown");
    }

    public void conf_set(String key, String value) {
        info("conf_set");
        nativeCurveFSConfSet(instance_ptr, key, value);
    }

    // directory*
    public void mkdirs(String path, int mode) throws IOException {
        info("mkdirs");
        nativeCurveFSMkDirs(instance_ptr, path, mode);
    }

    public void rmdir(String path) throws IOException {
        info("mkdirs");
        nativeCurveFSRmDir(instance_ptr, path);
    }

    public String[] listdir(String path) throws IOException {
        info("listdir", path.toString());
        return nativeCurveFSListDir(instance_ptr, path);
    }

    // file*
    public int open(String path, int flags, int mode) throws IOException {
        info("open", path.toString());
        return nativeCurveFSOpen(instance_ptr, path, flags, mode);
    }

    public long lseek(int fd, long offset, int whence) throws IOException {
        info("lseek");
        return nativeCurveFSLSeek(instance_ptr, fd, offset, whence);
    }

    public int read(int fd, byte[] buf, long size, long offset) throws IOException {
        info("read");
        long rc = nativieCurveFSRead(instance_ptr, fd, buf, size, offset);
        return (int) rc; // FIXME: int -> long
    }

    public int write(int fd, byte[] buf, long size, long offset) throws IOException {
        info("write");
        long rc = nativieCurveFSWrite(instance_ptr, fd, buf, size, offset);
        return (int) rc; // FIXME: int -> long
    }

    public void fsync(int fd) throws IOException {
        info("fsync");
        nativeCurveFSFSync(instance_ptr, fd);
    }

    public void close(int fd) throws IOException {
        info("close", Integer.toString(fd));
        nativeCurveFSClose(instance_ptr, fd);
    }

    public void unlink(String path) throws IOException {
        info("unlink");
        nativeCurveFSUnlink(instance_ptr, path);
    }

    // others
    public void statfs(String path, CurveStatVFS statvfs) throws IOException {
        info("statfs");
        nativeCurveFSStatFS(instance_ptr, path, statvfs);
    }

    public void lstat(String path, CurveStat stat) throws IOException {
        info("lstat", path.toString());
        nativeCurveFSLstat(instance_ptr, path, stat);
    }

    public void fstat(int fd, CurveStat stat) throws IOException {
        info("fstat");
        nativeCurveFSFStat(instance_ptr, fd, stat);
    }

    public void setattr(String path, CurveStat stat, int mask) throws IOException {
        info("setattr");
        nativeCurveFSSetAttr(instance_ptr, path, stat, mask);
    }

    public void chmod(String path, int mode) throws IOException {
        info("chmod");
        nativeCurveFSChmod(instance_ptr, path, mode);
    }

    public void rename(String src, String dst) throws IOException {
        info("rename");
        nativeCurveFSRename(instance_ptr, src, dst);
    }

    // dummy
    public int get_file_replication(int fd) throws IOException {
        info("get_file_replication");
        return 1;
    }

    public CurveFileExtent get_file_extent(int fd, long offset) throws IOException{
        info("get_file_extent");
        return new CurveFileExtent(0, 0, new int[0]);
    }

    public InetAddress get_osd_address(int osd) {
        info("get_osd_address");
        return new InetSocketAddress("localhost", 9000).getAddress();
    }

    public Bucket[] get_osd_crush_location(int osd) {
        info("get_osd_crush_location");
        return new Bucket[0];
    }
}