/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-07-07
 * Author: Jingli Chen (Wine93)
 */
package io.opencurve.curve.fs.libfs;

import java.io.IOException;

public class CurveFSMount {
    // init
    private native long nativeCurveFSCreate();
    private native void nativeCurveFSRelease(long instancePtr);
    private static native void nativeCurveFSConfSet(long instancePtr, String key, String value);
    private static native int nativeCurveFSMount(long instancePtr, String fsname);
    private static native int nativeCurveFSUmount(long instancePtr);
    // dir*
    private static native int nativeCurveFSMkDirs(long instancePtr, String path, int mode);
    private static native int nativeCurveFSRmDir(long instancePtr, String path);
    private static native String[] nativeCurveFSListDir(long instancePtr, String path);
    // file*
    private static native int nativeCurveFSOpen(long instancePtr, String path, int flags, int mode);
    private static native long nativeCurveFSLSeek(long instancePtr, int fd, long offset, int whence);
    private static native long nativieCurveFSRead(long instancePtr, int fd, byte[] buffer, long size, long offset);
    private static native long nativieCurveFSWrite(long instancePtr, int fd, byte[] buffer, long size, long offset);
    private static native int nativeCurveFSFSync(long instancePtr, int fd);
    private static native int nativeCurveFSClose(long instancePtr, int fd);
    private static native int nativeCurveFSUnlink(long instancePtr, String path);
    // others
    private static native int nativeCurveFSStatFS(long instancePtr, CurveFSStatVFS statvfs);
    private static native int nativeCurveFSLstat(long instancePtr, String path, CurveFSStat stat);
    private static native int nativeCurveFSFStat(long instancePtr, int fd, CurveFSStat stat);
    private static native int nativeCurveFSSetAttr(long instancePtr, String path, CurveFSStat stat, int mask);
    private static native int nativeCurveFSChmod(long instancePtr, String path, int mode);
    private static native int nativeCurveFSChown(long instancePtr, String path, int uid, int gid);
    private static native int nativeCurveFSRename(long instancePtr, String src, String dst);

    /*
     * Flags for open().
     *
     * Must be synchronized with JNI if changed.
     */
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
    public static final int SEEK_SET = 0;
    public static final int SEEK_CUR = 1;
    public static final int SEEK_END = 2;

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

    private static final String CURVEFS_DEBUG_ENV_VAR = "CURVEFS_DEBUG";
    private static final String CLASS_NAME = "io.opencurve.curve.fs.CurveFSMount";

    private long instancePtr;
    private boolean initialized = false;

    private static void accessLog(String name, String... args) {
        String value = System.getenv(CURVEFS_DEBUG_ENV_VAR);
        if (!Boolean.valueOf(value)) {
            return;
        }

        String params = String.join(",", args);
        String message = String.format("%s.%s(%s)", CLASS_NAME, name, params);
        System.out.println(message);
    }

    static {
        accessLog("loadLibrary");
        try {
            CurveFSNativeLoader.getInstance().loadLibrary();
        } catch(Exception e) {}
    }

    protected void finalize() throws Throwable {
        accessLog("finalize");
        if (initialized) {
            nativeCurveFSUmount(instancePtr);
            nativeCurveFSRelease(instancePtr);
        }
    }

    public CurveFSMount() {
        accessLog("CurveMount");
        instancePtr = nativeCurveFSCreate();
        initialized = true;
    }

    public void confSet(String key, String value) {
        accessLog("confSet", key, value);
        nativeCurveFSConfSet(instancePtr, key, value);
    }

    public void mount(String fsname, String fstype, Object option) {
        accessLog("mount");
        nativeCurveFSMount(instancePtr, fsname);
    }

    public void umount() {
        accessLog("umount");
        nativeCurveFSUmount(instancePtr);
    }

    public void shutdown() throws IOException {
        accessLog("shutdown");
    }

    // directory*
    public void mkdirs(String path, int mode) throws IOException {
        accessLog("mkdirs", path.toString());
        nativeCurveFSMkDirs(instancePtr, path, mode);
    }

    public void rmdir(String path) throws IOException {
        accessLog("rmdir", path.toString());
        nativeCurveFSRmDir(instancePtr, path);
    }

    public String[] listdir(String path) throws IOException {
        accessLog("listdir", path.toString());
        return nativeCurveFSListDir(instancePtr, path);
    }

    // file*
    public int open(String path, int flags, int mode) throws IOException {
        accessLog("open", path.toString());
        return nativeCurveFSOpen(instancePtr, path, flags, mode);
    }

    public long lseek(int fd, long offset, int whence) throws IOException {
        accessLog("lseek", String.valueOf(fd), String.valueOf(offset), String.valueOf(whence));
        return nativeCurveFSLSeek(instancePtr, fd, offset, whence);
    }

    public int read(int fd, byte[] buf, long size, long offset) throws IOException {
        accessLog("read", String.valueOf(fd), String.valueOf(size), String.valueOf(size));
        long rc = nativieCurveFSRead(instancePtr, fd, buf, size, offset);
        return (int) rc;
    }

    public int write(int fd, byte[] buf, long size, long offset) throws IOException {
        accessLog("write", String.valueOf(fd), String.valueOf(size), String.valueOf(size));
        long rc = nativieCurveFSWrite(instancePtr, fd, buf, size, offset);
        return (int) rc;
    }

    public void fsync(int fd) throws IOException {
        accessLog("fsync", String.valueOf(fd));
        nativeCurveFSFSync(instancePtr, fd);
    }

    public void close(int fd) throws IOException {
        accessLog("close", String.valueOf(fd));
        nativeCurveFSClose(instancePtr, fd);
    }

    public void unlink(String path) throws IOException {
        accessLog("unlink", path.toString());
        nativeCurveFSUnlink(instancePtr, path);
    }

    // others
    public void statfs(String path, CurveFSStatVFS statvfs) throws IOException {
        accessLog("statfs", path.toString());
        nativeCurveFSStatFS(instancePtr, statvfs);
    }

    public void lstat(String path, CurveFSStat stat) throws IOException {
        accessLog("lstat", path.toString());
        nativeCurveFSLstat(instancePtr, path, stat);
    }

    public void fstat(int fd, CurveFSStat stat) throws IOException {
        accessLog("fstat", String.valueOf(fd));
        nativeCurveFSFStat(instancePtr, fd, stat);
    }

    public void setattr(String path, CurveFSStat stat, int mask) throws IOException {
        accessLog("setattr", path.toString());
        nativeCurveFSSetAttr(instancePtr, path, stat, mask);
    }

    public void chmod(String path, int mode) throws IOException {
        accessLog("chmod", path.toString());
        nativeCurveFSChmod(instancePtr, path, mode);
    }

    public void chown(String path, int uid, int gid) throws IOException {
        accessLog("chown", path.toString(), String.valueOf(uid), String.valueOf(gid));
        nativeCurveFSChown(instancePtr, path, uid, gid);
    }

    public void rename(String src, String dst) throws IOException {
        accessLog("rename", src.toString(), dst.toString());
        nativeCurveFSRename(instancePtr, src, dst);
    }
}
