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

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class CurveFsMount extends CurveFsProto {
    // init
    private native long nativeCurveFSCreate();
    private native void nativeCurveFSRelease(long instancePtr);
    private static native void nativeCurveFsConfSet(long instancePtr, String key, String value);
    private static native int nativeCurveFsMount(long instancePtr, String fsname, String mountpoint);
    private static native int nativeCurveFsUmount(long instancePtr, String fsname, String mountpoint);
    // dir*
    private static native int nativeCurveFsMkDirs(long instancePtr, String path, int mode);
    private static native int nativeCurveFsRmDir(long instancePtr, String path);
    private static native String[] nativeCurveFsListDir(long instancePtr, String path);
    // file*
    private static native int nativeCurveFsOpen(long instancePtr, String path, int flags, int mode, File file);
    private static native long nativeCurveFsLSeek(long instancePtr, int fd, long offset, int whence);
    private static native long nativieCurveFsRead(long instancePtr, int fd, long offset, byte[] buffer, long size);
    private static native long nativieCurveFsWrite(long instancePtr, int fd, long offset, byte[] buffer, long size);
    private static native int nativeCurveFsFSync(long instancePtr, int fd);
    private static native int nativeCurveFsClose(long instancePtr, int fd);
    private static native int nativeCurveFsUnlink(long instancePtr, String path);
    // others
    private static native int nativeCurveFsStatFs(long instancePtr, StatVfs statvfs);
    private static native int nativeCurveFsLStat(long instancePtr, String path, Stat stat);
    private static native int nativeCurveFsFStat(long instancePtr, int fd, Stat stat);
    private static native int nativeCurveFsSetAttr(long instancePtr, String path, Stat stat, int mask);
    private static native int nativeCurveFsChmod(long instancePtr, String path, int mode);
    private static native int nativeCurveFsChown(long instancePtr, String path, int uid, int gid);
    private static native int nativeCurveFsRename(long instancePtr, String src, String dst);
    private static native int nativeCurveFsRemove(long instancePtr, String path);
    private static native int nativeCurveFsRemoveAll(long instancePtr, String path);

    public static class StatVfs {
        public long bsize;
        public long frsize;
        public long blocks;
        public long bavail;
        public long files;
        public long fsid;
        public long namemax;
    }

    public static class Stat {
        public boolean isFile;
        public boolean isDirectory;
        public boolean isSymlink;

        public int mode;
        public int uid;
        public int gid;
        public long size;
        public long blksize;
        public long blocks;
        public long atime;
        public long mtime;
    }

    public static class File {
        public int fd;
        public long length;
    };

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

    public static final String REGEX_CURVEFS_CONF = "^curvefs\\..*";
    public static final String KEY_FSNAME = "curvefs.name";

    private static long instancePtr;
    private static String fsname;
    private static String mountpoint;
    private static boolean initialized = false;

    static {
        try {
            CurveFsNativeLoader.getInstance().loadLibrary();
        } catch(Exception e) {}
    }

    public CurveFsMount() {
        instancePtr = nativeCurveFSCreate();
    }

    private void setConf(Configuration conf) throws IOException {
        Map<String,String> m = conf.getValByRegex(REGEX_CURVEFS_CONF);
        for (Map.Entry<String,String> entry : m.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equals(KEY_FSNAME)) {
                fsname = value;
                continue;
            }
            nativeCurveFsConfSet(instancePtr, key, value);
        }

        if (null == fsname || fsname.isEmpty()) {
            throw new IOException(KEY_FSNAME + " is not set");
        }
        mountpoint = UUID.randomUUID().toString();
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        if (!initialized) {
            setConf(conf);
            nativeCurveFsMount(instancePtr, fsname, mountpoint);
            initialized = true;
        }
    }

    @Override
    public void shutdown() throws IOException {
        if (initialized) {
            nativeCurveFsUmount(instancePtr, fsname, mountpoint);
            initialized = false;
        }
    }

    @Override
    public void mkdirs(String path, int mode) throws IOException {
        nativeCurveFsMkDirs(instancePtr, path, mode);
    }

    @Override
    public void rmdir(String path) throws IOException {
        nativeCurveFsRmDir(instancePtr, path);
    }

    @Override
    public String[] listdir(String path) throws IOException {
        return nativeCurveFsListDir(instancePtr, path);
    }

    @Override
    public void open(String path, int flags, int mode, File file) throws IOException {
        nativeCurveFsOpen(instancePtr, path, flags, mode, file);
    }

    @Override
    public long lseek(int fd, long offset, int whence) throws IOException {
        return nativeCurveFsLSeek(instancePtr, fd, offset, whence);
    }

    @Override
    public long read(int fd, long offset, byte[] buffer, long size) throws IOException {
        return nativieCurveFsRead(instancePtr, fd, offset, buffer, size);
    }

    @Override
    public long write(int fd, long offset, byte[] buffer, long size) throws IOException {
        return nativieCurveFsWrite(instancePtr, fd, offset, buffer, size);
    }

    @Override
    public void fsync(int fd) throws IOException {
        nativeCurveFsFSync(instancePtr, fd);
    }

    @Override
    public void close(int fd) throws IOException {
        nativeCurveFsClose(instancePtr, fd);
    }

    @Override
    public void unlink(String path) throws IOException {
        nativeCurveFsUnlink(instancePtr, path);
    }

    @Override
    public void statfs(String path, StatVfs statvfs) throws IOException {
        nativeCurveFsStatFs(instancePtr, statvfs);
    }

    @Override
    public void lstat(String path, Stat stat) throws IOException {
        nativeCurveFsLStat(instancePtr, path, stat);
    }

    @Override
    public void fstat(int fd, Stat stat) throws IOException {
        nativeCurveFsFStat(instancePtr, fd, stat);
    }

    @Override
    public void setattr(String path, Stat stat, int mask) throws IOException {
        nativeCurveFsSetAttr(instancePtr, path, stat, mask);
    }

    @Override
    public void chmod(String path, int mode) throws IOException {
        nativeCurveFsChmod(instancePtr, path, mode);
    }

    @Override
    public void chown(String path, int uid, int gid) throws IOException {
        nativeCurveFsChown(instancePtr, path, uid, gid);
    }

    @Override
    public void rename(String src, String dst) throws IOException {
        nativeCurveFsRename(instancePtr, src, dst);
    }

    @Override
    public void remove(String path) throws IOException {
        nativeCurveFsRemove(instancePtr, path);
    }

    @Override
    public void removeall(String path) throws IOException {
        nativeCurveFsRemoveAll(instancePtr, path);
    }
}
