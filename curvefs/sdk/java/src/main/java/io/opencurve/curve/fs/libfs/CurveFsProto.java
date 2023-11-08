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
 * Created Date: 2023-11-08
 * Author: Jingli Chen (Wine93)
 */

package io.opencurve.curve.fs.libfs;

import java.net.URI;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import io.opencurve.curve.fs.libfs.CurveFsMount.StatVfs;
import io.opencurve.curve.fs.libfs.CurveFsMount.Stat;
import io.opencurve.curve.fs.libfs.CurveFsMount.File;

public abstract class CurveFsProto {
    public abstract void initialize(URI uri, Configuration conf) throws IOException;
    public abstract void shutdown() throws IOException;

    public abstract void mkdirs(String path, int mode) throws IOException;
    public abstract void rmdir(String path) throws IOException;
    public abstract String[] listdir(String path) throws IOException;

    public abstract void open(String path, int flags, int mode, File file) throws IOException;
    public abstract long lseek(int fd, long offset, int whence) throws IOException;
    public abstract long write(int fd, long offset, byte[] buffer, long size) throws IOException;
    public abstract long read(int fd, long offset, byte[] buffer, long size) throws IOException;
    public abstract void fsync(int fd) throws IOException;
    public abstract void close(int fd) throws IOException;
    public abstract void unlink(String path) throws IOException;

    public abstract void statfs(String path, StatVfs statvfs) throws IOException;
    public abstract void lstat(String path, Stat stat) throws IOException;
    public abstract void fstat(int fd, Stat stat) throws IOException;
    public abstract void setattr(String path, Stat stat, int mask) throws IOException;
    public abstract void chmod(String path, int mode) throws IOException;
    public abstract void chown(String path, int uid, int gid) throws IOException;
    public abstract void rename(String src, String dst) throws IOException;
    public abstract void remove(String path) throws IOException;
    public abstract void removeall(String path) throws IOException;
}
