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
 * Created Date: 2023-08-01
 * Author: NetEase Media Bigdata
 */

package io.opencurve.curve.fs.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import io.opencurve.curve.fs.libfs.CurveFSStat;
import io.opencurve.curve.fs.libfs.CurveFSStatVFS;

import java.io.IOException;
import java.net.URI;

abstract class CurveFSProto {
    // init*
    abstract void initialize(URI uri, Configuration conf) throws IOException;
    abstract void shutdown() throws IOException;
    // directory*
    abstract void mkdirs(Path path, int mode) throws IOException;
    abstract void rmdir(Path path) throws IOException;
    abstract String[] listdir(Path path) throws IOException;
    // file*
    abstract int open(Path path, int flags, int mode) throws IOException;
    abstract long lseek(int fd, long offset, int whence) throws IOException;
    abstract int write(int fd, byte[] buf, long size, long offset) throws IOException;
    abstract int read(int fd, byte[] buf, long size, long offset) throws IOException;
    abstract void fsync(int fd) throws IOException;
    abstract void close(int fd) throws IOException;
    abstract void unlink(Path path) throws IOException;
    // others
    abstract void statfs(Path path, CurveFSStatVFS stat) throws IOException;
    abstract void lstat(Path path, CurveFSStat stat) throws IOException;
    abstract void fstat(int fd, CurveFSStat stat) throws IOException;
    abstract void setattr(Path path, CurveFSStat stat, int mask) throws IOException;
    abstract void chmod(Path path, int mode) throws IOException;
    abstract void chown(Path path, int uid, int gid) throws IOException;
    abstract void rename(Path src, Path dst) throws IOException;
}





