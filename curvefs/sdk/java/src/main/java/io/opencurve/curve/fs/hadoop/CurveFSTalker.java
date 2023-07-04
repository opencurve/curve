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

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import io.opencurve.curve.fs.libfs.CurveFSMount;
import io.opencurve.curve.fs.libfs.CurveFSStat;
import io.opencurve.curve.fs.libfs.CurveFSStatVFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

class CurveFSTalker extends CurveFSProto {
    private CurveFSMount mount;
    private String fsname = null;

    private static final String PREFIX_KEY = "curvefs";

    CurveFSTalker(Configuration conf, Log log) {
        mount = null;
    }

    private String tostr(Path path) {
        if (null == path) {
            return "/";
	    }
        return path.toUri().getPath();
    }

    private void load(Configuration conf) {
        Map<String,String> m = conf.getValByRegex("^" + PREFIX_KEY + "\\..*");
        for (Map.Entry<String,String> entry : m.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equals(PREFIX_KEY + ".name")) {
                fsname = value;
            } else {
                mount.confSet(key.substring(PREFIX_KEY.length() + 1), value);
            }
        }
    }

    @Override
    void initialize(URI uri, Configuration conf) throws IOException {
        mount = new CurveFSMount();
        load(conf);
        if (null == fsname || fsname.isEmpty()) {
            throw new IOException("curvefs.name is not set");
        }
        mount.mount(fsname, null, null);
    }

    @Override
    void shutdown() throws IOException {
        if (null != mount) {
            mount.umount();
        }
        mount = null;
    }

    @Override
    void mkdirs(Path path, int mode) throws IOException {
        mount.mkdirs(tostr(path), mode);
    }

    @Override
    void rmdir(Path path) throws IOException {
        mount.rmdir(tostr(path));
    }

    @Override
    String[] listdir(Path path) throws IOException {
        CurveFSStat stat = new CurveFSStat();
        try {
            mount.lstat(tostr(path), stat);
        } catch (FileNotFoundException e) {
            return null;
        }
        if (!stat.isDir()) {
            return null;
        }

        return mount.listdir(tostr(path));
    }

    @Override
    int open(Path path, int flags, int mode) throws IOException {
        return mount.open(tostr(path), flags, mode);
    }

    @Override
    long lseek(int fd, long offset, int whence) throws IOException {
        return mount.lseek(fd, offset, whence);
    }

    @Override
    int write(int fd, byte[] buf, long size, long offset) throws IOException {
        return mount.write(fd, buf, size, offset);
    }

    @Override
    int read(int fd, byte[] buf, long size, long offset) throws IOException {
        return mount.read(fd, buf, size, offset);
    }

    @Override
    void fsync(int fd) throws IOException {
        mount.fsync(fd);
    }

    @Override
    void close(int fd) throws IOException {
        mount.close(fd);
    }

    @Override
    void unlink(Path path) throws IOException {
        mount.unlink(tostr(path));
    }

    @Override
    void statfs(Path path, CurveFSStatVFS stat) throws IOException {
        mount.statfs(tostr(path), stat);
    }

    @Override
    void lstat(Path path, CurveFSStat stat) throws IOException {
        mount.lstat(tostr(path), stat);
    }

    @Override
    void fstat(int fd, CurveFSStat stat) throws IOException {
        mount.fstat(fd, stat);
    }

    @Override
    void setattr(Path path, CurveFSStat stat, int mask) throws IOException {
        mount.setattr(tostr(path), stat, mask);
    }

    @Override
    void chmod(Path path, int mode) throws IOException {
        mount.chmod(tostr(path), mode);
    }

    @Override
    void chown(Path path, int uid, int gid) throws IOException {
        mount.chown(tostr(path), uid, gid);
    }

    @Override
    void rename(Path src, Path dst) throws IOException {
        mount.rename(tostr(src), tostr(dst));
    }
}
