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
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import io.opencurve.curve.fs.libfs.CurveFSMount;
import io.opencurve.curve.fs.libfs.CurveFSStat;
import io.opencurve.curve.fs.libfs.CurveFSStatVFS;
import io.opencurve.curve.fs.hadoop.permission.Permission;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class CurveFileSystem extends FileSystem {
    private static final Log LOG = LogFactory.getLog(CurveFileSystem.class);

    private URI uri;
    private Path workingDir;
    private CurveFSProto curve = null;
    private Permission perm = null;

    public CurveFileSystem() {}

    public CurveFileSystem(Configuration conf) {
        setConf(conf);
    }

    private Path makeAbsolute(Path path) {
        if (path.isAbsolute()) {
            return path;
        }
        return new Path(workingDir, path);
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public String getScheme() {
        return "hdfs";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);

        if (curve == null) {
            curve = new CurveFSTalker(conf, LOG);
        }
        if (perm == null) {
            perm = new Permission();
        }

        perm.initialize(conf);
        curve.initialize(uri, conf);
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = getHomeDirectory();
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        path = makeAbsolute(path);

        int fd = curve.open(path, CurveFSMount.O_RDONLY, 0);

        CurveFSStat stat = new CurveFSStat();
        curve.fstat(fd, stat);

        CurveFSInputStream istream = new CurveFSInputStream(getConf(), curve, fd, stat.size, bufferSize);
        return new FSDataInputStream(istream);
    }

    @Override
    public void close() throws IOException {
        super.close();
        curve.shutdown();
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
        path = makeAbsolute(path);

        if (progress != null) {
            progress.progress();
        }

        int fd = curve.open(path, CurveFSMount.O_WRONLY | CurveFSMount.O_APPEND, 0);
        if (progress != null) {
            progress.progress();
        }

        CurveFSOutputStream ostream = new CurveFSOutputStream(getConf(), curve, fd, bufferSize);
        return new FSDataOutputStream(ostream, statistics);
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path dir) {
        workingDir = makeAbsolute(dir);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission perms) throws IOException {
        path = makeAbsolute(path);
        curve.mkdirs(path, (int) perms.toShort());
        return true;
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        FsPermission perms = FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf()));;
        return mkdirs(f, perms);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        path = makeAbsolute(path);

        CurveFSStat stat = new CurveFSStat();
        curve.lstat(path, stat);
        String owner = perm.getUsername(stat.uid);;
        String group = perm.getGroupname(stat.gid);;

        FileStatus status = new FileStatus(
            stat.size, stat.isDir(), 1, stat.blksize,
            stat.m_time, stat.a_time,
            new FsPermission((short) stat.mode), owner, group,
            path.makeQualified(this));
        return status;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        path = makeAbsolute(path);

        if (isFile(path)) {
            return new FileStatus[]{getFileStatus(path)};
        }

        String[] dirlist = curve.listdir(path);
        if (dirlist != null) {
            FileStatus[] status = new FileStatus[dirlist.length];
            for (int i = 0; i < status.length; i++) {
                status[i] = getFileStatus(new Path(path, dirlist[i]));
            }
            return status;
        } else {
            throw new FileNotFoundException("File " + path + " does not exist.");
        }
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        path = makeAbsolute(path);
        curve.chmod(path, permission.toShort());
    }

    @Override
    public void setTimes(Path path, long mtime, long atime) throws IOException {
        path = makeAbsolute(path);

        CurveFSStat stat = new CurveFSStat();

        int mask = 0;
        if (mtime != -1) {
            stat.m_time = mtime;
            mask |= CurveFSMount.SETATTR_MTIME;
        }

        if (atime != -1) {
            stat.a_time = atime;
            mask |= CurveFSMount.SETATTR_ATIME;
        }

        curve.setattr(path, stat, mask);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        path = makeAbsolute(path);

        boolean exists = exists(path);

        if (progress != null) {
            progress.progress();
        }

        int flags = CurveFSMount.O_WRONLY | CurveFSMount.O_CREAT;

        if (exists) {
            if (overwrite) {
                flags |= CurveFSMount.O_TRUNC;
            } else {
                throw new FileAlreadyExistsException();
            }
        } else {
            Path parent = path.getParent();
            if (parent != null) {
                if (!mkdirs(parent)) {
                    throw new IOException("mkdirs failed for " + parent.toString());
                }
            }
        }

        if (progress != null) {
            progress.progress();
        }

        int fd = curve.open(path, flags, (int) permission.toShort());

        if (progress != null) {
            progress.progress();
        }

        OutputStream ostream = new CurveFSOutputStream(getConf(), curve, fd, bufferSize);
        return new FSDataOutputStream(ostream, statistics);
    }

    @Override
    public void setOwner(Path path, String username, String groupname) throws IOException {
        CurveFSStat stat = new CurveFSStat();
        curve.lstat(path, stat);

        int uid = stat.uid;
        int gid = stat.gid;
        if (username != null) {
            uid = perm.getUid(username);
        }
        if (groupname != null) {
            gid = perm.getGid(groupname);
        }

        curve.chown(path, uid, gid);
    }

    @Deprecated
    @Override
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
                                                 boolean overwrite,
                                                 int bufferSize, short replication, long blockSize,
                                                 Progressable progress) throws IOException {
        path = makeAbsolute(path);

        Path parent = path.getParent();

        if (parent != null) {
            CurveFSStat stat = new CurveFSStat();
            curve.lstat(parent, stat);
            if (stat.isFile()) {
                throw new FileAlreadyExistsException(parent.toString());
            }
        }

        return this.create(path, permission, overwrite,
                bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        src = makeAbsolute(src);
        dst = makeAbsolute(dst);

        try {
            CurveFSStat stat = new CurveFSStat();
            curve.lstat(dst, stat);
            if (stat.isDir()) {
                return rename(src, new Path(dst, src.getName()));
            }
            return false;
        } catch (FileNotFoundException e) {
        }

        try {
            curve.rename(src, dst);
        } catch (FileNotFoundException e) {
            throw e;
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Deprecated
    @Override
    public boolean delete(Path path) throws IOException {
        return delete(path, false);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        path = makeAbsolute(path);

        FileStatus status;
        try {
            status = getFileStatus(path);
        } catch (FileNotFoundException e) {
            return false;
        }

        if (status.isFile()) {
            curve.unlink(path);
            return true;
        }

        FileStatus[] dirlist = listStatus(path);
        if (dirlist == null) {
            return false;
        }

        if (!recursive && dirlist.length > 0) {
            throw new IOException("Directory " + path.toString() + "is not empty.");
        }

        for (FileStatus fs : dirlist) {
            if (!delete(fs.getPath(), recursive)) {
                return false;
            }
        }

        curve.rmdir(path);
        return true;
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        CurveFSStatVFS stat = new CurveFSStatVFS();
        curve.statfs(p, stat);

        FsStatus status = new FsStatus(stat.bsize * stat.blocks,
                stat.bsize * (stat.blocks - stat.bavail),
                stat.bsize * stat.bavail);
        return status;
    }

    @Override
    public short getDefaultReplication() {
        return 1;
    }

    @Override
    public long getDefaultBlockSize() {
        return super.getDefaultBlockSize();
    }

    @Override
    protected int getDefaultPort() {
        return super.getDefaultPort();
    }

    @Override
    public String getCanonicalServiceName() {
        return null;
    }
}
