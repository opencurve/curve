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

import java.net.URI;
import java.nio.file.NotDirectoryException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileNotFoundException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import io.opencurve.curve.fs.hadoop.permission.Permission;
import io.opencurve.curve.fs.hadoop.CurveFsInputStream;
import io.opencurve.curve.fs.hadoop.CurveFsOutputStream;
import io.opencurve.curve.fs.libfs.CurveFsProto;
import io.opencurve.curve.fs.libfs.CurveFsMount;
import io.opencurve.curve.fs.libfs.CurveFsMount.StatVfs;
import io.opencurve.curve.fs.libfs.CurveFsMount.Stat;
import io.opencurve.curve.fs.libfs.CurveFsMount.File;
import io.opencurve.curve.fs.libfs.CurveFsException.NotADirectoryException;

public class CurveFileSystem extends FileSystem {
    private static final Log LOG = LogFactory.getLog(CurveFileSystem.class);
    private static final int O_RDONLY = CurveFsMount.O_RDONLY;
    private static final int O_WRONLY = CurveFsMount.O_WRONLY;
    private static final int O_TRUNC = CurveFsMount.O_TRUNC;
    private static final int O_APPEND = CurveFsMount.O_APPEND;
    private static final int O_CREAT = CurveFsMount.O_CREAT;
    private static final int SETATTR_MTIME = CurveFsMount.SETATTR_MTIME;
    private static final int SETATTR_ATIME = CurveFsMount.SETATTR_ATIME;

    private URI uri;
    private Path workingDir;
    private CurveFsProto curvefs = null;
    private Permission permission = null;

    public CurveFileSystem() {}

    public CurveFileSystem(Configuration conf) { setConf(conf); }

    private String absolutePath(Path path) {
        return makeQualified(path).toUri().getPath();
    }

    private int getCreateFlags(Boolean overwrite) {
        int flags = O_WRONLY | O_CREAT;
        if (overwrite) {
            flags |= O_TRUNC;
        }
        return flags;
    }

    private FSDataInputStream createFsDataInputStream(
        File file,
        int bufferSize) throws IOException {
        CurveFsInputStream istream = new CurveFsInputStream(
            getConf(), curvefs, file.fd, file.length, bufferSize);
        return new FSDataInputStream(istream);
    }

    private FSDataOutputStream createFsDataOutputStream(
        int fd,
        int bufferSize) throws IOException {
        OutputStream ostream = new CurveFsOutputStream(
            getConf(), curvefs, fd, bufferSize);
        return new FSDataOutputStream(ostream, statistics);
    }







    /**
     * Get the current working directory for the given FileSystem
     * @return the directory pathname
     */
    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    /**
     * Set the current working directory for the given FileSystem. All relative
     * paths will be resolved relative to it.
     *
     * @param new_dir Path of new working directory
     */
    @Override
    public void setWorkingDirectory(Path new_dir) {
        workingDir = fixRelativePart(new_dir);
        checkPath(workingDir);
    }

    /** Called after a new FileSystem instance is constructed.
     * @param name a uri whose authority section names the host, port, etc.
     *   for this FileSystem
     * @param conf the configuration
     */
    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);

        if (curvefs == null) {
            curvefs = new CurveFsMount();
        }
        if (permission == null) {
            permission = new Permission();
        }

        curvefs.initialize(uri, conf);
        permission.initialize(conf);
        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = getHomeDirectory();
    }

    /**
     * No more filesystem operations are needed.  Will
     * release any held locks.
     */
    @Override
    public void close() throws IOException {
        super.close();
        curvefs.shutdown();
    }

    /**
     * Return the protocol scheme for the FileSystem.
     * <p/>
     * This implementation throws an <code>UnsupportedOperationException</code>.
     *
     * @return the protocol scheme for the FileSystem.
     */
    @Override
    public String getScheme() {
        return "hdfs";
    }

    /** Returns a URI whose scheme and authority identify this FileSystem.*/
    @Override
    public URI getUri() {
        return uri;
    }

    /**
     * Call {@link #mkdirs(Path, FsPermission)} with default permission.
     */
    @Override
    public boolean mkdirs(Path f) throws IOException {
        FsPermission perms = FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf()));
        return mkdirs(f, perms);
    }

    /**
     * Make the given file and all non-existent parents into
     * directories. Has the semantics of Unix 'mkdir -p'.
     * Existence of the directory hierarchy is not an error.
     * @param f path to create
     * @param permission to apply to f
     */
    @Override
    public boolean mkdirs(Path f, FsPermission perms) throws IOException {
        String pat
        try {
            curvefs.mkdirs(absoultePath(f), perms.toShort());
        } catch (IOException e) {
            return false;
        }
        return true;
    }



    /**
     * Return a file status object that represents the path.
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist;
     *         IOException see specific implementation
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        String path = absolutePath(f);
        Stat stat = new Stat();
        curvefs.lstat(path, stat);
        return new FileStatus(stat.size,
            stat.isDirectory,
            1,
            stat.blksize,
            stat.mtime,
            stat.atime,
            new FsPermission((short) stat.mode),
            permission.getUsername(stat.uid),
            permission.getGroupname(stat.gid),
            makeQualified(f));  // e.g. curvefs://my/dir1
    }





    /**
     * List the statuses of the files/directories in the given path if the path is
     * a directory.
     *
     * @param f given path
     * @return the statuses of the files/directories in the given patch
     * @throws FileNotFoundException when the path does not exist;
     *         IOException see specific implementation
     */
    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        String path = absolutePath(f);
        try {
            String[] dirlist = curvefs.listdir(path);
        } catch(NotDirectoryException e) {
            return new FileStatus[]{getFileStatus(f)};
        }


        path = makeAbsolute(path);

        if (isFile(path)) {
            return new FileStatus[]{getFileStatus(path)};
        }

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

    /**
     * Opens an FSDataInputStream at the indicated Path.
     * @param f the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        String path = absolutePath(f);
        File file = new File();
        curvefs.open(path, O_RDONLY, 0, file);
        return createFsDataInputStream(file, bufferSize);
    }

    /**
     * Delete a file
     * @deprecated Use {@link #delete(Path, boolean)} instead.
     */
    @Deprecated
    @Override
    public boolean delete(Path path) throws IOException {
        return delete(path, false);
    }

    /**
     * Create an FSDataOutputStream at the indicated Path with write-progress
     * reporting.
     * @param f the file name to open
     * @param permission
     * @param overwrite if a file with this name already exists, then if true,
     *   the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize
     * @param progress
     * @throws IOException
     * @see #setPermission(Path, FsPermission)
     */
    @Override
    public FSDataOutputStream create(Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) throws IOException {
        String path = absolutePath(f);
        File file = new File();
        int flags = getCreateFlags(overwrite);

        for ( ; ; ) {
            try {
                curvefs.open(path, flags, permission.toShort(), file);
            } catch(FileNotFoundException e) {  // parent directorty not exist
                Path parent = makeQualified(f).getParent();
                try {
                    mkdirs(parent, FsPermission.getDirDefault());
                } catch (FileAlreadyExistsException ignored) {}
                flags = getCreateFlags(false);
                continue;   // create file again
            }
            break;
        }
        return createFsDataOutputStream(file.fd, bufferSize);
    }

    /**
     * Opens an FSDataOutputStream at the indicated Path with write-progress
     * reporting. Same as create(), except fails if parent directory doesn't
     * already exist.
     * @param f the file name to open
     * @param permission
     * @param overwrite if a file with this name already exists, then if true,
     * the file will be overwritten, and if false an error will be thrown.
     * @param bufferSize the size of the buffer to be used.
     * @param replication required block replication for the file.
     * @param blockSize
     * @param progress
     * @throws IOException
     * @see #setPermission(Path, FsPermission)
     * @deprecated API only for 0.20-append
     */
    @Deprecated
    @Override
    public FSDataOutputStream createNonRecursive(
        Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) throws IOException {
        String path = absolutePath(f);
        File file = new File();
        int flags = getCreateFlags(overwrite);
        curvefs.open(path, flags, permission.toShort(), file);
        return createFsDataOutputStream(file.fd, bufferSize);
    }

    /**
     * Append to an existing file (optional operation).
     * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
     * @param f the existing file to be appended.
     * @throws IOException
     */
    @Override
    public FSDataOutputStream append(
        Path f,
        int bufferSize,
        Progressable progress) throws IOException {
        String path = absolutePath(f);
        File file = new File();
        curvefs.open(path, O_WRONLY | O_APPEND, 0, file);
        return createFsDataOutputStream(file.fd, bufferSize);
    }

    /**
     * Renames Path src to Path dst.  Can take place on local fs
     * or remote DFS.
     * @param src path to be renamed
     * @param dst new path after rename
     * @throws IOException on failure
     * @return true if rename is successful
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        String srcPath = absolutePath(src);
        String dstPath = absolutePath(dst);

        // TODO(Wine93): improve the rename performance

        try {
            curvefs.rename(srcPath, dstPath);
        } catch(FileNotFoundException e) {  // src path not exist
            return false;
        } catch (FileAlreadyExistsException e) {
            FileStatus status = getFileStatus(dst);
            if (!status.isDir()) {
                return false;
            }
        }

        return true;


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

    /** Delete a file.
     *
     * @param f the path to delete.
     * @param recursive if path is a directory and set to
     * true, the directory is deleted else throws an exception. In
     * case of a file the recursive can be set to either true or false.
     * @return  true if delete is successful else false.
     * @throws IOException
     */
    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        String path = absolutePath(f);
        try {
            if (recursive) {
                curvefs.removeall(path);
            } else {
                curvefs.remove(path);
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * Returns a status object describing the use and capacity of the
     * file system. If the file system has multiple partitions, the
     * use and capacity of the partition pointed to by the specified
     * path is reflected.
     * @param p Path for which status should be obtained. null means
     * the default partition.
     * @return a FsStatus object
     * @throws IOException
     *           see specific implementation
     */
    @Override
    public FsStatus getStatus(Path p) throws IOException {
        String path = absolutePath(p);
        StatVfs statvfs = new StatVfs();
        curvefs.statfs(path, statvfs);
        return new FsStatus(statvfs.bsize * statvfs.blocks,     // capacity
            statvfs.bsize * (statvfs.blocks - statvfs.bavail),  // used
            statvfs.bsize * statvfs.bavail);                    // remaining
    }

    /**
     * Set permission of a path.
     * @param p
     * @param permission
     */
    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        String path = absolutePath(p);
        curvefs.chmod(path, permission.toShort());
    }

    /**
     * Set owner of a path (i.e. a file or a directory).
     * The parameters username and groupname cannot both be null.
     * @param p The path
     * @param username If it is null, the original username remains unchanged.
     * @param groupname If it is null, the original groupname remains unchanged.
     */
    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
        String path = absolutePath(p);
        Stat stat = new Stat();
        curvefs.lstat(path, stat);  // TODO(Wine93): remove this operation

        int uid = stat.uid;
        int gid = stat.gid;
        if (username != null) {
            uid = permission.getUid(username);
        }
        if (groupname != null) {
            gid = permission.getGid(groupname);
        }
        curvefs.chown(path, uid, gid);
    }

    /**
     * Set access time of a file
     * @param p The path
     * @param mtime Set the modification time of this file.
     *              The number of milliseconds since Jan 1, 1970.
     *              A value of -1 means that this call should not set modification time.
     * @param atime Set the access time of this file.
     *              The number of milliseconds since Jan 1, 1970.
     *              A value of -1 means that this call should not set access time.
     */
    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        String path = absolutePath(p);
        Stat stat = new Stat();
        int mask = 0;
        if (mtime != -1) {
            stat.mtime = mtime;
            mask |= SETATTR_MTIME;
        }
        if (atime != -1) {
            stat.atime = atime;
            mask |= SETATTR_ATIME;
        }
        curvefs.setattr(path, stat, mask);
    }

    @Override
    public short getDefaultReplication() { return 1; }

    @Override
    public long getDefaultBlockSize() { return super.getDefaultBlockSize(); }

    @Override
    public String getCanonicalServiceName() { return null; }
}
