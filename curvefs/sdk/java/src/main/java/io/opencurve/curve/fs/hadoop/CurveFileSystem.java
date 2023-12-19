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
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import io.opencurve.curve.fs.hadoop.permission.Permission;
import io.opencurve.curve.fs.libfs.CurveFsProto;
import io.opencurve.curve.fs.libfs.CurveFsMount;
import io.opencurve.curve.fs.libfs.CurveFsMount.StatVfs;
import io.opencurve.curve.fs.libfs.CurveFsMount.Stat;
import io.opencurve.curve.fs.libfs.CurveFsMount.File;
import io.opencurve.curve.fs.libfs.CurveFsMount.Dirent;
import io.opencurve.curve.fs.libfs.CurveFsException.NotADirectoryException;

public class CurveFileSystem extends FileSystem {
    private static final int O_RDONLY = CurveFsMount.O_RDONLY;
    private static final int O_WRONLY = CurveFsMount.O_WRONLY;
    private static final int O_APPEND = CurveFsMount.O_APPEND;
    private static final int SETATTR_MTIME = CurveFsMount.SETATTR_MTIME;
    private static final int SETATTR_ATIME = CurveFsMount.SETATTR_ATIME;

    private URI uri;
    private Path workingDir;
    private CurveFsProto curvefs = null;
    private Permission permission = null;

    public CurveFileSystem() {}

    public CurveFileSystem(Configuration conf) { setConf(conf); }

    // e.g. /my/dir1
    private String makeAbsolute(Path path) {
        return makeQualified(path).toUri().getPath();
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
        try {
            curvefs.mkdirs(makeAbsolute(f), perms.toShort());
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    private FileStatus newFileStatus(Path f, Stat stat) {
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
     * Return a file status object that represents the path.
     * @param f The path we want information from
     * @return a FileStatus object
     * @throws FileNotFoundException when the path does not exist;
     *         IOException see specific implementation
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        Stat stat = new Stat();
        curvefs.lstat(makeAbsolute(f), stat);
        return newFileStatus(f, stat);
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
        Dirent[] dirents;
        try {
            dirents = curvefs.listdir(makeAbsolute(f));
        } catch(NotADirectoryException e) {
            return new FileStatus[]{ getFileStatus(f) };
        }

        FileStatus[] statuses = new FileStatus[dirents.length];
        for (int i = 0; i < dirents.length; i++) {
            Path p = makeQualified(new Path(f, new String(dirents[i].name)));
            statuses[i] = newFileStatus(p, dirents[i].stat);
        }
        return statuses;
    }

    private FSDataInputStream createFsDataInputStream(File file,
                                                      int bufferSize) throws IOException {
        CurveFsInputStream istream =
            new CurveFsInputStream(getConf(), curvefs, file.fd, file.length, bufferSize);
        return new FSDataInputStream(istream);
    }

    private FSDataOutputStream createFsDataOutputStream(File file,
                                                        int bufferSize) throws IOException {
        OutputStream ostream =
            new CurveFsOutputStream(getConf(), curvefs, file.fd, bufferSize);
        return new FSDataOutputStream(ostream, statistics);
    }

    /**
     * Opens an FSDataInputStream at the indicated Path.
     * @param f the file name to open
     * @param bufferSize the size of the buffer to be used.
     */
    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        File file = new File();
        curvefs.open(makeAbsolute(f), O_RDONLY, file);
        return createFsDataInputStream(file, bufferSize);
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
        File file = new File();
        for ( ; ; ) {
            try {
                curvefs.create(makeAbsolute(f), permission.toShort(), file);
            } catch(FileNotFoundException e) {  // parent directorty not exist
                Path parent = makeQualified(f).getParent();
                try {
                    mkdirs(parent, FsPermission.getDirDefault());
                } catch (FileAlreadyExistsException ignored) {}
                continue;  // create file again
            } catch(FileAlreadyExistsException e)  {
                if (!overwrite || isDirectory(f)) {
                    throw new FileAlreadyExistsException("File already exists: " + f);
                }
                delete(f, false);
                continue;  // create file again
            }
            break;
        }
        return createFsDataOutputStream(file, bufferSize);
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
    public FSDataOutputStream createNonRecursive(Path f,
                                                 FsPermission permission,
                                                 boolean overwrite,
                                                 int bufferSize,
                                                 short replication,
                                                 long blockSize,
                                                 Progressable progress) throws IOException {
        File file = new File();
        for ( ; ; ) {
            try {
                curvefs.create(makeAbsolute(f), permission.toShort(), file);
            } catch(FileAlreadyExistsException e)  {
                if (!overwrite || isDirectory(f)) {
                    throw new FileAlreadyExistsException("File already exists: " + f);
                }
                delete(f, false);
                continue;  // create file again
            }
            break;
        }
        return createFsDataOutputStream(file, bufferSize);
    }

    /**
     * Append to an existing file (optional operation).
     * Same as append(f, getConf().getInt("io.file.buffer.size", 4096), null)
     * @param f the existing file to be appended.
     * @throws IOException
     */
    @Override
    public FSDataOutputStream append(Path f,
                                     int bufferSize,
                                     Progressable progress) throws IOException {
        File file = new File();
        curvefs.open(makeAbsolute(f), O_WRONLY | O_APPEND, file);
        return createFsDataOutputStream(file, bufferSize);
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
        try {
            curvefs.rename(makeAbsolute(src), makeAbsolute(dst));
        } catch(FileNotFoundException e) {  // src path not exist
            return false;
        } catch (FileAlreadyExistsException e) {
            FileStatus status = getFileStatus(dst);
            if (!status.isDirectory()) {
                return false;
            }
            // FIXME:
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
        try {
            if (recursive) {
                curvefs.removeall(makeAbsolute(f));
            } else {
                curvefs.remove(makeAbsolute(f));
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
        StatVfs statvfs = new StatVfs();
        curvefs.statfs(makeAbsolute(p), statvfs);
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
        curvefs.chmod(makeAbsolute(p), permission.toShort());
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
        Stat stat = new Stat();
        curvefs.lstat(makeAbsolute(p), stat);  // TODO(Wine93): remove this operation

        int uid = stat.uid;
        int gid = stat.gid;
        if (username != null) {
            uid = permission.getUid(username);
        }
        if (groupname != null) {
            gid = permission.getGid(groupname);
        }
        curvefs.chown(makeAbsolute(p), uid, gid);
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
        curvefs.setattr(makeAbsolute(p), stat, mask);
    }
}