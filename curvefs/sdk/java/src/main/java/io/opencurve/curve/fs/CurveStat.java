package io.opencurve.curve.fs;

public class CurveStat {

    public boolean is_file;       /* S_ISREG */
    public boolean is_directory;  /* S_ISDIR */
    public boolean is_symlink;    /* S_ISLNK */

    public int mode;
    public int uid;
    public int gid;
    public long size;
    public long blksize;
    public long blocks;
    public long a_time;
    public long m_time;

    public boolean isFile() {
        return is_file;
    }

    public boolean isDir() {
        return is_directory;
    }

    public boolean isSymlink() {
        return is_symlink;
    }
}
