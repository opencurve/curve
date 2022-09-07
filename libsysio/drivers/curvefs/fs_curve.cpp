#include <stdio.h>
#include <unistd.h>
#include <time.h>

#include <sys/types.h>
#include <sys/queue.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>

extern "C"
{
#include "fs.h"
#include "mount.h"
#include "dev.h"
#include "sysio-cmn.h"
#include "xtio.h"
}


#include "fs_curve.h"

#if 0
static char fuse_dir_template[FUSE_D_RECLEN(1) + FUSE_D_RECLEN(2)];
#endif

/* given pointer to inode, return pointer to fuse_inode  */
#define I2FI(ino) ((struct curvefs_inode *)(ino)->i_private)
#define P2FI(pno) (I2FI(pno->p_base->pb_ino))

/* given pointer to filesys, return pointer to fuse filesys  */
#define FS2FFS(fs) ((struct fuse_filesys *)(fs)->fs_private)

#if 0
struct fuse_filesys{
	LIST_HEAD(, fuse_inode) ffs_finodes; 	/* all i-nodes list */
};
#endif

static int _sysio_curvefs_fsswop_mount(const char *source,
                                       unsigned flags,
                                       const void *data,
                                       struct pnode *tocover,
                                       struct mount **mntp);

static struct fssw_ops curvefs_fssw_ops = {
    _sysio_curvefs_fsswop_mount,
};

static void _sysio_fuse_fsop_gone(struct filesys *fs);

struct filesys_ops fuse_fs_ops = {
    _sysio_fuse_fsop_gone,
};

static void fuse_i_destory(struct curvefs_inode *cino)
{

    LIST_REMOVE(cino, fi_link);
    free(cino);

    return;
}

/* given pointer to filesys, return pointer to fuse filesys  */
#define FS2FFS(fs) ((struct fuse_filesys *)(fs)->fs_private)

static void _sysio_fuse_fsop_gone(struct filesys *fs)
{
    struct fuse_filesys *ffs;
    struct curvefs_inode *cino, *ofino;

    ffs = FS2FFS(fs);
    cino = ffs->ffs_finodes.lh_first;
    while (cino)
    {
        ofino = cino;
        cino = cino->fi_link.le_next;
        fuse_i_destory(ofino);
    }

    /* free FS record */
    free(ffs);

    return;
}

static struct curvefs_inode *fuse_i_alloc(struct fuse_filesys *ffs, fuse_ino_t ino, struct intnl_stat *st)
{

    struct curvefs_inode *cino = NULL;

    cino = (struct curvefs_inode *)malloc(sizeof(struct curvefs_inode));
    if (!cino)
        return NULL;

    cino->_ino = ino;
    cino->_fid.fid_data = &cino->_ino;
    cino->_fid.fid_len = sizeof(cino->_ino);
    cino->fi_st = *st;

    LIST_INSERT_HEAD(&ffs->ffs_finodes, cino, fi_link);

    return cino;
}

static int fuse_inop_lookup(struct pnode *pno, struct inode **inop, struct intent *intnt, const char *path);

static int fuse_inop_getattr(struct pnode *pno, struct intnl_stat *stbuf);

static int fuse_inop_setattr(struct pnode *pno, unsigned mask, struct intnl_stat *stbuf);

static ssize_t fuse_filldirentries(struct pnode *pno, _SYSIO_OFF_T *posp, char *buf, size_t nbytes);

static int fuse_inop_mkdir(struct pnode *pno, mode_t mode);

static int fuse_inop_rmdir(struct pnode *pno);

static int fuse_inop_symlink(struct pnode *pno, const char *data);

static int fuse_inop_readlink(struct pnode *pno, char *buf, size_t bufsiz);

static int fuse_inop_open(struct pnode *pno, int flags, mode_t mode);

static int fuse_inop_close(struct pnode *pno);

static int fuse_inop_link(struct pnode *old, struct pnode *_new);

static int fuse_inop_unlink(struct pnode *pno);

static int fuse_inop_rename(struct pnode *old, struct pnode *_new);

static int fuse_inop_read(struct ioctx *ctx);

static int fuse_inop_write(struct ioctx *ctx);

static _SYSIO_OFF_T fuse_inop_pos(struct pnode *pno, _SYSIO_OFF_T off);

static int fuse_inop_iodone(struct ioctx *ioctx);

static int fuse_inop_fcntl(struct pnode *pno, int cmd, va_list ap, int *rtn);

static int fuse_inop_sync(struct pnode *pno);

static int fuse_inop_datasync(struct pnode *pno);

static int fuse_inop_ioctl(struct pnode *pno, unsigned long int request, va_list ap);

static int fuse_inop_mknod(struct pnode *pno, mode_t mode, dev_t dev);

#ifdef _HAVE_STATVFS
static int fuse_inop_statvfs(struct pnode *pno, struct inode *ino, struct intnl_statvfs *buf);
#endif

static int fuse_inop_ftruncate(struct pnode *pno, size_t size);

static void fuse_inop_gone(struct inode *ino);

struct inode_ops fuse_i_ops = {
    fuse_inop_lookup,
    fuse_inop_getattr,
    fuse_inop_setattr,
    fuse_filldirentries,
    NULL,
    fuse_inop_mkdir,
    fuse_inop_rmdir,
    fuse_inop_symlink,
    fuse_inop_readlink,
    fuse_inop_open,
    fuse_inop_close,
    fuse_inop_link,
    fuse_inop_unlink,
    fuse_inop_rename,
    fuse_inop_read,
    fuse_inop_write,
    fuse_inop_pos,
    fuse_inop_iodone,
    fuse_inop_fcntl,
    NULL,
    fuse_inop_sync,
    NULL,
    fuse_inop_datasync,
    fuse_inop_ioctl,
    fuse_inop_mknod,
#ifdef _HAVE_STATVFS
    fuse_inop_statvfs,
#endif
    _sysio_p_generic_perms_check,
    // fuse_inop_ftruncate,
    fuse_inop_gone};

static ino_t __nxtnum = 1;

static inline ino_t fuse_inum_alloc()
{
    return __nxtnum++;
}

#if 0
/*
 * Initialize this driver.
 */
int _sysio_curvefs_init() {
	return _sysio_fssw_register("curvefs", &curvefs_fssw_ops);
}
#endif

#define CURVEFS_INIT(_crvfs)                            \
    do                                                  \
    {                                                   \
        memset(_crvfs, 0, sizeof(struct fuse_filesys)); \
        LIST_INIT(&((_crvfs)->ffs_finodes));            \
    } while (0)

#define CURVEFS_STAT_INIT(__s, __dev, __inm, __uid, __gid, __mode)              \
    do                                                                          \
    {                                                                           \
        memset(&__s, 0, sizeof(__s));                                           \
        __s.st_dev = __dev;                                                     \
        __s.st_mode = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR | (__mode & 07777); \
        __s.st_nlink = 2;                                                       \
        __s.st_uid = __uid;                                                     \
        __s.st_gid = __gid;                                                     \
        __s.st_size = 0;                                                        \
        __s.st_blksize = FUSE_BLKSIZE;                                          \
        __s.st_blocks = 0;                                                      \
        __s.st_ctime = __s.st_mtime = __s.st_atime = 0;                         \
        __s.st_ino = __inm;                                                     \
    } while (0)

static struct qstr __noname = {NULL, 0, 0};

static int _sysio_curvefs_fsswop_mount(const char *source,
                                       unsigned flags,
                                       const void *data __IS_UNUSED,
                                       struct pnode *tocover,
                                       struct mount **mntp)
{

    dev_t dev;
    ino_t inum;
    int err = 0;
    uid_t uid;
    gid_t gid;
    size_t name_len = 0;
    struct pnode_base *rootpb = NULL;
    struct mount *mnt = NULL;
    struct inode *rooti = NULL;
    struct filesys *fs = NULL;
    struct fuse_filesys *ffs = NULL;
    struct curvefs_inode *cino = NULL;
    struct intnl_stat stat;
    mode_t mode;
    const char *fstype = "curvefs";

    ffs = (struct fuse_filesys *)malloc(sizeof(struct fuse_filesys));
    if (!ffs)
    {
        err = -ENOMEM;
        goto error;
    }

    CURVEFS_INIT(ffs);

    /* create root i-node */
    dev = _sysio_dev_alloc();
    inum = fuse_inum_alloc();
    uid = getuid();
    gid = getgid();
    CURVEFS_STAT_INIT(stat, dev, inum, uid, gid, mode);
    cino = fuse_i_alloc(ffs, inum, &stat);
    if (!cino)
    {
        err = -ENOSPC;
        goto error;
    }

    name_len = strlen(fstype);

#if 0
    fs = _sysio_fs_new(&fuse_fs_ops, 0, fstype, name_len, ffs);
#endif
    fs = _sysio_fs_new(&fuse_fs_ops, 0, ffs);
    if (NULL == fs)
    {
        err = -ENOMEM;
        goto error;
    }

    /* create root inode for system */
    rooti = _sysio_i_new(fs, &cino->_fid, &stat, 1, &fuse_i_ops, cino);
    if (NULL == rooti)
    {
        err = -ENOMEM;
        goto error;
    }

    /* allocate and initialize a new base path node */
    rootpb = _sysio_pb_new(&__noname, NULL, rooti);
    if (NULL == rootpb)
    {
        err = -ENOMEM;
        goto error;
    }

    CURVEFS_DPRINTF("_sysio_curvefs_fsswop_mount come here\n");
    err = _sysio_mounti(fs, rooti, flags, tocover, &mnt);
    if (err)
    {
        goto error;
    }
    *mntp = mnt;

    goto out;

error:
    if (mnt && _sysio_do_unmount(mnt) != 0)
    {
        abort();
    }
    if (rootpb)
    {
        _sysio_pb_gone(rootpb);
        rooti = NULL;
    }

    if (rooti)
    {
        I_RELE(rooti);
    }

    if (fs)
    {
        FS_RELE(fs);
        goto out;
    }

    if (cino)
    {
        fuse_i_destory(cino);
        goto out;
    }

    if (ffs)
    {
        free(ffs);
    }

out:
    return err;
}

/* Init the dirver */
int _sysio_curvefs_init()
{

    struct intnl_dirent *de;
    off_t off;

#if 0
	/* Fill in the dir template */
	de = (struct intnl_dirent *)fuse_dir_template;
#ifdef _DIRENT_HAVE_D_OFF
    de->d_off =
#endif
    off = de->d_reclen = FUSE_D_RECLEN(1);
    de->d_type = FUSE_D_TYPEOF(S_IFDIR);
    de->d_name[0] = '.';
#ifdef _DIRENT_HAVE_D_NAMLEN
    de->d_namlen = 1;
#endif
    /*
    * Move to entry for `..'
    */
    de = (struct intnl_dirent *)((char *)de + off);
    de->d_reclen = FUSE_D_RECLEN(2);
#ifdef _DIRENT_HAVE_D_NAMLEN
    de->d_namlen = 2;
#endif
#ifdef _DIRENT_HAVE_D_OFF
    de->d_off =
#endif
    off += de->d_reclen;
    de->d_type = FUSE_D_TYPEOF(S_IFDIR);
    de->d_name[0] = de->d_name[1] = '.';
    de->d_name[2] = ' ';
#endif

    return _sysio_fssw_register("curvefs", &curvefs_fssw_ops);
}

// send the queue notification
static void sendFuseQueueNotify()
{
    return;
}

static inline stCoRoutine_t *get_co()
{
    return (stCoRoutine_t *)co_self();
}

// TODO inque and to message
static inline void send_fuse_mesg(struct Fuse_Queue_Mesg *msg)
{
    return;
}

static inline fuse_ino_t get_inodeid(struct inode *ino)
{
    return *((fuse_ino_t *)(ino->i_fid->fid_data));
}

// extern void send_curvefs_request(struct Fuse_Queue_Mesg*);
extern void send_curvefs_request(void *msg);

#if 0
	/**
	 * Look up a directory entry by name and get its attributes.
	 *
	 * Valid replies:
	 *   fuse_reply_entry
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param parent inode number of the parent directory
	 * @param name the name to look up
	 */
	void (*lookup) (fuse_req_t req, fuse_ino_t parent, const char *name);
#endif
static int fuse_inop_lookup(struct pnode *pno, struct inode **inop, struct intent *intnt, const char *path)
{

    struct inode *ino = NULL;
    struct filesys *fs = NULL;
    struct fuse_filesys *ffs = NULL;
    struct curvefs_inode *cino = NULL;

    CURVEFS_DPRINTF("into the fuse_inop_lookup \n");

    /* if already have inode just return, maybe a revalidate call */
    if (NULL != *inop)
    {
        cino = I2FI(*inop);
        assert(cino);
        (*inop)->i_stbuf = cino->fi_st;

        return 0;
    }

    struct timeval start, end;
    double time;


    ino = pno->p_parent->p_base->pb_ino;
    fs = ino->i_fs;
    ffs = FS2FFS(fs);

    fuse_ino_t pino = get_inodeid(ino);

    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Lookup_in args_in;
    struct fuse_entry_param args_out;

    args_in._path = pno->p_base->pb_key.pbk_name.name;
    args_in._ino = pino;
    fuse_msg._method = FUSE_LOWLEVEL_LOOKUP;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = &args_out;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    CURVEFS_DPRINTF("into the fuse_inop_lookup ino=%ld path=%s co=%p msg=%p\n", pino, path, fuse_msg.co, &fuse_msg);
    gettimeofday(&start, NULL);

    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    CURVEFS_DPRINTF("fuse_inop_lookup recover from the co_yield ret=%d ino=%ld\n", fuse_msg._ret, args_out.ino);


    if (fuse_msg._ret)
    {
        // return -ENOENT;
        return fuse_msg._ret;
    }

    cino = fuse_i_alloc(ffs, args_out.ino, (struct intnl_stat *)&args_out.attr);
    ino = _sysio_i_new(fs, &cino->_fid, (struct intnl_stat *)&args_out.attr, 1, &fuse_i_ops, cino);

    *inop = ino;

    gettimeofday(&end, NULL);
    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	CURVEFS_DPRINTF("curvefs fuse_inop_lookup cost %lf seconds\n", time);

    return 0;
}

#if 0
	/**
	 * Get file attributes.
	 *
	 * If writeback caching is enabled, the kernel may have a
	 * better idea of a file's length than the FUSE file system
	 * (eg if there has been a write that extended the file size,
	 * but that has not yet been passed to the filesystem.n
	 *
	 * In this case, the st_size value provided by the file system
	 * will be ignored.
	 *
	 * Valid replies:
	 *   fuse_reply_attr
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param ino the inode number
	 * @param fi for future use, currently always NULL
	 */

	void (*getattr) (fuse_req_t req, fuse_ino_t ino,
			 struct fuse_file_info *fi);
#endif

static int fuse_inop_getattr(struct pnode *pno, struct intnl_stat *stbuf)
{

    struct inode *ino = NULL;
    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Getattr_in args_in;
    struct stat args_out;

    ino = pno->p_base->pb_ino;
    fuse_ino_t fino = get_inodeid(ino);

    args_in._ino = fino;
    args_in._finfo = &(((struct curvefs_inode *)(pno->p_base->pb_ino)->i_private)->fi_file);
    fuse_msg._method = FUSE_LOWLEVEL_GETATTR;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = &args_out;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    if (fuse_msg._ret)
    {
        return fuse_msg._ret;
    }

    memcpy(stbuf, (const void *)(&args_out), sizeof(struct stat));

    return 0;
}

#if 0
	/**
	 * Set file attributes
	 *
	 * In the 'attr' argument only members indicated by the 'to_set'
	 * bitmask contain valid values.  Other members contain undefined
	 * values.
	 *
	 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
	 * expected to reset the setuid and setgid bits if the file
	 * size or owner is being changed.
	 *
	 * If the setattr was invoked from the ftruncate() system call
	 * under Linux kernel versions 2.6.15 or later, the fi->fh will
	 * contain the value set by the open method or will be undefined
	 * if the open method didn't set any value.  Otherwise (not
	 * ftruncate call, or kernel version earlier than 2.6.15) the fi
	 * parameter will be NULL.
	 *
	 * Valid replies:
	 *   fuse_reply_attr
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param ino the inode number
	 * @param attr the attributes
	 * @param to_set bit mask of attributes which should be set
	 * @param fi file information, or NULL
	 */
	void (*setattr) (fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			 int to_set, struct fuse_file_info *fi);
#endif

static int fuse_inop_setattr(struct pnode *pno, unsigned mask, struct intnl_stat *stbuf)
{
    struct inode *ino = NULL;
    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Setattr_in args_in;
    struct stat args_out;

    CURVEFS_DPRINTF("now coming into fuse_inop_setattr pno=%p mask=%d stbuf=%p\n", pno, mask, stbuf);

    ino = pno->p_base->pb_ino;
    fuse_ino_t fino = get_inodeid(ino);

    args_in._ino = fino;
    args_in._mask = mask;
    args_in._stat = (struct stat *)stbuf;
    args_in._finfo = &(((struct curvefs_inode *)(pno->p_base->pb_ino)->i_private)->fi_file);
    fuse_msg._method = FUSE_LOWLEVEL_SETATTR;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = &args_out;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    if (fuse_msg._ret)
    {
        return fuse_msg._ret;
    }

    memcpy(stbuf, (const void *)(&args_out), sizeof(struct stat));

    return 0;
}

static ssize_t fuse_inop_filldirentries(struct inode *ino, _SYSIO_OFF_T *posp, char *buf, size_t nbytes);

static int fuse_inop_mkdir(struct pnode *pno, mode_t mode)
{
    return 0;
}

static int fuse_inop_rmdir(struct pnode *pno)
{
    return 0;
}

static int fuse_inop_symlink(struct pnode *pno, const char *data)
{
    return 0;
}

static int fuse_inop_readlink(struct pnode *pno, char *buf, size_t bufsiz)
{
    return 0;
}

#if 0
	/**
	 * Open a file
	 *
	 * Open flags are available in fi->flags. The following rules
	 * apply.
	 *
	 *  - Creation (O_CREAT, O_EXCL, O_NOCTTY) flags will be
	 *    filtered out / handled by the kernel.
	 *
	 *  - Access modes (O_RDONLY, O_WRONLY, O_RDWR) should be used
	 *    by the filesystem to check if the operation is
	 *    permitted.  If the ``-o default_permissions`` mount
	 *    option is given, this check is already done by the
	 *    kernel before calling open() and may thus be omitted by
	 *    the filesystem.
	 *
	 *  - When writeback caching is enabled, the kernel may send
	 *    read requests even for files opened with O_WRONLY. The
	 *    filesystem should be prepared to handle this.
	 *
	 *  - When writeback caching is disabled, the filesystem is
	 *    expected to properly handle the O_APPEND flag and ensure
	 *    that each write is appending to the end of the file.
	 * 
         *  - When writeback caching is enabled, the kernel will
	 *    handle O_APPEND. However, unless all changes to the file
	 *    come through the kernel this will not work reliably. The
	 *    filesystem should thus either ignore the O_APPEND flag
	 *    (and let the kernel handle it), or return an error
	 *    (indicating that reliably O_APPEND is not available).
	 *
	 * Filesystem may store an arbitrary file handle (pointer,
	 * index, etc) in fi->fh, and use this in other all other file
	 * operations (read, write, flush, release, fsync).
	 *
	 * Filesystem may also implement stateless file I/O and not store
	 * anything in fi->fh.
	 *
	 * There are also some flags (direct_io, keep_cache) which the
	 * filesystem may set in fi, to change the way the file is opened.
	 * See fuse_file_info structure in <fuse_common.h> for more details.
	 *
	 * If this request is answered with an error code of ENOSYS
	 * and FUSE_CAP_NO_OPEN_SUPPORT is set in
	 * `fuse_conn_info.capable`, this is treated as success and
	 * future calls to open and release will also succeed without being
	 * sent to the filesystem process.
	 *
	 * Valid replies:
	 *   fuse_reply_open
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param ino the inode number
	 * @param fi file information
	 */
	void (*open) (fuse_req_t req, fuse_ino_t ino,
		      struct fuse_file_info *fi);
#endif

static int fuse_inop_open(struct pnode *pno, int flags, mode_t mode)
{
    struct inode *ino = NULL;
    fuse_ino_t fino;
    int rv;

    ino = pno->p_base->pb_ino;

    CURVEFS_DPRINTF("-------------- in fuse_inop_open ino=%p flags=%d mode=%d\n", ino, flags, mode);

    struct timeval start, end;
    double time;

    gettimeofday(&start, NULL);

    /* file does not exists */
    if (NULL == ino)
    {
        if (flags & O_CREAT)
        {
            CURVEFS_DPRINTF("fuse_inop_open mknod\n");
            rv = PNOP_MKNOD(pno, mode, 0);
            if (0 != rv)
            {
                return rv;
            }
            ino = pno->p_base->pb_ino;
        }
    }

    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Open_in args_in;
    struct Args_Open_out args_out;

    fuse_msg._method = FUSE_LOWLEVEL_OPEN;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = &args_out;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    args_in._ino = get_inodeid(ino);
    args_in._info.flags = flags & ~(O_CREAT | O_EXCL | O_NOCTTY);

    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    if (0 != fuse_msg._ret)
    {
        return fuse_msg._ret;
    }

    ((struct curvefs_inode *)(pno->p_base->pb_ino)->i_private)->fi_file.fh = args_out._fout.fh;
    ((struct curvefs_inode *)(pno->p_base->pb_ino)->i_private)->fi_file.flags = args_out._fout.open_flags;

	gettimeofday(&end, NULL);
    time = end.tv_sec + (end.tv_usec/1000000.0) - start.tv_sec - (start.tv_usec/1000000.0);
	CURVEFS_DPRINTF("curvefs fuse_inop_open cost %lf seconds\n", time);

    return 0;
}

#if 0
	/**
	 * Release an open file
	 *
	 * Release is called when there are no more references to an open
	 * file: all file descriptors are closed and all memory mappings
	 * are unmapped.
	 *
	 * For every open call there will be exactly one release call (unless
	 * the filesystem is force-unmounted).
	 *
	 * The filesystem may reply with an error, but error values are
	 * not returned to close() or munmap() which triggered the
	 * release.
	 *
	 * fi->fh will contain the value set by the open method, or will
	 * be undefined if the open method didn't set any value.
	 * fi->flags will contain the same flags as for open.
	 *
	 * Valid replies:
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param ino the inode number
	 * @param fi file information
	 */
	void (*release) (fuse_req_t req, fuse_ino_t ino,
			 struct fuse_file_info *fi);
#endif

static int fuse_inop_close(struct pnode *pno)
{
    struct inode *ino = NULL;
    fuse_ino_t fino;
    int rv;

    ino = pno->p_base->pb_ino;

    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Close_in args_in;

    fuse_msg._method = FUSE_LOWLEVEL_RELEASE;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = NULL;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    args_in._ino = get_inodeid(ino);
    args_in._finfo = &(((struct curvefs_inode *)(pno->p_base->pb_ino)->i_private)->fi_file);

    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    return fuse_msg._ret;
}

static int fuse_inop_link(struct pnode *old, struct pnode *_new)
{
    return 0;
}

static int fuse_inop_unlink(struct pnode *pno)
{
    return 0;
}

static int fuse_inop_rename(struct pnode *old, struct pnode *_new)
{
    return 0;
}

#if 0
	/**
	 * Read data
	 *
	 * Read should send exactly the number of bytes requested except
	 * on EOF or error, otherwise the rest of the data will be
	 * substituted with zeroes.  An exception to this is when the file
	 * has been opened in 'direct_io' mode, in which case the return
	 * value of the read system call will reflect the return value of
	 * this operation.
	 *
	 * fi->fh will contain the value set by the open method, or will
	 * be undefined if the open method didn't set any value.
	 *
	 * Valid replies:
	 *   fuse_reply_buf
	 *   fuse_reply_iov
	 *   fuse_reply_data
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param ino the inode number
	 * @param size number of bytes to read
	 * @param off offset to read from
	 * @param fi file information
	 */
	void (*read) (fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		      struct fuse_file_info *fi);
#endif

static int fuse_inop_read(struct ioctx *ctx)
{
    struct inode *ino = NULL;
    fuse_ino_t fino;
    int rv;

    CURVEFS_DPRINTF("coming into fuse_inop_readio count=%ld xtv count=%ld\n", ctx->ioctx_iovlen, ctx->ioctx_xtvlen);

    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Read_in args_in;
    struct Args_Read_out args_out;

    fuse_msg._method = FUSE_LOWLEVEL_READ;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = &args_out;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    ino = ctx->ioctx_pno->p_base->pb_ino;
    args_in._ino = get_inodeid(ino);
    args_in._finfo = &(((struct curvefs_inode *)(ino)->i_private)->fi_file);
    args_in._size = ctx->ioctx_iov->iov_len;
    args_in._off = ctx->ioctx_xtv->xtv_off;

    args_out._buf = (char *)ctx->ioctx_iov->iov_base;
    args_out._size = ctx->ioctx_iov->iov_len;

    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    if (0 == fuse_msg._ret) {
        ctx->ioctx_cc = args_out._size;
    } else {
        ctx->ioctx_cc = -1;
        ctx->ioctx_errno = -fuse_msg._ret;
    }
#if 0
    if (fuse_msg._ret)
    {
        return fuse_msg._ret;
    }

    return args_out._size;
#endif

    return 0;
}

#if 0
	/**
	 * Write data
	 *
	 * Write should return exactly the number of bytes requested
	 * except on error.  An exception to this is when the file has
	 * been opened in 'direct_io' mode, in which case the return value
	 * of the write system call will reflect the return value of this
	 * operation.
	 *
	 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
	 * expected to reset the setuid and setgid bits.
	 *
	 * fi->fh will contain the value set by the open method, or will
	 * be undefined if the open method didn't set any value.
	 *
	 * Valid replies:
	 *   fuse_reply_write
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param ino the inode number
	 * @param buf data to write
	 * @param size number of bytes to write
	 * @param off offset to write to
	 * @param fi file information
	 */
	void (*write) (fuse_req_t req, fuse_ino_t ino, const char *buf,
		       size_t size, off_t off, struct fuse_file_info *fi);
#endif

static int fuse_inop_write(struct ioctx *ctx)
{
    struct inode *ino = NULL;
    fuse_ino_t fino;
    int rv;

    struct timeval start, end;
    double time;

    gettimeofday(&start, NULL);

    CURVEFS_DPRINTF("coming into fuse_inop_write io count=%ld xtv count=%ld\n", ctx->ioctx_iovlen, ctx->ioctx_xtvlen);

    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Write_in args_in;
    struct Args_Write_out args_out;

    fuse_msg._method = FUSE_LOWLEVEL_WRITE;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = &args_out;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    ino = ctx->ioctx_pno->p_base->pb_ino;
    args_in._ino = get_inodeid(ino);
    args_in._finfo = &(((struct curvefs_inode *)(ino)->i_private)->fi_file);
    args_in._buf = (char *)ctx->ioctx_iov->iov_base;
    args_in._size = ctx->ioctx_iov->iov_len;
    args_in._off = ctx->ioctx_xtv->xtv_off;
    
    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    gettimeofday(&end, NULL);
    time = end.tv_sec + (end.tv_usec / 1000000.0) - start.tv_sec - (start.tv_usec / 1000000.0);
    CURVEFS_DPRINTF ("curvefs fuse_inop_write cost %lf seconds ret=%d size=%d err=%d\n", time, fuse_msg._ret, args_out._size, ctx->ioctx_errno);

    if (0 == fuse_msg._ret) {
        ctx->ioctx_cc = args_out._size;
    } else {
        ctx->ioctx_cc = -1;
        ctx->ioctx_errno = -fuse_msg._ret;
    }

#if 0
    if (fuse_msg._ret)
    {
        return fuse_msg._ret;
    }

    return args_out._size;
#endif

    return 0;
}

static _SYSIO_OFF_T fuse_inop_pos(struct pnode *pno, _SYSIO_OFF_T off)
{
    return 0;
}

static int fuse_inop_iodone(struct ioctx *ioctx)
{
    /* just use to indicate finish iodone */
    return 1;
}

static int fuse_inop_fcntl(struct pnode *pno, int cmd, va_list ap, int *rtn)
{
    return 0;
}

static int fuse_inop_sync(struct pnode *pno)
{
    return 0;
}

static int fuse_inop_datasync(struct pnode *pno)
{
    return 0;
}

static int fuse_inop_ioctl(struct pnode *pno, unsigned long int request, va_list ap)
{
    CURVEFS_DPRINTF("coming into fuse_inop_ioctl\n");
    return 0;
}

#if 0
	/**
	 * Create file node
	 *
	 * Create a regular file, character device, block device, fifo or
	 * socket node.
	 *
	 * Valid replies:
	 *   fuse_reply_entry
	 *   fuse_reply_err
	 *
	 * @param req request handle
	 * @param parent inode number of the parent directory
	 * @param name to create
	 * @param mode file type and mode with which to create the new file
	 * @param rdev the device number (only valid if created file is a device)
	 */
	void (*mknod) (fuse_req_t req, fuse_ino_t parent, const char *name,
		       mode_t mode, dev_t rdev);
#endif
static int fuse_inop_mknod(struct pnode *pno, mode_t mode, dev_t dev)
{
    struct inode *ino = NULL;
    struct filesys *fs = NULL;
    struct fuse_filesys *ffs = NULL;
    struct curvefs_inode *cino = NULL;

    CURVEFS_DPRINTF("-------------- in fuse_inop_mknod pno=%p ino=%p\n", pno, ino);

    ino = pno->p_parent->p_base->pb_ino;
    fs = ino->i_fs;
    ffs = FS2FFS(fs);
    fuse_ino_t pino = get_inodeid(ino);

    struct Fuse_Queue_Mesg fuse_msg;
    struct Args_Mknod_in args_in;
    struct fuse_entry_param args_out;

    fuse_msg._method = FUSE_LOWLEVEL_MKNOD;
    fuse_msg._args_in = &args_in;
    fuse_msg._args_out = &args_out;
    fuse_msg.co = get_co();
    fuse_msg._ret = 0;

    args_in._ino = pino;
    args_in._path = pno->p_base->pb_key.pbk_name.name;
    args_in._mode = mode;

    send_curvefs_request((void *)&fuse_msg);
    co_yield (fuse_msg.co);

    if (fuse_msg._ret)
    {
        return fuse_msg._ret;
    }

    CURVEFS_DPRINTF("fuse_inop_mknod msg=%p out=%p the ino=%ld, mode=%d\n", &fuse_msg, fuse_msg._args_out, args_out.ino, args_out.attr.st_mode);
    cino = fuse_i_alloc(ffs, args_out.ino, (struct intnl_stat *)&args_out.attr);
    ino = _sysio_i_new(fs, &cino->_fid, (struct intnl_stat *)&args_out.attr, 1, &fuse_i_ops, cino);

    PB_SET_ASSOC(pno->p_base, ino);
    return 0;
}

#ifdef _HAVE_STATVFS
static int fuse_inop_statvfs(struct pnode *pno, struct inode *ino, struct intnl_statvfs *buf)
{
    return 0;
}
#endif

static int fuse_inop_ftruncate(struct pnode *pno, size_t size)
{
    CURVEFS_DPRINTF("-------------- in fuse_inop_ftruncate pno=%p size=%ld\n", pno, size);
    return 0;
}

static void fuse_inop_gone(struct inode *ino)
{
    struct curvefs_inode *cino = I2FI(ino);
    fuse_i_destory(cino);
    return;
}

static ssize_t fuse_filldirentries(struct pnode *pno, _SYSIO_OFF_T *posp, char *buf, size_t nbytes)
{
    return 0;
}