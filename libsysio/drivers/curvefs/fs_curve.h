#ifndef FS_CURVE_INCLUDE
#define FS_CURVE_INCLUDE

#include <sys/queue.h>
#include <sys/stat.h>

#define FUSE_USE_VERSION 34 
#include "fuse.h"
#include "fuse_lowlevel.h"
#include "fuse_kernel.h"

#include "co_routine_inner.h"
#include "co_routine.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "sysio.h"
#include "inode.h"

/*
 * fuse file system driver support.
 */
#define FUSE_BLKSIZE 4096

enum Fuse_Lowlevel_Method {
    FUSE_LOWLEVEL_INIT,
    FUSE_LOWLEVEL_DESTROY,
    FUSE_LOWLEVEL_LOOKUP,
    FUSE_LOWLEVEL_FORGET,
    FUSE_LOWLEVEL_GETATTR,
    FUSE_LOWLEVEL_SETATTR,
    FUSE_LOWLEVEL_READLINK,
    FUSE_LOWLEVEL_MKNOD,
    FUSE_LOWLEVEL_MKDIR,
    FUSE_LOWLEVEL_UNLINK,
    FUSE_LOWLEVEL_RMDIR,
    FUSE_LOWLEVEL_SYMLINK,
    FUSE_LOWLEVEL_RENAME,
    FUSE_LOWLEVEL_LINK,
    FUSE_LOWLEVEL_OPEN,
    FUSE_LOWLEVEL_READ,
    FUSE_LOWLEVEL_WRITE,
    FUSE_LOWLEVEL_FLUSH,
    FUSE_LOWLEVEL_RELEASE,
    FUSE_LOWLEVEL_FSYNC,
    FUSE_LOWLEVEL_OPENDIR,
    FUSE_LOWLEVEL_READDIR,
    FUSE_LOWLEVEL_FSYNCDIR,
    FUSE_LOWLEVEL_STATFS,
    FUSE_LOWLEVEL_SETXATTR,
    FUSE_LOWLEVEL_GETXATTR,
    FUSE_LOWLEVEL_LISTXATTR,
    FUSE_LOWLEVEL_REMOVEXATTR,
    FUSE_LOWLEVEL_ACCESS,
    FUSE_LOWLEVEL_CREATE,
    FUSE_LOWLEVEL_GETLK,
    FUSE_LOWLEVEL_SETLK,
    FUSE_LOWLEVEL_BMAP,
    FUSE_LOWLEVEL_IOCTL,
    FUSE_LOWLEVEL_POLL,
    FUSE_LOWLEVEL_WRITE_BUF,
    FUSE_LOWLEVEL_RETRIEVE_REPLY,
    FUSE_LOWLEVEL_FORGET_MULTI,
    FUSE_LOWLEVEL_FLOCK,
    FUSE_LOWLEVEL_FALLOCATE,
    FUSE_LOWLEVEL_READDIRPLUS,
    FUSE_LOWLEVEL_COPY_FILE_RANGE,
    FUSE_LOWLEVEL_LSEEK,
    FUSE_LOWLEVEL_END_NUMBER,
};

struct curvefs_inode {
    LIST_ENTRY(curvefs_inode) fi_link;         /* i-nodes list link */
    fuse_ino_t _ino;
    struct file_identifier _fid;
    struct intnl_stat fi_st;
    struct fuse_file_info fi_file;
};

#if 0
struct fuse_inode {
        LIST_ENTRY(fuse_inode) fi_link;         /* i-nodes list link */
        struct intnl_stat fi_st;                /* attrs */
        struct file_identifier fi_fileid;       /* file ID */
        void* fi_data;                          /* file data */
        struct fuse_file_info* fi_file;         /* fuse file info */
	    struct fuse_lowlevel_ops fi_ll_ops;		/* fuse operations */
	    char* fi_path;				/* file path */
};
#endif

struct fuse_filesys {
	LIST_HEAD(, curvefs_inode) ffs_finodes; 	/* all i-nodes list */
};

/*
 * Given mode bits, return directory entry type code.
 */
#define FUSE_D_TYPEOF(m)      (((m) & S_IFMT) >> 12)

#if 0
/* calculate size of a directory entry given length of the entry name  */
#define FUSE_D_RECLEN(namelen) \
	(((size_t )&((struct intnl_dirent *)0)->d_name + \
          (namelen) + 1 + sizeof(void *)) & \
         ~(sizeof(void *) - 1))
#endif

struct Args_Lookup_in {
    fuse_ino_t _ino;
    const char* _path;
};

struct Fuse_Queue_Mesg {
    enum Fuse_Lowlevel_Method _method;
    void* _args_in;
    void* _args_out;
    stCoRoutine_t* co;
    int _ret;
};

struct Args_Getattr_in {
    fuse_ino_t _ino;
    struct fuse_file_info* _finfo;
};

struct Args_Setattr_in {
    fuse_ino_t _ino;
    unsigned _mask;
    struct stat* _stat;
    struct fuse_file_info* _finfo;
};

struct Args_Mknod_in {
    fuse_ino_t _ino;
    const char* _path;
    mode_t _mode;
};

struct Args_Open_in {
    fuse_ino_t _ino;
    struct fuse_file_info _info;
};

struct Args_Open_out {
    struct fuse_open_out _fout;
};

struct Args_Close_in {
    fuse_ino_t _ino;
    struct fuse_file_info* _finfo;
};

struct Args_Write_in {
    fuse_ino_t _ino;
    const char* _buf;
    size_t _size;
    off_t _off;
    struct fuse_file_info* _finfo;
};

struct Args_Write_out {
    uint32_t _size;
};

struct Args_Read_in {
    fuse_ino_t _ino;
    const char* _buf;
    size_t _size;
    off_t _off;
    struct fuse_file_info* _finfo;
};

struct Args_Read_out {
    const char* _buf;
    size_t _size;
};

/*
 * Native file system driver support.
 */


extern int _sysio_curvefs_init(void);

#ifdef __cplusplus
}
#endif

#endif
