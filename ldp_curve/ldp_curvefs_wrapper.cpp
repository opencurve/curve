
#include <signal.h>
#include <dlfcn.h>

#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>
#include <dirent.h>
#include <stdlib.h>

#include <sys/stat.h>
#include <sys/select.h>

#include "ldp_curvefs_wrapper.h"

enum swrap_dbglvl_e
{
  SWRAP_LOG_ERROR = 0,
  SWRAP_LOG_WARN,
  SWRAP_LOG_DEBUG,
  SWRAP_LOG_TRACE
};

/* Macros for accessing mutexes */
#define SWRAP_LOCK(m) do { \
        pthread_mutex_lock(&(m ## _mutex)); \
} while(0)

#define SWRAP_UNLOCK(m) do { \
        pthread_mutex_unlock(&(m ## _mutex)); \
} while(0)

/* Add new global locks here please */
#define SWRAP_LOCK_ALL \
        SWRAP_LOCK(libc_symbol_binding); \

#define SWRAP_UNLOCK_ALL \
        SWRAP_UNLOCK(libc_symbol_binding); \


/* The mutex for accessing the global libc.symbols */
static pthread_mutex_t libc_symbol_binding_mutex = PTHREAD_MUTEX_INITIALIZER;

/* GCC have printf type attribute check. */
#ifdef HAVE_FUNCTION_ATTRIBUTE_FORMAT
#define PRINTF_ATTRIBUTE(a,b) __attribute__ ((__format__ (__printf__, a, b)))
#else
#define PRINTF_ATTRIBUTE(a,b)
#endif /* HAVE_FUNCTION_ATTRIBUTE_FORMAT */

/* Function prototypes */

#ifdef NDEBUG
#define SWRAP_LOG(...)
#else
static unsigned int swrap_log_lvl = SWRAP_LOG_WARN;

static void
swrap_log (enum swrap_dbglvl_e dbglvl, const char *func,
	   const char *format, ...)
PRINTF_ATTRIBUTE (3, 4);
#define SWRAP_LOG(dbglvl, ...) swrap_log((dbglvl), __func__, __VA_ARGS__)

static void swrap_log (enum swrap_dbglvl_e dbglvl,
		  const char *func, const char *format, ...) {
    char buffer[1024];
    va_list va;

    va_start (va, format);
    vsnprintf (buffer, sizeof (buffer), format, va);
    va_end (va);

    if (dbglvl <= swrap_log_lvl) {
        switch (dbglvl) {
	    case SWRAP_LOG_ERROR:
	        fprintf (stderr, "SWRAP_ERROR(%d) - %s: %s\n", (int) getpid (), func, buffer);
	        break;
	    case SWRAP_LOG_WARN:
	        fprintf (stderr, "SWRAP_WARN(%d) - %s: %s\n", (int) getpid (), func, buffer);
	        break;
	    case SWRAP_LOG_DEBUG:
	        fprintf (stderr, "SWRAP_DEBUG(%d) - %s: %s\n", (int) getpid (), func, buffer);
	        break;
	    case SWRAP_LOG_TRACE:
	        fprintf (stderr, "SWRAP_TRACE(%d) - %s: %s\n", (int) getpid (), func, buffer);
	        break;
	    }
    }
}
#endif

/*********************************************************
 * SWRAP LOADING LIBC FUNCTIONS
 *********************************************************/
typedef int (* __libc_access) (const char * __name, int __type);

typedef int (* __libc_acct) (const char *__name);

typedef int (* __libc_chdir) (const char *__path);

typedef int (* __libc_chmod) (const char *__file, __mode_t __mode);

typedef int (* __libc_chown) (const char *__file, __uid_t __owner, __gid_t __group);

typedef int (* __libc_chroot) (const char *__path);

typedef int (* __libc_close) (int __fd);

typedef int (* __libc_close_range) (unsigned int __fd, unsigned int __max_fd,
			int __flags);

typedef ssize_t (* __libc_copy_file_range) (int __infd, __off64_t *__pinoff,
			 int __outfd, __off64_t *__poutoff,
			 size_t __length, unsigned int __flags);

typedef int (* __libc_creat) (const char *__file, mode_t __mode);
typedef int (* __libc_creat64) (const char *__file, mode_t __mode);

typedef int (* __libc_dup) (int __fd);
typedef int (* __libc_dup2) (int __fd, int __fd2);
typedef int (* __libc_dup3) (int __fd, int __fd2, int __flags);

typedef int (* __libc_faccessat) (int __fd, const char *__file, int __type, int __flag);

typedef int (* __libc_posix_fadvise64) (int __fd, off64_t __offset, off64_t __len,
			    int __advise);
typedef int (* __libc_posix_fallocate) (int __fd, off_t __offset, off_t __len);

typedef int (* __libc_posix_fallocate64) (int __fd, off64_t __offset, off64_t __len);

typedef int (* __libc_fallocate) (int __fd, int __mode, __off_t __offset, __off_t __len);

typedef int (* __libc_fallocate64) (int __fd, int __mode, __off64_t __offset,
        __off64_t __len);

typedef int (* __libc_fchdir) (int __fd);

typedef int (* __libc_fchmod) (int __fd, __mode_t __mode);

typedef int (* __libc_fchmodat) (int __fd, const char *__file, __mode_t __mode,
		     int __flag);

typedef int (* __libc_fchown) (int __fd, __uid_t __owner, __gid_t __group);

typedef int (* __libc_fchownat) (int __fd, const char *__file, __uid_t __owner,
		     __gid_t __group, int __flag);

typedef int (* __libc_fcntl) (int __fd, int __cmd, ...);
typedef int (* __libc_fcntl64) (int __fd, int __cmd, ...);

typedef int (* __libc_fdatasync) (int __fildes);

typedef ssize_t (* __libc_fgetxattr) (int __fd, const char *__name, void *__value,
			  size_t __size);

typedef ssize_t (* __libc_flistxattr) (int __fd, char *__list, size_t __size);

typedef int (* __libc_flock) (int __fd, int __operation);

typedef int (* __libc_fremovexattr) (int __fd, const char *__name);

typedef int (* __libc_fsetxattr) (int __fd, const char *__name, const void *__value,
		      size_t __size, int __flags);

typedef int (* __libc_fstat) (int __fd, struct stat *__buf);

typedef int (* __libc_fstat64) (int __fd, struct stat64 *__buf);

typedef int (* __libc_fstatat64) (int __fd, const char *__restrict __file,
		      struct stat64 *__restrict __buf, int __flag);

typedef int (* __libc_fstatfs) (int __fildes, struct statfs *__buf);

typedef int (* __libc_fstatfs64) (int __fildes, struct statfs64 *__buf);

typedef int (* __libc_fsync) (int __fd);

typedef int (* __libc_ftime) (struct timeb *__timebuf);

typedef int (* __libc_ftruncate) (int __fd, __off_t __length);

typedef int (* __libc_ftruncate64) (int __fd, __off64_t __length);

typedef int (* __libc_futimes) (int __fd, const struct timeval __tvp[2]);

typedef int (* __libc_futimesat) (int __fd, const char *__file,
		      const struct timeval __tvp[2]);

typedef char * (* __libc_getcwd) (char *__buf, size_t __size);

typedef __ssize_t (* __libc_getdents64) (int __fd, void *__buffer, size_t __length);

typedef ssize_t (* __libc_getxattr) (const char *__path, const char *__name,
			 void *__value, size_t __size);

typedef int (* __libc_ioctl) (int __fd, unsigned long int __request, ...);

typedef int (* __libc_lchown) (const char *__file, __uid_t __owner, __gid_t __group);

typedef ssize_t (* __libc_lgetxattr) (const char *__path, const char *__name,
			  void *__value, size_t __size);

typedef int (* __libc_link) (const char *__from, const char *__to);

typedef int (* __libc_linkat) (int __fromfd, const char *__from, int __tofd,
		   const char *__to, int __flags);

typedef ssize_t (* __libc_listxattr) (const char *__path, char *__list, size_t __size);

typedef ssize_t (* __libc_llistxattr) (const char *__path, char *__list, size_t __size);

typedef int (* __libc_lremovexattr) (const char *__path, const char *__name);

typedef __off_t (* __libc_lseek) (int __fd, __off_t __offset, int __whence);

typedef __off64_t (* __libc_lseek64) (int __fd, __off64_t __offset, int __whence);

typedef int (* __libc_lsetxattr) (const char *__path, const char *__name,
		      const void *__value, size_t __size, int __flags);

typedef int (* __libc_lstat) (const char *__restrict __file,
		  struct stat *__restrict __buf);

typedef int (* __libc_lstat64) (const char *__restrict __file,
		    struct stat64 *__restrict __buf);

typedef int (* __libc_madvise) (void *__addr, size_t __len, int __advice);

typedef int (* __libc_posix_madvise) (void *__addr, size_t __len, int __advice);

typedef int (* __libc_mkdir) (const char *__path, __mode_t __mode);

typedef int (* __libc_mkdirat) (int __fd, const char *__path, __mode_t __mode);

typedef int (* __libc_mknod) (const char *__path, __mode_t __mode, __dev_t __dev);

typedef int (* __libc_mknodat) (int __fd, const char *__path, __mode_t __mode,
		    __dev_t __dev);

typedef void * (* __libc_mmap) (void *__addr, size_t __len, int __prot,
		   int __flags, int __fd, __off_t __offset);

typedef void * (* __libc_mmap64) (void *__addr, size_t __len, int __prot,
		     int __flags, int __fd, __off64_t __offset);


typedef int (* __libc_mount) (const char *__special_file, const char *__dir,
		  const char *__fstype, unsigned long int __rwflag,
		  const void *__data);

typedef void * (* __libc_mremap) (void *__addr, size_t __old_len, size_t __new_len,
		     int __flags, ...);

typedef int (* __libc_msync) (void *__addr, size_t __len, int __flags);

typedef int (* __libc_munmap) (void *__addr, size_t __len);

typedef int (* __libc_open) (const char *__file, int __oflag, ...);

typedef int (* __libc_open64) (const char *__file, int __oflag, ...);

typedef int (* __libc_openat) (int __fd, const char *__file, int __oflag, ...);

typedef int (* __libc_openat64) (int __fd, const char *__file, int __oflag, ...);

typedef ssize_t (* __libc_pread) (int __fd, void *__buf, size_t __nbytes,
		      __off_t __offset);

typedef ssize_t (* __libc_pread64) (int __fd, void *__buf, size_t __nbytes,
			__off64_t __offset);

typedef ssize_t (* __libc_preadv) (int __fd, const struct iovec *__iovec, int __count,
		       __off_t __offset);

typedef ssize_t (* __libc_preadv2) (int __fp, const struct iovec *__iovec, int __count,
			__off_t __offset, int ___flags);

typedef ssize_t (* __libc_pwrite) (int __fd, const void *__buf, size_t __n,
		       __off_t __offset);

typedef ssize_t (* __libc_pwrite64) (int __fd, const void *__buf, size_t __n,
			 __off64_t __offset);
    
typedef ssize_t (* __libc_pwritev) (int __fd, const struct iovec *__iovec, int __count,
			__off_t __offset);

typedef ssize_t (* __libc_pwritev2) (int __fd, const struct iovec *__iodev, int __count,
			 __off_t __offset, int __flags);

typedef ssize_t (* __libc_read) (int __fd, void *__buf, size_t __nbytes);

typedef __ssize_t (* __libc_readahead) (int __fd, __off64_t __offset, size_t __count);

typedef struct dirent * (* __libc_readdir) (DIR *__dirp);

typedef struct dirent64 * (* __libc_readdir64) (DIR *__dirp);

typedef ssize_t (* __libc_readlink) (const char *__restrict __path,
			 char *__restrict __buf, size_t __len);

typedef ssize_t (* __libc_readlinkat) (int __fd, const char *__restrict __path,
			   char *__restrict __buf, size_t __len);

typedef ssize_t (* __libc_readv) (int __fd, const struct iovec *__iovec, int __count);

typedef int (* __libc_remap_file_pages) (void *__start, size_t __size, int __prot,
			     size_t __pgoff, int __flags);

typedef int (* __libc_removexattr) (const char *__path, const char *__name);

typedef int (* __libc_rename) (const char *__old, const char *__new);

typedef int (* __libc_renameat) (int __oldfd, const char *__old, int __newfd,
		     const char *__new);

typedef int (* __libc_renameat2) (int __oldfd, const char *__old, int __newfd,
		      const char *__new, unsigned int __flags);

typedef int (* __libc_rmdir) (const char *__path);

typedef int (* __libc_select) (int __nfds, fd_set *__restrict __readfds,
		   fd_set *__restrict __writefds,
		   fd_set *__restrict __exceptfds,
		   struct timeval *__restrict __timeout);

typedef ssize_t (* __libc_sendfile) (int __out_fd, int __in_fd, off_t *__offset,
			 size_t __count);

typedef ssize_t (* __libc_sendfile64) (int __out_fd, int __in_fd, __off64_t *__offset,
			   size_t __count);

typedef int (* __libc_setxattr) (const char *__path, const char *__name,
		     const void *__value, size_t __size, int __flags);

typedef int (* __libc_stat) (const char *__restrict __file,
		 struct stat *__restrict __buf);

typedef int (* __libc_stat64) (const char *__restrict __file,
		   struct stat64 *__restrict __buf);

typedef int (* __libc_statfs) (const char *__file, struct statfs *__buf);

typedef int (* __libc_statfs64) (const char *__file, struct statfs64 *__buf);

typedef int (* __libc_statx) (int __dirfd, const char *__restrict __path, int __flags,
           unsigned int __mask, struct statx *__restrict __buf);

typedef int (* __libc_symlink) (const char *__from, const char *__to);

typedef int (* __libc_symlinkat) (const char *__from, int __tofd,
		      const char *__to);

typedef void (* __libc_sync) (void);

typedef int (* __libc_sync_file_range) (int __fd, __off64_t __offset, __off64_t __count,
			    unsigned int __flags);

typedef int (* __libc_syncfs) (int __fd);

typedef int (* __libc_truncate) (const char *__file, __off_t __length);

typedef int (* __libc_truncate64) (const char *__file, __off64_t __length);

typedef int (* __libc_umount) (const char *__special_file);

typedef int (* __libc_umount2) (const char *__special_file, int __flags);

typedef int (* __libc_unlink) (const char *__name);

typedef int (* __libc_unlinkat) (int __fd, const char *__name, int __flag);

typedef int (* __libc_utime) (const char *__file,
		  const struct utimbuf *__file_times);

typedef int (* __libc_utimensat) (int __fd, const char *__path,
		      const struct timespec __times[2],
		      int __flags);

typedef int (* __libc_utimes) (const char *__file, const struct timeval __tvp[2]);

typedef ssize_t (* __libc_write) (int __fd, const void *__buf, size_t __n);

typedef ssize_t (* __libc_writev) (int __fd, const struct iovec *__iovec, int __count);



#define SWRAP_SYMBOL_ENTRY(i) \
        union { \
                __libc_##i f; \
                void *obj; \
        } _libc_##i

struct swrap_libc_symbols {
    SWRAP_SYMBOL_ENTRY(access);
    SWRAP_SYMBOL_ENTRY(acct);
    SWRAP_SYMBOL_ENTRY(chdir);
    SWRAP_SYMBOL_ENTRY(chmod);
    SWRAP_SYMBOL_ENTRY(chown);
    SWRAP_SYMBOL_ENTRY(chroot);
    SWRAP_SYMBOL_ENTRY(close);
    SWRAP_SYMBOL_ENTRY(close_range);
    SWRAP_SYMBOL_ENTRY(copy_file_range);
    SWRAP_SYMBOL_ENTRY(creat);
    SWRAP_SYMBOL_ENTRY(creat64);
    SWRAP_SYMBOL_ENTRY(dup);
    SWRAP_SYMBOL_ENTRY(dup2);
    SWRAP_SYMBOL_ENTRY(dup3);
    SWRAP_SYMBOL_ENTRY(faccessat);
    SWRAP_SYMBOL_ENTRY(posix_fadvise64);
    SWRAP_SYMBOL_ENTRY(posix_fallocate);
    SWRAP_SYMBOL_ENTRY(posix_fallocate64);
    SWRAP_SYMBOL_ENTRY(fallocate);
    SWRAP_SYMBOL_ENTRY(fallocate64);
    SWRAP_SYMBOL_ENTRY(fchdir);
    SWRAP_SYMBOL_ENTRY(fchmod);
    SWRAP_SYMBOL_ENTRY(fchmodat);
    SWRAP_SYMBOL_ENTRY(fchown);
    SWRAP_SYMBOL_ENTRY(fchownat);
    SWRAP_SYMBOL_ENTRY(fcntl);
    SWRAP_SYMBOL_ENTRY(fcntl64);
    SWRAP_SYMBOL_ENTRY(fdatasync);
    SWRAP_SYMBOL_ENTRY(fgetxattr);
    SWRAP_SYMBOL_ENTRY(flistxattr);
    SWRAP_SYMBOL_ENTRY(flock);
    SWRAP_SYMBOL_ENTRY(fremovexattr);
    SWRAP_SYMBOL_ENTRY(fsetxattr);
    SWRAP_SYMBOL_ENTRY(fstat);
    SWRAP_SYMBOL_ENTRY(fstat64);
    SWRAP_SYMBOL_ENTRY(fstatat64);
    SWRAP_SYMBOL_ENTRY(fstatfs);
    SWRAP_SYMBOL_ENTRY(fstatfs64);
    SWRAP_SYMBOL_ENTRY(fsync);
    SWRAP_SYMBOL_ENTRY(ftime);
    SWRAP_SYMBOL_ENTRY(ftruncate);
    SWRAP_SYMBOL_ENTRY(ftruncate64);
    SWRAP_SYMBOL_ENTRY(futimes);
    SWRAP_SYMBOL_ENTRY(futimesat);
    SWRAP_SYMBOL_ENTRY(getcwd);
    SWRAP_SYMBOL_ENTRY(getdents64);
    SWRAP_SYMBOL_ENTRY(getxattr);
    SWRAP_SYMBOL_ENTRY(ioctl);
    SWRAP_SYMBOL_ENTRY(lchown);
    SWRAP_SYMBOL_ENTRY(lgetxattr);
    SWRAP_SYMBOL_ENTRY(link);
    SWRAP_SYMBOL_ENTRY(linkat);
    SWRAP_SYMBOL_ENTRY(listxattr);
    SWRAP_SYMBOL_ENTRY(llistxattr);
    SWRAP_SYMBOL_ENTRY(lremovexattr);
    SWRAP_SYMBOL_ENTRY(lseek);
    SWRAP_SYMBOL_ENTRY(lseek64);
    SWRAP_SYMBOL_ENTRY(lsetxattr);
    SWRAP_SYMBOL_ENTRY(lstat);
    SWRAP_SYMBOL_ENTRY(lstat64);
    SWRAP_SYMBOL_ENTRY(madvise);
    SWRAP_SYMBOL_ENTRY(posix_madvise);
    SWRAP_SYMBOL_ENTRY(mkdir);
    SWRAP_SYMBOL_ENTRY(mkdirat);
    SWRAP_SYMBOL_ENTRY(mknod);
    SWRAP_SYMBOL_ENTRY(mknodat);
    SWRAP_SYMBOL_ENTRY(mmap);
    SWRAP_SYMBOL_ENTRY(mmap64);
    SWRAP_SYMBOL_ENTRY(mount);
    SWRAP_SYMBOL_ENTRY(mremap);
    SWRAP_SYMBOL_ENTRY(msync);
    SWRAP_SYMBOL_ENTRY(munmap);
    SWRAP_SYMBOL_ENTRY(open);
    SWRAP_SYMBOL_ENTRY(open64);
    SWRAP_SYMBOL_ENTRY(openat);
    SWRAP_SYMBOL_ENTRY(openat64);
    SWRAP_SYMBOL_ENTRY(pread);
    SWRAP_SYMBOL_ENTRY(pread64);
    SWRAP_SYMBOL_ENTRY(preadv);
    SWRAP_SYMBOL_ENTRY(preadv2);
    SWRAP_SYMBOL_ENTRY(pwrite);
    SWRAP_SYMBOL_ENTRY(pwrite64);
    SWRAP_SYMBOL_ENTRY(pwritev);
    SWRAP_SYMBOL_ENTRY(pwritev2);
    SWRAP_SYMBOL_ENTRY(read);
    SWRAP_SYMBOL_ENTRY(readahead);
    SWRAP_SYMBOL_ENTRY(readdir);
    SWRAP_SYMBOL_ENTRY(readdir64);
    SWRAP_SYMBOL_ENTRY(readlink);
    SWRAP_SYMBOL_ENTRY(readlinkat);
    SWRAP_SYMBOL_ENTRY(readv);
    SWRAP_SYMBOL_ENTRY(remap_file_pages);
    SWRAP_SYMBOL_ENTRY(removexattr);
    SWRAP_SYMBOL_ENTRY(rename);
    SWRAP_SYMBOL_ENTRY(renameat);
    SWRAP_SYMBOL_ENTRY(renameat2);
    SWRAP_SYMBOL_ENTRY(rmdir);
    SWRAP_SYMBOL_ENTRY(select);
    SWRAP_SYMBOL_ENTRY(sendfile);
    SWRAP_SYMBOL_ENTRY(sendfile64);
    SWRAP_SYMBOL_ENTRY(setxattr);
    SWRAP_SYMBOL_ENTRY(stat);
    SWRAP_SYMBOL_ENTRY(stat64);
    SWRAP_SYMBOL_ENTRY(statfs);
    SWRAP_SYMBOL_ENTRY(statfs64);
    SWRAP_SYMBOL_ENTRY(statx);
    SWRAP_SYMBOL_ENTRY(symlink);
    SWRAP_SYMBOL_ENTRY(symlinkat);
    SWRAP_SYMBOL_ENTRY(sync);
    SWRAP_SYMBOL_ENTRY(sync_file_range);
    SWRAP_SYMBOL_ENTRY(syncfs);
    SWRAP_SYMBOL_ENTRY(truncate);
    SWRAP_SYMBOL_ENTRY(truncate64);
    SWRAP_SYMBOL_ENTRY(umount);
    SWRAP_SYMBOL_ENTRY(umount2);
    SWRAP_SYMBOL_ENTRY(unlink);
    SWRAP_SYMBOL_ENTRY(unlinkat);
    SWRAP_SYMBOL_ENTRY(utime);
    SWRAP_SYMBOL_ENTRY(utimensat);
    SWRAP_SYMBOL_ENTRY(utimes);
    SWRAP_SYMBOL_ENTRY(write);
    SWRAP_SYMBOL_ENTRY(writev);
};

struct swrap {
    struct {
        void *handle;
        void *socket_handle;
        struct swrap_libc_symbols symbols;
    } libc;
};

static struct swrap swrap;

#define LIBC_NAME "libc.so"

enum swrap_lib {
    SWRAP_LIBC,
};

#ifndef NDEBUG
static const char * swrap_str_lib (enum swrap_lib lib) {
    switch (lib) {
    case SWRAP_LIBC:
        return "libc";
    }

    /* Compiler would warn us about unhandled enum value if we get here */
    return "unknown";
}
#endif

static void * swrap_load_lib_handle (enum swrap_lib lib) {
    int flags = RTLD_LAZY;
    void *handle = NULL;
    int i;

#if defined(RTLD_DEEPBIND) && !defined(CLIB_SANITIZE_ADDR)
    flags |= RTLD_DEEPBIND;
#endif

    switch (lib) {
    case SWRAP_LIBC:
        handle = swrap.libc.handle;
#ifdef LIBC_SO
        if (handle == NULL) {
            handle = dlopen (LIBC_SO, flags);
            swrap.libc.handle = handle;
        }
#endif
        if (handle == NULL) {
            for (i = 10; i >= 0; i--) {
                char soname[256] = { 0 };

                snprintf (soname, sizeof (soname), "libc.so.%d", i);
                handle = dlopen (soname, flags);
                if (handle != NULL) {
                    break;
                }
	        }

	        swrap.libc.handle = handle;
	    }
        break;
    }

    if (handle == NULL) {
        SWRAP_LOG (SWRAP_LOG_ERROR, "Failed to dlopen library: %s\n", dlerror ());
        exit (-1);
    }

    return handle;
}

static void* _swrap_bind_symbol (enum swrap_lib lib, const char *fn_name) {
    void *handle;
    void *func;

    handle = swrap_load_lib_handle (lib);

    func = dlsym (handle, fn_name);
    if (func == NULL) {
        SWRAP_LOG (SWRAP_LOG_ERROR, "Failed to find %s: %s\n", fn_name, dlerror ());
        exit (-1);
    }

    SWRAP_LOG (SWRAP_LOG_TRACE, "Loaded %s from %s", fn_name, swrap_str_lib (lib));

    return func;
}

#define swrap_bind_symbol_libc(sym_name) \
        SWRAP_LOCK(libc_symbol_binding); \
        if (swrap.libc.symbols._libc_##sym_name.obj == NULL) { \
                swrap.libc.symbols._libc_##sym_name.obj = \
                        _swrap_bind_symbol(SWRAP_LIBC, #sym_name); \
        } \
        SWRAP_UNLOCK(libc_symbol_binding)

/*
 * IMPORTANT
 *
 * Functions especially from libc need to be loaded individually, you can't load
 * all at once or gdb will segfault at startup. The same applies to valgrind and
 * has probably something todo with with the linker.
 * So we need load each function at the point it is called the first time.
 */
/* Test for access to NAME using the real UID and real GID. */
int libc_access (const char * __name, int __type) {
    swrap_bind_symbol_libc (access);
    return swrap.libc.symbols._libc_access.f (__name, __type);
}

/* Turn accounting on if NAME is an existing file. The System will then write
   a record for each process as it terminates, to this file. If NAME is NULL, 
   turn accounting off. This call is restricted to the super-user.  */
int libc_acct (const char *__name) {
    swrap_bind_symbol_libc (acct);
    return swrap.libc.symbols._libc_acct.f (__name);
}

/* Change the process's working directory to PATH.  */
int libc_chdir (const char *__path) {
    swrap_bind_symbol_libc (chdir);
    return swrap.libc.symbols._libc_chdir.f (__path);    
}

/* Set file access perminssions for FILE to MODE.
   If FILE is a symbolic link, this affects its target instead.  */
int libc_chmod (const char *__file, __mode_t __mode) {
    swrap_bind_symbol_libc (chmod);
    return swrap.libc.symbols._libc_chmod.f (__file, __mode); 
}

/* Change the owner and group of FILE.  */
int libc_chown (const char *__file, __uid_t __owner, __gid_t __group) {
    swrap_bind_symbol_libc (chown);
    return swrap.libc.symbols._libc_chown.f (__file, __owner, __group); 
}

/* Make PATH be the root directory (the starting point for absolute paths).
   This call is restricted to the super-user.  */
int libc_chroot (const char *__path) {
    swrap_bind_symbol_libc (chroot);
    return swrap.libc.symbols._libc_chroot.f (__path); 
}

/* Close the file descriptor FD.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_close (int __fd) {
    swrap_bind_symbol_libc (close);
    return swrap.libc.symbols._libc_close.f (__fd); 
}

/* Close all file descriptors in the range FD up to MAX_FD.  The flag FLAGS
   are define by the CLOSE_RANGE prefix.  This function behaves like close
   on the range and gaps where the file descriptor is invalid or errors
   encountered while closing file descriptors are ignored.   Returns 0 on
   successor or -1 for failure (and sets errno accordingly).  */
int libc_close_range (unsigned int __fd, unsigned int __max_fd,
			int __flags) {
    swrap_bind_symbol_libc (close_range);
    return swrap.libc.symbols._libc_close_range.f (__fd, __max_fd, __flags); 
}

/* Copy LENGTH bytes from INFD to OUTFD.  */
ssize_t libc_copy_file_range (int __infd, __off64_t *__pinoff,
			 int __outfd, __off64_t *__poutoff,
			 size_t __length, unsigned int __flags) {
    swrap_bind_symbol_libc (copy_file_range);
    return swrap.libc.symbols._libc_copy_file_range.f (__infd, __pinoff, __outfd, __poutoff, __length, __flags); 
}

/* Create and open FILE, with mode MODE.  This takes an `int' MODE
   argument because that is what `mode_t' will be widened to.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_creat (const char *__file, mode_t __mode) {
    swrap_bind_symbol_libc (creat);
    return swrap.libc.symbols._libc_creat.f (__file, __mode); 
}

int libc_creat64 (const char *__file, mode_t __mode) {
    swrap_bind_symbol_libc (creat64);
    return swrap.libc.symbols._libc_creat64.f (__file, __mode); 
}

/* Duplicate FD, returning a new file descriptor on the same file.  */
int libc_dup (int __fd) {
    swrap_bind_symbol_libc (dup);
    return swrap.libc.symbols._libc_dup.f (__fd); 
}

/* Duplicate FD to FD2, closing FD2 and making it open on the same file.  */
int libc_dup2 (int __fd, int __fd2) {
    swrap_bind_symbol_libc (dup2);
    return swrap.libc.symbols._libc_dup2.f (__fd, __fd2); 
}

/* Duplicate FD to FD2, closing FD2 and making it open on the same
   file while setting flags according to FLAGS.  */
int libc_dup3 (int __fd, int __fd2, int __flags) {
    swrap_bind_symbol_libc (dup3);
    return swrap.libc.symbols._libc_dup3.f (__fd, __fd2, __flags); 
}

/* Test for access to FILE relative to the directory FD is open on.
   If AT_EACCESS is set in FLAG, then use effective IDs like `eaccess',
   otherwise use real IDs like `access'.  */
int libc_faccessat (int __fd, const char *__file, int __type, int __flag) {
    swrap_bind_symbol_libc (faccessat);
    return swrap.libc.symbols._libc_faccessat.f (__fd, __file, __type, __flag); 
}

int libc_posix_fadvise64 (int __fd, off64_t __offset, off64_t __len,
			    int __advise) {
    swrap_bind_symbol_libc (posix_fadvise64);
    return swrap.libc.symbols._libc_posix_fadvise64.f (__fd, __offset, __len, __advise); 
}

/* Reserve storage for the data of the file associated with FD.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int libc_posix_fallocate (int __fd, off_t __offset, off_t __len) {
    swrap_bind_symbol_libc (posix_fallocate);
    return swrap.libc.symbols._libc_posix_fallocate.f (__fd, __offset, __len); 
}

int libc_posix_fallocate64 (int __fd, off64_t __offset, off64_t __len) {
    swrap_bind_symbol_libc (posix_fallocate64);
    return swrap.libc.symbols._libc_posix_fallocate64.f (__fd, __offset, __len); 
}

/* Reserve storage for the data of the file associated with FD.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int libc_fallocate (int __fd, int __mode, __off_t __offset, __off_t __len) {
    swrap_bind_symbol_libc (fallocate);
    return swrap.libc.symbols._libc_fallocate.f (__fd, __mode, __offset, __len); 
}

int libc_fallocate64 (int __fd, int __mode, __off64_t __offset,
			__off64_t __len) {
    swrap_bind_symbol_libc (fallocate64);
    return swrap.libc.symbols._libc_fallocate64.f (__fd, __mode, __offset, __len); 
}

/* Change the process's working directory to the one FD is open on.  */
int libc_fchdir (int __fd) {
    swrap_bind_symbol_libc (fchdir);
    return swrap.libc.symbols._libc_fchdir.f (__fd); 
}

/* Set file access permissions of the file FD is open on to MODE.  */
int libc_fchmod (int __fd, __mode_t __mode) {
    swrap_bind_symbol_libc (fchmod);
    return swrap.libc.symbols._libc_fchmod.f (__fd, __mode); 
}

/* Set file access permissions of FILE relative to
   the directory FD is open on.  */
int libc_fchmodat (int __fd, const char *__file, __mode_t __mode,
		     int __flag) {
    swrap_bind_symbol_libc (fchmodat);
    return swrap.libc.symbols._libc_fchmodat.f (__fd, __file, __mode, __flag); 
}

/* Change the owner and group of the file that FD is open on.  */
int libc_fchown (int __fd, __uid_t __owner, __gid_t __group) {
    swrap_bind_symbol_libc (fchown);
    return swrap.libc.symbols._libc_fchown.f (__fd, __owner, __group); 
}

/* Change the owner and group of FILE relative to the directory FD is open
   on.  */
int libc_fchownat (int __fd, const char *__file, __uid_t __owner,
		     __gid_t __group, int __flag) {
    swrap_bind_symbol_libc (fchownat);
    return swrap.libc.symbols._libc_fchownat.f (__fd, __file, __owner, __group, __flag); 
}

/* Do the file control operation described by CMD on FD.
   The remaining arguments are interpreted depending on CMD.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_fcntl (int __fd, int __cmd, va_list ap) {
    swrap_bind_symbol_libc (fcntl);
    return swrap.libc.symbols._libc_fcntl.f (__fd, __cmd, va_arg (ap, long int)); 
}

int libc_fcntl64 (int __fd, int __cmd, va_list ap) {
    swrap_bind_symbol_libc (fcntl64);
    return swrap.libc.symbols._libc_fcntl64.f (__fd, __cmd, va_arg (ap, long int)); 
}

/* Synchronize at least the data part of a file with the underlying
   media.  */
int libc_fdatasync (int __fildes) {
    swrap_bind_symbol_libc (fdatasync);
    return swrap.libc.symbols._libc_fdatasync.f (__fildes); 
}

/* Get the attribute NAME of the file descriptor FD to VALUE (which is SIZE
   bytes long).  Return 0 on success, -1 for errors.  */
ssize_t libc_fgetxattr (int __fd, const char *__name, void *__value,
			  size_t __size) {
    swrap_bind_symbol_libc (fgetxattr);
    return swrap.libc.symbols._libc_fgetxattr.f (__fd, __name, __value, __size); 
}

/* List attributes of the file descriptor FD into the user-supplied buffer
   LIST (which is SIZE bytes big).  Return 0 on success, -1 for errors.  */
ssize_t libc_flistxattr (int __fd, char *__list, size_t __size) {
    swrap_bind_symbol_libc (flistxattr);
    return swrap.libc.symbols._libc_flistxattr.f (__fd, __list, __size); 
}

/* Apply or remove an advisory lock, according to OPERATION,
   on the file FD refers to.  */
int libc_flock (int __fd, int __operation) {
    swrap_bind_symbol_libc (flock);
    return swrap.libc.symbols._libc_flock.f (__fd, __operation); 
}

/* Remove the attribute NAME from the file descriptor FD.  Return 0 on
   success, -1 for errors.  */
int libc_fremovexattr (int __fd, const char *__name) {
    swrap_bind_symbol_libc (fremovexattr);
    return swrap.libc.symbols._libc_fremovexattr.f (__fd, __name); 
}

/* Set the attribute NAME of the file descriptor FD to VALUE (which is SIZE
   bytes long).  Return 0 on success, -1 for errors.  */
int libc_fsetxattr (int __fd, const char *__name, const void *__value,
		      size_t __size, int __flags) {
    swrap_bind_symbol_libc (fsetxattr);
    return swrap.libc.symbols._libc_fsetxattr.f (__fd, __name, __value, __size, __flags); 
}

/* Get file attributes for the file, device, pipe, or socket
   that file descriptor FD is open on and put them in BUF.  */
int libc_fstat (int __fd, struct stat *__buf) {
    swrap_bind_symbol_libc (fstat);
    return swrap.libc.symbols._libc_fstat.f (__fd, __buf); 
}

int libc_fstat64 (int __fd, struct stat64 *__buf) {
    swrap_bind_symbol_libc (fstat64);
    return swrap.libc.symbols._libc_fstat64.f (__fd, __buf); 
}

int libc_fstatat64 (int __fd, const char *__restrict __file,
		      struct stat64 *__restrict __buf, int __flag) {
    swrap_bind_symbol_libc (fstatat64);
    return swrap.libc.symbols._libc_fstatat64.f (__fd, __file, __buf, __flag); 
}

/* Return information about the filesystem containing the file FILDES
   refers to.  */
int libc_fstatfs (int __fildes, struct statfs *__buf) {
    swrap_bind_symbol_libc (fstatfs);
    return swrap.libc.symbols._libc_fstatfs.f (__fildes, __buf); 
}

int libc_fstatfs64 (int __fildes, struct statfs64 *__buf) {
    swrap_bind_symbol_libc (fstatfs64);
    return swrap.libc.symbols._libc_fstatfs64.f (__fildes, __buf); 
}

/* Make all changes done to FD actually appear on disk.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_fsync (int __fd) {
    swrap_bind_symbol_libc (fsync);
    return swrap.libc.symbols._libc_fsync.f (__fd); 
}

/* Fill in TIMEBUF with information about the current time.  */

int libc_ftime (struct timeb *__timebuf) {
    swrap_bind_symbol_libc (ftime);
    return swrap.libc.symbols._libc_ftime.f (__timebuf); 
}

/* Truncate the file FD is open on to LENGTH bytes.  */
int libc_ftruncate (int __fd, __off_t __length) {
    swrap_bind_symbol_libc (ftruncate);
    return swrap.libc.symbols._libc_ftruncate.f (__fd, __length); 
}

int libc_ftruncate64 (int __fd, __off64_t __length) {
    swrap_bind_symbol_libc (ftruncate64);
    return swrap.libc.symbols._libc_ftruncate64.f (__fd, __length); 
}

/* Same as `utimes', but takes an open file descriptor instead of a name.  */
int libc_futimes (int __fd, const struct timeval __tvp[2]) {
    swrap_bind_symbol_libc (futimes);
    return swrap.libc.symbols._libc_futimes.f (__fd, __tvp); 
}

/* Change the access time of FILE relative to FD to TVP[0] and the
   modification time of FILE to TVP[1].  If TVP is a null pointer, use
   the current time instead.  Returns 0 on success, -1 on errors.  */
int libc_futimesat (int __fd, const char *__file,
		      const struct timeval __tvp[2]) {
    swrap_bind_symbol_libc (futimesat);
    return swrap.libc.symbols._libc_futimesat.f (__fd, __file, __tvp); 
}


/* Get the pathname of the current working directory,
   and put it in SIZE bytes of BUF.  Returns NULL if the
   directory couldn't be determined or SIZE was too small.
   If successful, returns BUF.  In GNU, if BUF is NULL,
   an array is allocated with `malloc'; the array is SIZE
   bytes long, unless SIZE == 0, in which case it is as
   big as necessary.  */
char *libc_getcwd (char *__buf, size_t __size) {
    swrap_bind_symbol_libc (getcwd);
    return swrap.libc.symbols._libc_getcwd.f (__buf, __size); 
}

/* Read from the directory descriptor FD into LENGTH bytes at BUFFER.
   Return the number of bytes read on success (0 for end of
   directory), and -1 for failure.  */
__ssize_t libc_getdents64 (int __fd, void *__buffer, size_t __length) {
    swrap_bind_symbol_libc (getdents64);
    return swrap.libc.symbols._libc_getdents64.f (__fd, __buffer, __length); 
}

/* Get the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long).  Return 0 on success, -1 for errors.  */
ssize_t libc_getxattr (const char *__path, const char *__name,
			 void *__value, size_t __size) {
    swrap_bind_symbol_libc (getxattr);
    return swrap.libc.symbols._libc_getxattr.f (__path, __name, __value, __size); 
}

/* Perform the I/O control operation specified by REQUEST on FD.
   One argument may follow; its presence and type depend on REQUEST.
   Return value depends on REQUEST.  Usually -1 indicates error.  */
int libc_ioctl (int __fd, unsigned long int __request, va_list ap) {
    swrap_bind_symbol_libc (ioctl);
    return swrap.libc.symbols._libc_ioctl.f (__fd, __request, va_arg (ap, long int)); 
}

/* Change owner and group of FILE, if it is a symbolic
   link the ownership of the symbolic link is changed.  */
int libc_lchown (const char *__file, __uid_t __owner, __gid_t __group) {
    swrap_bind_symbol_libc (lchown);
    return swrap.libc.symbols._libc_lchown.f (__file, __owner, __group); 
}

/* Get the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long), not following symlinks for the last pathname component.
   Return 0 on success, -1 for errors.  */
ssize_t libc_lgetxattr (const char *__path, const char *__name,
			  void *__value, size_t __size) {
    swrap_bind_symbol_libc (lgetxattr);
    return swrap.libc.symbols._libc_lgetxattr.f (__path, __name, __value, __size); 
}

/* Make a link to FROM named TO.  */
int libc_link (const char *__from, const char *__to) {
    swrap_bind_symbol_libc (link);
    return swrap.libc.symbols._libc_link.f (__from, __to); 
}

/* Like link but relative paths in TO and FROM are interpreted relative
   to FROMFD and TOFD respectively.  */
int libc_linkat (int __fromfd, const char *__from, int __tofd,
		   const char *__to, int __flags) {
    swrap_bind_symbol_libc (linkat);
    return swrap.libc.symbols._libc_linkat.f (__fromfd, __from, __tofd, __to, __flags); 
}

/* List attributes of the file pointed to by PATH into the user-supplied
   buffer LIST (which is SIZE bytes big).  Return 0 on success, -1 for
   errors.  */
ssize_t libc_listxattr (const char *__path, char *__list, size_t __size) {
    swrap_bind_symbol_libc (listxattr);
    return swrap.libc.symbols._libc_listxattr.f (__path, __list, __size); 
}

/* List attributes of the file pointed to by PATH into the user-supplied
   buffer LIST (which is SIZE bytes big), not following symlinks for the
   last pathname component.  Return 0 on success, -1 for errors.  */
ssize_t libc_llistxattr (const char *__path, char *__list, size_t __size) {
    swrap_bind_symbol_libc (llistxattr);
    return swrap.libc.symbols._libc_llistxattr.f (__path, __list, __size); 
}

/* Remove the attribute NAME from the file pointed to by PATH, not
   following symlinks for the last pathname component.  Return 0 on
   success, -1 for errors.  */
int libc_lremovexattr (const char *__path, const char *__name) {
    swrap_bind_symbol_libc (lremovexattr);
    return swrap.libc.symbols._libc_lremovexattr.f (__path, __name); 
}

/* Move FD's file position to OFFSET bytes from the
   beginning of the file (if WHENCE is SEEK_SET),
   the current position (if WHENCE is SEEK_CUR),
   or the end of the file (if WHENCE is SEEK_END).
   Return the new file position.  */
__off_t libc_lseek (int __fd, __off_t __offset, int __whence) {
    swrap_bind_symbol_libc (lseek);
    return swrap.libc.symbols._libc_lseek.f (__fd, __offset, __whence); 
}

__off64_t libc_lseek64 (int __fd, __off64_t __offset, int __whence) {
    swrap_bind_symbol_libc (lseek64);
    return swrap.libc.symbols._libc_lseek64.f (__fd, __offset, __whence); 
}

/* Set the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long), not following symlinks for the last pathname component.
   Return 0 on success, -1 for errors.  */
int libc_lsetxattr (const char *__path, const char *__name,
		      const void *__value, size_t __size, int __flags) {
    swrap_bind_symbol_libc (lsetxattr);
    return swrap.libc.symbols._libc_lsetxattr.f (__path, __name, __value, __size, __flags); 
}

/* Get file attributes about FILE and put them in BUF.
   If FILE is a symbolic link, do not follow it.  */
int libc_lstat (const char *__restrict __file,
		  struct stat *__restrict __buf) {
    swrap_bind_symbol_libc (lstat);
    return swrap.libc.symbols._libc_lstat.f (__file, __buf); 
}

int libc_lstat64 (const char *__restrict __file,
		    struct stat64 *__restrict __buf) {
    swrap_bind_symbol_libc (lstat64);
    return swrap.libc.symbols._libc_lstat64.f (__file, __buf); 
}

/* Advise the system about particular usage patterns the program follows
   for the region starting at ADDR and extending LEN bytes.  */
int libc_madvise (void *__addr, size_t __len, int __advice) {
    swrap_bind_symbol_libc (madvise);
    return swrap.libc.symbols._libc_madvise.f (__addr, __len, __advice); 
}

/* This is the POSIX name for this function.  */
int libc_posix_madvise (void *__addr, size_t __len, int __advice) {
    swrap_bind_symbol_libc (posix_madvise);
    return swrap.libc.symbols._libc_posix_madvise.f (__addr, __len, __advice); 
}

/* Create a new directory named PATH, with permission bits MODE.  */
int libc_mkdir (const char *__path, __mode_t __mode) {
    swrap_bind_symbol_libc (mkdir);
    return swrap.libc.symbols._libc_mkdir.f (__path, __mode); 
}

/* Like mkdir, create a new directory with permission bits MODE.  But
   interpret relative PATH names relative to the directory associated
   with FD.  */
int libc_mkdirat (int __fd, const char *__path, __mode_t __mode) {
    swrap_bind_symbol_libc (mkdirat);
    return swrap.libc.symbols._libc_mkdirat.f (__fd, __path, __mode); 
}

/* Create a device file named PATH, with permission and special bits MODE
   and device number DEV (which can be constructed from major and minor
   device numbers with the `makedev' macro above).  */
int libc_mknod (const char *__path, __mode_t __mode, __dev_t __dev) {
    swrap_bind_symbol_libc (mknod);
    return swrap.libc.symbols._libc_mknod.f (__path, __mode, __dev); 
}

/* Like mknod, create a new device file with permission bits MODE and
   device number DEV.  But interpret relative PATH names relative to
   the directory associated with FD.  */
int libc_mknodat (int __fd, const char *__path, __mode_t __mode,
		    __dev_t __dev) {
    swrap_bind_symbol_libc (mknodat);
    return swrap.libc.symbols._libc_mknodat.f (__fd, __path, __mode, __dev); 
}


/* Map addresses starting near ADDR and extending for LEN bytes.  from
   OFFSET into the file FD describes according to PROT and FLAGS.  If ADDR
   is nonzero, it is the desired mapping address.  If the MAP_FIXED bit is
   set in FLAGS, the mapping will be at ADDR exactly (which must be
   page-aligned); otherwise the system chooses a convenient nearby address.
   The return value is the actual mapping address chosen or MAP_FAILED
   for errors (in which case `errno' is set).  A successful `mmap' call
   deallocates any previous mapping for the affected region.  */

void *libc_mmap (void *__addr, size_t __len, int __prot,
		   int __flags, int __fd, __off_t __offset) {
    swrap_bind_symbol_libc (mmap);
    return swrap.libc.symbols._libc_mmap.f (__addr, __len, __prot, __flags, __fd, __offset); 
}

void *libc_mmap64 (void *__addr, size_t __len, int __prot,
		     int __flags, int __fd, __off64_t __offset) {
    swrap_bind_symbol_libc (mmap64);
    return swrap.libc.symbols._libc_mmap64.f (__addr, __len, __prot, __flags, __fd, __offset); 
}


/* Mount a filesystem.  */
int libc_mount (const char *__special_file, const char *__dir,
		  const char *__fstype, unsigned long int __rwflag,
		  const void *__data) {
    swrap_bind_symbol_libc (mount);
    return swrap.libc.symbols._libc_mount.f (__special_file, __dir, __fstype, __rwflag, __data); 
}

/* Remap pages mapped by the range [ADDR,ADDR+OLD_LEN) to new length
   NEW_LEN.  If MREMAP_MAYMOVE is set in FLAGS the returned address
   may differ from ADDR.  If MREMAP_FIXED is set in FLAGS the function
   takes another parameter which is a fixed address at which the block
   resides after a successful call.  */
void *libc_mremap (void *__addr, size_t __old_len, size_t __new_len,
		     int __flags, va_list ap) {
    swrap_bind_symbol_libc (mremap);
    return swrap.libc.symbols._libc_mremap.f (__addr, __old_len, __new_len, __flags, va_arg (ap, long int)); 
}

/* Synchronize the region starting at ADDR and extending LEN bytes with the
   file it maps.  Filesystem operations on a file being mapped are
   unpredictable before this is done.  Flags are from the MS_* set.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_msync (void *__addr, size_t __len, int __flags) {
    swrap_bind_symbol_libc (msync);
    return swrap.libc.symbols._libc_msync.f (__addr, __len, __flags); 
}

/* Deallocate any mapping for the region starting at ADDR and extending LEN
   bytes.  Returns 0 if successful, -1 for errors (and sets errno).  */
int libc_munmap (void *__addr, size_t __len) {
    swrap_bind_symbol_libc (munmap);
    return swrap.libc.symbols._libc_munmap.f (__addr, __len); 
}

/* Open FILE and return a new file descriptor for it, or -1 on error.
   OFLAG determines the type of access used.  If O_CREAT or O_TMPFILE is set
   in OFLAG, the third argument is taken as a `mode_t', the mode of the
   created file.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_open (const char *__file, int __oflag, va_list ap) {
    swrap_bind_symbol_libc (open);
    return swrap.libc.symbols._libc_open.f (__file, __oflag, va_arg (ap, long int)); 
}

int libc_open64 (const char *__file, int __oflag, va_list ap) {
    swrap_bind_symbol_libc (open64);
    return swrap.libc.symbols._libc_open64.f (__file, __oflag, va_arg (ap, long int)); 

}

/* Similar to `open' but a relative path name is interpreted relative to
   the directory for which FD is a descriptor.

   NOTE: some other `openat' implementation support additional functionality
   through this interface, especially using the O_XATTR flag.  This is not
   yet supported here.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_openat (int __fd, const char *__file, int __oflag, va_list ap) {
    swrap_bind_symbol_libc (openat);
    return swrap.libc.symbols._libc_openat.f (__fd, __file, __oflag, va_arg (ap, long int)); 
}

int libc_openat64 (int __fd, const char *__file, int __oflag, va_list ap) {
    swrap_bind_symbol_libc (openat64);
    return swrap.libc.symbols._libc_openat64.f (__fd, __file, __oflag, va_arg (ap, long int)); 
}

/* Read NBYTES into BUF from FD at the given position OFFSET without
   changing the file pointer.  Return the number read, -1 for errors
   or 0 for EOF.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_pread (int __fd, void *__buf, size_t __nbytes,
		      __off_t __offset) {
    swrap_bind_symbol_libc (pread);
    return swrap.libc.symbols._libc_pread.f (__fd, __buf, __nbytes, __offset); 
}

/* Read NBYTES into BUF from FD at the given position OFFSET without
   changing the file pointer.  Return the number read, -1 for errors
   or 0 for EOF.  */
ssize_t libc_pread64 (int __fd, void *__buf, size_t __nbytes,
			__off64_t __offset) {
    swrap_bind_symbol_libc (pread64);
    return swrap.libc.symbols._libc_pread64.f (__fd, __buf, __nbytes, __offset); 
}

/* Read data from file descriptor FD at the given position OFFSET
   without change the file pointer, and put the result in the buffers
   described by IOVEC, which is a vector of COUNT 'struct iovec's.
   The buffers are filled in the order specified.  Operates just like
   'pread' (see <unistd.h>) except that data are put in IOVEC instead
   of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_preadv (int __fd, const struct iovec *__iovec, int __count,
		       __off_t __offset) {
    swrap_bind_symbol_libc (preadv);
    return swrap.libc.symbols._libc_preadv.f (__fd, __iovec, __count, __offset); 
}

/* Same as preadv but with an additional flag argumenti defined at uio.h.  */
ssize_t libc_preadv2 (int __fp, const struct iovec *__iovec, int __count,
			__off_t __offset, int ___flags) {
    swrap_bind_symbol_libc (preadv2);
    return swrap.libc.symbols._libc_preadv2.f (__fp, __iovec, __count, __offset, ___flags); 
}

/* Write N bytes of BUF to FD at the given position OFFSET without
   changing the file pointer.  Return the number written, or -1.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_pwrite (int __fd, const void *__buf, size_t __n,
		       __off_t __offset) {
    swrap_bind_symbol_libc (pwrite);
    return swrap.libc.symbols._libc_pwrite.f (__fd, __buf, __n, __offset); 
}

/* Write N bytes of BUF to FD at the given position OFFSET without
   changing the file pointer.  Return the number written, or -1.  */
ssize_t libc_pwrite64 (int __fd, const void *__buf, size_t __n,
			 __off64_t __offset) {
    swrap_bind_symbol_libc (pwrite64);
    return swrap.libc.symbols._libc_pwrite64.f (__fd, __buf, __n, __offset); 
}
    
/* Write data pointed by the buffers described by IOVEC, which is a
   vector of COUNT 'struct iovec's, to file descriptor FD at the given
   position OFFSET without change the file pointer.  The data is
   written in the order specified.  Operates just like 'pwrite' (see
   <unistd.h>) except that the data are taken from IOVEC instead of a
   contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_pwritev (int __fd, const struct iovec *__iovec, int __count,
			__off_t __offset) {
    swrap_bind_symbol_libc (pwritev);
    return swrap.libc.symbols._libc_pwritev.f (__fd, __iovec, __count, __offset); 
}

/* Same as preadv but with an additional flag argument defined at uio.h.  */
ssize_t libc_pwritev2 (int __fd, const struct iovec *__iodev, int __count,
			 __off_t __offset, int __flags) {
    swrap_bind_symbol_libc (pwritev2);
    return swrap.libc.symbols._libc_pwritev2.f (__fd, __iodev, __count, __offset, __flags); 
}

/* Read NBYTES into BUF from FD.  Return the
   number read, -1 for errors or 0 for EOF.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_read (int __fd, void *__buf, size_t __nbytes) {
    swrap_bind_symbol_libc (read);
    return swrap.libc.symbols._libc_read.f (__fd, __buf, __nbytes); 
}

/* Provide kernel hint to read ahead.  */
__ssize_t libc_readahead (int __fd, __off64_t __offset, size_t __count) {
    swrap_bind_symbol_libc (readahead);
    return swrap.libc.symbols._libc_readahead.f (__fd, __offset, __count); 
}

/* Read a directory entry from DIRP.  Return a pointer to a `struct
   dirent' describing the entry, or NULL for EOF or error.  The
   storage returned may be overwritten by a later readdir call on the
   same DIR stream.

   If the Large File Support API is selected we have to use the
   appropriate interface.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
struct dirent *libc_readdir (DIR *__dirp) {
    swrap_bind_symbol_libc (readdir);
    return swrap.libc.symbols._libc_readdir.f (__dirp); 
}

struct dirent64 *libc_readdir64 (DIR *__dirp) {
    swrap_bind_symbol_libc (readdir64);
    return swrap.libc.symbols._libc_readdir64.f (__dirp); 
}

/* Read the contents of the symbolic link PATH into no more than
   LEN bytes of BUF.  The contents are not null-terminated.
   Returns the number of characters read, or -1 for errors.  */
ssize_t libc_readlink (const char *__restrict __path,
			 char *__restrict __buf, size_t __len) {
    swrap_bind_symbol_libc (readlink);
    return swrap.libc.symbols._libc_readlink.f (__path, __buf, __len); 
}

/* Like readlink but a relative PATH is interpreted relative to FD.  */
ssize_t libc_readlinkat (int __fd, const char *__restrict __path,
			   char *__restrict __buf, size_t __len) {
    swrap_bind_symbol_libc (readlinkat);
    return swrap.libc.symbols._libc_readlinkat.f (__fd, __path, __buf, __len); 
}

/* Read data from file descriptor FD, and put the result in the
   buffers described by IOVEC, which is a vector of COUNT 'struct iovec's.
   The buffers are filled in the order specified.
   Operates just like 'read' (see <unistd.h>) except that data are
   put in IOVEC instead of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_readv (int __fd, const struct iovec *__iovec, int __count) {
    swrap_bind_symbol_libc (readv);
    return swrap.libc.symbols._libc_readv.f (__fd, __iovec, __count); 
}

/* Remap arbitrary pages of a shared backing store within an existing
   VMA.  */
int libc_remap_file_pages (void *__start, size_t __size, int __prot,
			     size_t __pgoff, int __flags) {
    swrap_bind_symbol_libc (remap_file_pages);
    return swrap.libc.symbols._libc_remap_file_pages.f (__start, __size, __prot, __pgoff, __flags); 
}

/* Remove the attribute NAME from the file pointed to by PATH.  Return 0
   on success, -1 for errors.  */
int libc_removexattr (const char *__path, const char *__name) {
    swrap_bind_symbol_libc (removexattr);
    return swrap.libc.symbols._libc_removexattr.f (__path, __name); 
}

/* Rename file OLD to NEW.  */
int libc_rename (const char *__old, const char *__new) {
    swrap_bind_symbol_libc (rename);
    return swrap.libc.symbols._libc_rename.f (__old, __new); 
}

/* Rename file OLD relative to OLDFD to NEW relative to NEWFD.  */
int libc_renameat (int __oldfd, const char *__old, int __newfd,
		     const char *__new) {
    swrap_bind_symbol_libc (renameat);
    return swrap.libc.symbols._libc_renameat.f (__oldfd, __old, __newfd, __new); 
}

/* Rename file OLD relative to OLDFD to NEW relative to NEWFD, with
   additional flags.  */
int libc_renameat2 (int __oldfd, const char *__old, int __newfd,
		      const char *__new, unsigned int __flags) {
    swrap_bind_symbol_libc (renameat2);
    return swrap.libc.symbols._libc_renameat2.f (__oldfd, __old, __newfd, __new, __flags); 
}

/* Remove the directory PATH.  */
int libc_rmdir (const char *__path) {
    swrap_bind_symbol_libc (rmdir);
    return swrap.libc.symbols._libc_rmdir.f (__path); 
}

/* Check the first NFDS descriptors each in READFDS (if not NULL) for read
   readiness, in WRITEFDS (if not NULL) for write readiness, and in EXCEPTFDS
   (if not NULL) for exceptional conditions.  If TIMEOUT is not NULL, time out
   after waiting the interval specified therein.  Returns the number of ready
   descriptors, or -1 for errors.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_select (int __nfds, fd_set *__restrict __readfds,
		   fd_set *__restrict __writefds,
		   fd_set *__restrict __exceptfds,
		   struct timeval *__restrict __timeout) {
    swrap_bind_symbol_libc (select);
    return swrap.libc.symbols._libc_select.f (__nfds, __readfds, __writefds, __exceptfds, __timeout); 
}

/* Send up to COUNT bytes from file associated with IN_FD starting at
   *OFFSET to descriptor OUT_FD.  Set *OFFSET to the IN_FD's file position
   following the read bytes.  If OFFSET is a null pointer, use the normal
   file position instead.  Return the number of written bytes, or -1 in
   case of error.  */
ssize_t libc_sendfile (int __out_fd, int __in_fd, off_t *__offset,
			 size_t __count) {
    swrap_bind_symbol_libc (sendfile);
    return swrap.libc.symbols._libc_sendfile.f (__out_fd, __in_fd, __offset, __count); 
}

ssize_t libc_sendfile64 (int __out_fd, int __in_fd, __off64_t *__offset,
			   size_t __count) {
    swrap_bind_symbol_libc (sendfile64);
    return swrap.libc.symbols._libc_sendfile64.f (__out_fd, __in_fd, __offset, __count); 
}

/* Set the attribute NAME of the file pointed to by PATH to VALUE (which
   is SIZE bytes long).  Return 0 on success, -1 for errors.  */
int libc_setxattr (const char *__path, const char *__name,
		     const void *__value, size_t __size, int __flags) {
    swrap_bind_symbol_libc (setxattr);
    return swrap.libc.symbols._libc_setxattr.f (__path, __name, __value, __size, __flags); 
}

/* Get file attributes for FILE and put them in BUF.  */
int libc_stat (const char *__restrict __file,
		 struct stat *__restrict __buf) {
    swrap_bind_symbol_libc (stat);
    return swrap.libc.symbols._libc_stat.f (__file, __buf); 
}

int libc_stat64 (const char *__restrict __file,
		   struct stat64 *__restrict __buf) {
    swrap_bind_symbol_libc (stat64);
    return swrap.libc.symbols._libc_stat64.f (__file, __buf); 
}

int libc_statfs (const char *__file, struct statfs *__buf) {
    swrap_bind_symbol_libc (statfs);
    return swrap.libc.symbols._libc_statfs.f (__file, __buf); 
}

int libc_statfs64 (const char *__file, struct statfs64 *__buf) {
    swrap_bind_symbol_libc (statfs64);
    return swrap.libc.symbols._libc_statfs64.f (__file, __buf); 
}

/* Fill *BUF with information about PATH in DIRFD.  */
int libc_statx (int __dirfd, const char *__restrict __path, int __flags,
           unsigned int __mask, struct statx *__restrict __buf) {
    swrap_bind_symbol_libc (statx);
    return swrap.libc.symbols._libc_statx.f (__dirfd, __path, __flags, __mask, __buf); 
}

/* Make a symbolic link to FROM named TO.  */
int libc_symlink (const char *__from, const char *__to) {
    swrap_bind_symbol_libc (symlink);
    return swrap.libc.symbols._libc_symlink.f (__from, __to); 
}

/* Like symlink but a relative path in TO is interpreted relative to TOFD.  */
int libc_symlinkat (const char *__from, int __tofd,
		      const char *__to) {
    swrap_bind_symbol_libc (symlinkat);
    return swrap.libc.symbols._libc_symlinkat.f (__from, __tofd, __to); 
}

/* Make all changes done to all files actually appear on disk.  */
void libc_sync (void) {
    swrap_bind_symbol_libc (sync);
    return swrap.libc.symbols._libc_sync.f (); 
}

/* Selective file content synch'ing.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int libc_sync_file_range (int __fd, __off64_t __offset, __off64_t __count,
			    unsigned int __flags) {
    swrap_bind_symbol_libc (sync_file_range);
    return swrap.libc.symbols._libc_sync_file_range.f (__fd, __offset, __count, __flags); 
}

/* Make all changes done to all files on the file system associated
   with FD actually appear on disk.  */
int libc_syncfs (int __fd) {
    swrap_bind_symbol_libc (syncfs);
    return swrap.libc.symbols._libc_syncfs.f (__fd); 
}

/* Truncate FILE to LENGTH bytes.  */
int libc_truncate (const char *__file, __off_t __length) {
    swrap_bind_symbol_libc (truncate);
    return swrap.libc.symbols._libc_truncate.f (__file, __length); 
}

int libc_truncate64 (const char *__file, __off64_t __length) {
    swrap_bind_symbol_libc (truncate64);
    return swrap.libc.symbols._libc_truncate64.f (__file, __length); 
}

/* Unmount a filesystem.  */
int libc_umount (const char *__special_file) {
    swrap_bind_symbol_libc (umount);
    return swrap.libc.symbols._libc_umount.f (__special_file); 
}

/* Unmount a filesystem.  Force unmounting if FLAGS is set to MNT_FORCE.  */
int libc_umount2 (const char *__special_file, int __flags) {
    swrap_bind_symbol_libc (umount2);
    return swrap.libc.symbols._libc_umount2.f (__special_file, __flags); 
}

/* Remove the link NAME.  */
int libc_unlink (const char *__name) {
    swrap_bind_symbol_libc (unlink);
    return swrap.libc.symbols._libc_unlink.f (__name); 
}

/* Remove the link NAME relative to FD.  */
int libc_unlinkat (int __fd, const char *__name, int __flag) {
    swrap_bind_symbol_libc (unlinkat);
    return swrap.libc.symbols._libc_unlinkat.f (__fd, __name, __flag); 
}

/* Set the access and modification times of FILE to those given in
   *FILE_TIMES.  If FILE_TIMES is NULL, set them to the current time.  */
int libc_utime (const char *__file,
		  const struct utimbuf *__file_times) {
    swrap_bind_symbol_libc (utime);
    return swrap.libc.symbols._libc_utime.f (__file, __file_times); 
}


/* Set file access and modification times relative to directory file
   descriptor.  */
int libc_utimensat (int __fd, const char *__path,
		      const struct timespec __times[2],
		      int __flags) {
    swrap_bind_symbol_libc (utimensat);
    return swrap.libc.symbols._libc_utimensat.f (__fd, __path, __times, __flags); 
}

/* Change the access time of FILE to TVP[0] and the modification time of
   FILE to TVP[1].  If TVP is a null pointer, use the current time instead.
   Returns 0 on success, -1 on errors.  */
int libc_utimes (const char *__file, const struct timeval __tvp[2]) {
    swrap_bind_symbol_libc (utimes);
    return swrap.libc.symbols._libc_utimes.f (__file, __tvp); 
}

/* Write N bytes of BUF to FD.  Return the number written, or -1.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_write (int __fd, const void *__buf, size_t __n) {
    swrap_bind_symbol_libc (write);
    return swrap.libc.symbols._libc_write.f (__fd, __buf, __n); 
}

/* Write data pointed by the buffers described by IOVEC, which
   is a vector of COUNT 'struct iovec's, to file descriptor FD.
   The data is written in the order specified.
   Operates just like 'write' (see <unistd.h>) except that the data
   are taken from IOVEC instead of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_writev (int __fd, const struct iovec *__iovec, int __count) {
    swrap_bind_symbol_libc (writev);
    return swrap.libc.symbols._libc_writev.f (__fd, __iovec, __count); 
}
