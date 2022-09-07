#ifndef LDP_CURVEFS_WRAPPER_H
#define LDP_CURVEFS_WRAPPER_H

#include <stdio.h>

/* Test for access to NAME using the real UID and real GID. */
int libc_access (const char * __name, int __type);

/* Turn accounting on if NAME is an existing file. The System will then write
   a record for each process as it terminates, to this file. If NAME is NULL, 
   turn accounting off. This call is restricted to the super-user.  */
int libc_acct (const char *__name);

/* Change the process's working directory to PATH.  */
int libc_chdir (const char *__path);

/* Set file access perminssions for FILE to MODE.
   If FILE is a symbolic link, this affects its target instead.  */
int libc_chmod (const char *__file, __mode_t __mode);

/* Change the owner and group of FILE.  */
int libc_chown (const char *__file, __uid_t __owner, __gid_t __group);

/* Make PATH be the root directory (the starting point for absolute paths).
   This call is restricted to the super-user.  */
int libc_chroot (const char *__path);

/* Close the file descriptor FD.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_close (int __fd);

/* Close all file descriptors in the range FD up to MAX_FD.  The flag FLAGS
   are define by the CLOSE_RANGE prefix.  This function behaves like close
   on the range and gaps where the file descriptor is invalid or errors
   encountered while closing file descriptors are ignored.   Returns 0 on
   successor or -1 for failure (and sets errno accordingly).  */
int libc_close_range (unsigned int __fd, unsigned int __max_fd,
			int __flags);

/* Copy LENGTH bytes from INFD to OUTFD.  */
ssize_t libc_copy_file_range (int __infd, __off64_t *__pinoff,
			 int __outfd, __off64_t *__poutoff,
			 size_t __length, unsigned int __flags);

/* Create and open FILE, with mode MODE.  This takes an `int' MODE
   argument because that is what `mode_t' will be widened to.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_creat (const char *__file, mode_t __mode);
int libc_creat64 (const char *__file, mode_t __mode);

/* Duplicate FD, returning a new file descriptor on the same file.  */
int libc_dup (int __fd);

/* Duplicate FD to FD2, closing FD2 and making it open on the same file.  */
int libc_dup2 (int __fd, int __fd2);

/* Duplicate FD to FD2, closing FD2 and making it open on the same
   file while setting flags according to FLAGS.  */
int libc_dup3 (int __fd, int __fd2, int __flags);

/* Test for access to FILE relative to the directory FD is open on.
   If AT_EACCESS is set in FLAG, then use effective IDs like `eaccess',
   otherwise use real IDs like `access'.  */
int libc_faccessat (int __fd, const char *__file, int __type, int __flag);

int libc_posix_fadvise64 (int __fd, off64_t __offset, off64_t __len,
			    int __advise);

/* Reserve storage for the data of the file associated with FD.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int libc_posix_fallocate (int __fd, off_t __offset, off_t __len);

int libc_posix_fallocate64 (int __fd, off64_t __offset, off64_t __len);

/* Reserve storage for the data of the file associated with FD.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int libc_fallocate (int __fd, int __mode, __off_t __offset, __off_t __len);

int libc_fallocate64 (int __fd, int __mode, __off64_t __offset,
			__off64_t __len);

/* Change the process's working directory to the one FD is open on.  */
int libc_fchdir (int __fd);

/* Set file access permissions of the file FD is open on to MODE.  */
int libc_fchmod (int __fd, __mode_t __mode);

/* Set file access permissions of FILE relative to
   the directory FD is open on.  */
int libc_fchmodat (int __fd, const char *__file, __mode_t __mode,
		     int __flag);

/* Change the owner and group of the file that FD is open on.  */
int libc_fchown (int __fd, __uid_t __owner, __gid_t __group);

/* Change the owner and group of FILE relative to the directory FD is open
   on.  */
int libc_fchownat (int __fd, const char *__file, __uid_t __owner,
		     __gid_t __group, int __flag);

/* Do the file control operation described by CMD on FD.
   The remaining arguments are interpreted depending on CMD.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_fcntl (int __fd, int __cmd, va_list ap);
int libc_fcntl64 (int __fd, int __cmd, va_list ap);

/* Synchronize at least the data part of a file with the underlying
   media.  */
int libc_fdatasync (int __fildes);

/* Get the attribute NAME of the file descriptor FD to VALUE (which is SIZE
   bytes long).  Return 0 on success, -1 for errors.  */
ssize_t libc_fgetxattr (int __fd, const char *__name, void *__value,
			  size_t __size);

/* List attributes of the file descriptor FD into the user-supplied buffer
   LIST (which is SIZE bytes big).  Return 0 on success, -1 for errors.  */
ssize_t libc_flistxattr (int __fd, char *__list, size_t __size);

/* Apply or remove an advisory lock, according to OPERATION,
   on the file FD refers to.  */
int libc_flock (int __fd, int __operation);

/* Remove the attribute NAME from the file descriptor FD.  Return 0 on
   success, -1 for errors.  */
int libc_fremovexattr (int __fd, const char *__name);

/* Set the attribute NAME of the file descriptor FD to VALUE (which is SIZE
   bytes long).  Return 0 on success, -1 for errors.  */
int libc_fsetxattr (int __fd, const char *__name, const void *__value,
		      size_t __size, int __flags);

/* Get file attributes for the file, device, pipe, or socket
   that file descriptor FD is open on and put them in BUF.  */
int libc_fstat (int __fd, struct stat *__buf);

int libc_fstat64 (int __fd, struct stat64 *__buf);

int libc_fstatat64 (int __fd, const char *__restrict __file,
		      struct stat64 *__restrict __buf, int __flag);

/* Return information about the filesystem containing the file FILDES
   refers to.  */
int libc_fstatfs (int __fildes, struct statfs *__buf);

int libc_fstatfs64 (int __fildes, struct statfs64 *__buf);

/* Make all changes done to FD actually appear on disk.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_fsync (int __fd);

/* Fill in TIMEBUF with information about the current time.  */

int libc_ftime (struct timeb *__timebuf);

/* Truncate the file FD is open on to LENGTH bytes.  */
int libc_ftruncate (int __fd, __off_t __length);

int libc_ftruncate64 (int __fd, __off64_t __length);

/* Same as `utimes', but takes an open file descriptor instead of a name.  */
int libc_futimes (int __fd, const struct timeval __tvp[2]);

/* Change the access time of FILE relative to FD to TVP[0] and the
   modification time of FILE to TVP[1].  If TVP is a null pointer, use
   the current time instead.  Returns 0 on success, -1 on errors.  */
int libc_futimesat (int __fd, const char *__file,
		      const struct timeval __tvp[2]);


/* Get the pathname of the current working directory,
   and put it in SIZE bytes of BUF.  Returns NULL if the
   directory couldn't be determined or SIZE was too small.
   If successful, returns BUF.  In GNU, if BUF is NULL,
   an array is allocated with `malloc'; the array is SIZE
   bytes long, unless SIZE == 0, in which case it is as
   big as necessary.  */
char *libc_getcwd (char *__buf, size_t __size);

/* Read from the directory descriptor FD into LENGTH bytes at BUFFER.
   Return the number of bytes read on success (0 for end of
   directory), and -1 for failure.  */
__ssize_t libc_getdents64 (int __fd, void *__buffer, size_t __length);

/* Get the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long).  Return 0 on success, -1 for errors.  */
ssize_t libc_getxattr (const char *__path, const char *__name,
			 void *__value, size_t __size);

/* Perform the I/O control operation specified by REQUEST on FD.
   One argument may follow; its presence and type depend on REQUEST.
   Return value depends on REQUEST.  Usually -1 indicates error.  */
int libc_ioctl (int __fd, unsigned long int __request, va_list ap);

/* Change owner and group of FILE, if it is a symbolic
   link the ownership of the symbolic link is changed.  */
int libc_lchown (const char *__file, __uid_t __owner, __gid_t __group);

/* Get the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long), not following symlinks for the last pathname component.
   Return 0 on success, -1 for errors.  */
ssize_t libc_lgetxattr (const char *__path, const char *__name,
			  void *__value, size_t __size);

/* Make a link to FROM named TO.  */
int libc_link (const char *__from, const char *__to);

/* Like link but relative paths in TO and FROM are interpreted relative
   to FROMFD and TOFD respectively.  */
int libc_linkat (int __fromfd, const char *__from, int __tofd,
		   const char *__to, int __flags);

/* List attributes of the file pointed to by PATH into the user-supplied
   buffer LIST (which is SIZE bytes big).  Return 0 on success, -1 for
   errors.  */
ssize_t libc_listxattr (const char *__path, char *__list, size_t __size);

/* List attributes of the file pointed to by PATH into the user-supplied
   buffer LIST (which is SIZE bytes big), not following symlinks for the
   last pathname component.  Return 0 on success, -1 for errors.  */
ssize_t libc_llistxattr (const char *__path, char *__list, size_t __size);

/* Remove the attribute NAME from the file pointed to by PATH, not
   following symlinks for the last pathname component.  Return 0 on
   success, -1 for errors.  */
int libc_lremovexattr (const char *__path, const char *__name);

/* Move FD's file position to OFFSET bytes from the
   beginning of the file (if WHENCE is SEEK_SET),
   the current position (if WHENCE is SEEK_CUR),
   or the end of the file (if WHENCE is SEEK_END).
   Return the new file position.  */
__off_t libc_lseek (int __fd, __off_t __offset, int __whence);

__off64_t libc_lseek64 (int __fd, __off64_t __offset, int __whence);

/* Set the attribute NAME of the file pointed to by PATH to VALUE (which is
   SIZE bytes long), not following symlinks for the last pathname component.
   Return 0 on success, -1 for errors.  */
int libc_lsetxattr (const char *__path, const char *__name,
		      const void *__value, size_t __size, int __flags);

/* Get file attributes about FILE and put them in BUF.
   If FILE is a symbolic link, do not follow it.  */
int libc_lstat (const char *__restrict __file,
		  struct stat *__restrict __buf);

int libc_lstat64 (const char *__restrict __file,
		    struct stat64 *__restrict __buf);

/* Advise the system about particular usage patterns the program follows
   for the region starting at ADDR and extending LEN bytes.  */
int libc_madvise (void *__addr, size_t __len, int __advice);

/* This is the POSIX name for this function.  */
int libc_posix_madvise (void *__addr, size_t __len, int __advice);

/* Create a new directory named PATH, with permission bits MODE.  */
int libc_mkdir (const char *__path, __mode_t __mode);

/* Like mkdir, create a new directory with permission bits MODE.  But
   interpret relative PATH names relative to the directory associated
   with FD.  */
int libc_mkdirat (int __fd, const char *__path, __mode_t __mode);

/* Create a device file named PATH, with permission and special bits MODE
   and device number DEV (which can be constructed from major and minor
   device numbers with the `makedev' macro above).  */
int libc_mknod (const char *__path, __mode_t __mode, __dev_t __dev);

/* Like mknod, create a new device file with permission bits MODE and
   device number DEV.  But interpret relative PATH names relative to
   the directory associated with FD.  */
int libc_mknodat (int __fd, const char *__path, __mode_t __mode,
		    __dev_t __dev);


/* Map addresses starting near ADDR and extending for LEN bytes.  from
   OFFSET into the file FD describes according to PROT and FLAGS.  If ADDR
   is nonzero, it is the desired mapping address.  If the MAP_FIXED bit is
   set in FLAGS, the mapping will be at ADDR exactly (which must be
   page-aligned); otherwise the system chooses a convenient nearby address.
   The return value is the actual mapping address chosen or MAP_FAILED
   for errors (in which case `errno' is set).  A successful `mmap' call
   deallocates any previous mapping for the affected region.  */

void *libc_mmap (void *__addr, size_t __len, int __prot,
		   int __flags, int __fd, __off_t __offset);

void *libc_mmap64 (void *__addr, size_t __len, int __prot,
		     int __flags, int __fd, __off64_t __offset);


/* Mount a filesystem.  */
int libc_mount (const char *__special_file, const char *__dir,
		  const char *__fstype, unsigned long int __rwflag,
		  const void *__data);

/* Remap pages mapped by the range [ADDR,ADDR+OLD_LEN) to new length
   NEW_LEN.  If MREMAP_MAYMOVE is set in FLAGS the returned address
   may differ from ADDR.  If MREMAP_FIXED is set in FLAGS the function
   takes another parameter which is a fixed address at which the block
   resides after a successful call.  */
void *libc_mremap (void *__addr, size_t __old_len, size_t __new_len,
		     int __flags, va_list ap);

/* Synchronize the region starting at ADDR and extending LEN bytes with the
   file it maps.  Filesystem operations on a file being mapped are
   unpredictable before this is done.  Flags are from the MS_* set.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_msync (void *__addr, size_t __len, int __flags);

/* Deallocate any mapping for the region starting at ADDR and extending LEN
   bytes.  Returns 0 if successful, -1 for errors (and sets errno).  */
int libc_munmap (void *__addr, size_t __len);

/* Open FILE and return a new file descriptor for it, or -1 on error.
   OFLAG determines the type of access used.  If O_CREAT or O_TMPFILE is set
   in OFLAG, the third argument is taken as a `mode_t', the mode of the
   created file.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_open (const char *__file, int __oflag, va_list ap);

int libc_open64 (const char *__file, int __oflag, va_list ap);

/* Similar to `open' but a relative path name is interpreted relative to
   the directory for which FD is a descriptor.

   NOTE: some other `openat' implementation support additional functionality
   through this interface, especially using the O_XATTR flag.  This is not
   yet supported here.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
int libc_openat (int __fd, const char *__file, int __oflag, va_list ap);

int libc_openat64 (int __fd, const char *__file, int __oflag, va_list ap);

/* Read NBYTES into BUF from FD at the given position OFFSET without
   changing the file pointer.  Return the number read, -1 for errors
   or 0 for EOF.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_pread (int __fd, void *__buf, size_t __nbytes,
		      __off_t __offset);

/* Read NBYTES into BUF from FD at the given position OFFSET without
   changing the file pointer.  Return the number read, -1 for errors
   or 0 for EOF.  */
ssize_t libc_pread64 (int __fd, void *__buf, size_t __nbytes,
			__off64_t __offset);

/* Read data from file descriptor FD at the given position OFFSET
   without change the file pointer, and put the result in the buffers
   described by IOVEC, which is a vector of COUNT 'struct iovec's.
   The buffers are filled in the order specified.  Operates just like
   'pread' (see <unistd.h>) except that data are put in IOVEC instead
   of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_preadv (int __fd, const struct iovec *__iovec, int __count,
		       __off_t __offset);

/* Same as preadv but with an additional flag argumenti defined at uio.h.  */
ssize_t libc_preadv2 (int __fp, const struct iovec *__iovec, int __count,
			__off_t __offset, int ___flags);

/* Write N bytes of BUF to FD at the given position OFFSET without
   changing the file pointer.  Return the number written, or -1.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_pwrite (int __fd, const void *__buf, size_t __n,
		       __off_t __offset);

/* Write N bytes of BUF to FD at the given position OFFSET without
   changing the file pointer.  Return the number written, or -1.  */
ssize_t libc_pwrite64 (int __fd, const void *__buf, size_t __n,
			 __off64_t __offset);
    
/* Write data pointed by the buffers described by IOVEC, which is a
   vector of COUNT 'struct iovec's, to file descriptor FD at the given
   position OFFSET without change the file pointer.  The data is
   written in the order specified.  Operates just like 'pwrite' (see
   <unistd.h>) except that the data are taken from IOVEC instead of a
   contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_pwritev (int __fd, const struct iovec *__iovec, int __count,
			__off_t __offset);

/* Same as preadv but with an additional flag argument defined at uio.h.  */
ssize_t libc_pwritev2 (int __fd, const struct iovec *__iodev, int __count,
			 __off_t __offset, int __flags);

/* Read NBYTES into BUF from FD.  Return the
   number read, -1 for errors or 0 for EOF.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_read (int __fd, void *__buf, size_t __nbytes);

/* Provide kernel hint to read ahead.  */
__ssize_t libc_readahead (int __fd, __off64_t __offset, size_t __count);

/* Read a directory entry from DIRP.  Return a pointer to a `struct
   dirent' describing the entry, or NULL for EOF or error.  The
   storage returned may be overwritten by a later readdir call on the
   same DIR stream.

   If the Large File Support API is selected we have to use the
   appropriate interface.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
struct dirent *libc_readdir (DIR *__dirp);

struct dirent64 *libc_readdir64 (DIR *__dirp);

/* Read the contents of the symbolic link PATH into no more than
   LEN bytes of BUF.  The contents are not null-terminated.
   Returns the number of characters read, or -1 for errors.  */
ssize_t libc_readlink (const char *__restrict __path,
			 char *__restrict __buf, size_t __len);

/* Like readlink but a relative PATH is interpreted relative to FD.  */
ssize_t libc_readlinkat (int __fd, const char *__restrict __path,
			   char *__restrict __buf, size_t __len);

/* Read data from file descriptor FD, and put the result in the
   buffers described by IOVEC, which is a vector of COUNT 'struct iovec's.
   The buffers are filled in the order specified.
   Operates just like 'read' (see <unistd.h>) except that data are
   put in IOVEC instead of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_readv (int __fd, const struct iovec *__iovec, int __count);

/* Remap arbitrary pages of a shared backing store within an existing
   VMA.  */
int libc_remap_file_pages (void *__start, size_t __size, int __prot,
			     size_t __pgoff, int __flags);

/* Remove the attribute NAME from the file pointed to by PATH.  Return 0
   on success, -1 for errors.  */
int libc_removexattr (const char *__path, const char *__name);

/* Rename file OLD to NEW.  */
int libc_rename (const char *__old, const char *__new);

/* Rename file OLD relative to OLDFD to NEW relative to NEWFD.  */
int libc_renameat (int __oldfd, const char *__old, int __newfd,
		     const char *__new);

/* Rename file OLD relative to OLDFD to NEW relative to NEWFD, with
   additional flags.  */
int libc_renameat2 (int __oldfd, const char *__old, int __newfd,
		      const char *__new, unsigned int __flags);

/* Remove the directory PATH.  */
int libc_rmdir (const char *__path);

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
		   struct timeval *__restrict __timeout);

/* Send up to COUNT bytes from file associated with IN_FD starting at
   *OFFSET to descriptor OUT_FD.  Set *OFFSET to the IN_FD's file position
   following the read bytes.  If OFFSET is a null pointer, use the normal
   file position instead.  Return the number of written bytes, or -1 in
   case of error.  */
ssize_t libc_sendfile (int __out_fd, int __in_fd, off_t *__offset,
			 size_t __count);

ssize_t libc_sendfile64 (int __out_fd, int __in_fd, __off64_t *__offset,
			   size_t __count);

/* Set the attribute NAME of the file pointed to by PATH to VALUE (which
   is SIZE bytes long).  Return 0 on success, -1 for errors.  */
int libc_setxattr (const char *__path, const char *__name,
		     const void *__value, size_t __size, int __flags);

/* Get file attributes for FILE and put them in BUF.  */
int libc_stat (const char *__restrict __file,
		 struct stat *__restrict __buf);

int libc_stat64 (const char *__restrict __file,
		   struct stat64 *__restrict __buf);

int libc_statfs (const char *__file, struct statfs *__buf);

int libc_statfs64 (const char *__file, struct statfs64 *__buf);

/* Fill *BUF with information about PATH in DIRFD.  */
int libc_statx (int __dirfd, const char *__restrict __path, int __flags,
           unsigned int __mask, struct statx *__restrict __buf);

/* Make a symbolic link to FROM named TO.  */
int libc_symlink (const char *__from, const char *__to);

/* Like symlink but a relative path in TO is interpreted relative to TOFD.  */
int libc_symlinkat (const char *__from, int __tofd,
		      const char *__to);

/* Make all changes done to all files actually appear on disk.  */
void libc_sync (void);

/* Selective file content synch'ing.

   This function is a possible cancellation point and therefore not
   marked with __THROW.  */
int libc_sync_file_range (int __fd, __off64_t __offset, __off64_t __count,
			    unsigned int __flags);

/* Make all changes done to all files on the file system associated
   with FD actually appear on disk.  */
int libc_syncfs (int __fd);

/* Truncate FILE to LENGTH bytes.  */
int libc_truncate (const char *__file, __off_t __length);

int libc_truncate64 (const char *__file, __off64_t __length);

/* Unmount a filesystem.  */
int libc_umount (const char *__special_file);

/* Unmount a filesystem.  Force unmounting if FLAGS is set to MNT_FORCE.  */
int libc_umount2 (const char *__special_file, int __flags);

/* Remove the link NAME.  */
int libc_unlink (const char *__name);

/* Remove the link NAME relative to FD.  */
int libc_unlinkat (int __fd, const char *__name, int __flag);

/* Set the access and modification times of FILE to those given in
   *FILE_TIMES.  If FILE_TIMES is NULL, set them to the current time.  */
int libc_utime (const char *__file,
		  const struct utimbuf *__file_times);


/* Set file access and modification times relative to directory file
   descriptor.  */
int libc_utimensat (int __fd, const char *__path,
		      const struct timespec __times[2],
		      int __flags);

/* Change the access time of FILE to TVP[0] and the modification time of
   FILE to TVP[1].  If TVP is a null pointer, use the current time instead.
   Returns 0 on success, -1 on errors.  */
int libc_utimes (const char *__file, const struct timeval __tvp[2]);

/* Write N bytes of BUF to FD.  Return the number written, or -1.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_write (int __fd, const void *__buf, size_t __n);

/* Write data pointed by the buffers described by IOVEC, which
   is a vector of COUNT 'struct iovec's, to file descriptor FD.
   The data is written in the order specified.
   Operates just like 'write' (see <unistd.h>) except that the data
   are taken from IOVEC instead of a contiguous buffer.

   This function is a cancellation point and therefore not marked with
   __THROW.  */
ssize_t libc_writev (int __fd, const struct iovec *__iovec, int __count);

#endif
