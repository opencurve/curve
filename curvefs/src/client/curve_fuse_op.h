/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Project: curve
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */
#ifndef CURVEFS_SRC_CLIENT_CURVE_FUSE_OP_H_
#define CURVEFS_SRC_CLIENT_CURVE_FUSE_OP_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include "curvefs/src/client/fuse_common.h"

#ifdef __cplusplus
extern "C" {
#endif

int InitLog(const char *confPath, const char *argv0);

int InitFuseClient(const struct MountOption *mountOption);

void UnInitFuseClient();

/**
 * Initialize filesystem
 *
 * This function is called when libfuse establishes
 * communication with the FUSE kernel module. The file system
 * should use this module to inspect and/or modify the
 * connection parameters provided in the `conn` structure.
 *
 * Note that some parameters may be overwritten by options
 * passed to fuse_session_new() which take precedence over the
 * values set in this handler.
 *
 * There's no reply to this function
 *
 * @param userdata the user data passed to fuse_session_new()
 */
void FuseOpInit(void *userdata, struct fuse_conn_info *conn);

/**
 * Clean up filesystem.
 *
 * Called on filesystem exit. When this method is called, the
 * connection to the kernel may be gone already, so that eg. calls
 * to fuse_lowlevel_notify_* will fail.
 *
 * There's no reply to this function
 *
 * @param userdata the user data passed to fuse_session_new()
 */
void FuseOpDestroy(void *userdata);

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
void FuseOpLookup(fuse_req_t req, fuse_ino_t parent, const char *name);

/**
 * Forget about an inode
 *
 * This function is called when the kernel removes an inode
 * from its internal caches.
 *
 * The inode's lookup count increases by one for every call to
 * fuse_reply_entry and fuse_reply_create. The nlookup parameter
 * indicates by how much the lookup count should be decreased.
 *
 * Inodes with a non-zero lookup count may receive request from
 * the kernel even after calls to unlink, rmdir or (when
 * overwriting an existing file) rename. Filesystems must handle
 * such requests properly and it is recommended to defer removal
 * of the inode until the lookup count reaches zero. Calls to
 * unlink, rmdir or rename will be followed closely by forget
 * unless the file or directory is open, in which case the
 * kernel issues forget only after the release or releasedir
 * calls.
 *
 * Note that if a file system will be exported over NFS the
 * inodes lifetime must extend even beyond forget. See the
 * generation field in struct fuse_entry_param above.
 *
 * On unmount the lookup count for all inodes implicitly drops
 * to zero. It is not guaranteed that the file system will
 * receive corresponding forget messages for the affected
 * inodes.
 *
 * Valid replies:
 *   fuse_reply_none
 *
 * @param req request handle
 * @param ino the inode number
 * @param nlookup the number of lookups to forget
 */
void FuseOpForget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup);

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
void FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi);

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
void FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
         int to_set, struct fuse_file_info *fi);

/**
 * Read symbolic link
 *
 * Valid replies:
 *   fuse_reply_readlink
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 */
void FuseOpReadLink(fuse_req_t req, fuse_ino_t ino);

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
void FuseOpMkNod(fuse_req_t req, fuse_ino_t parent, const char *name,
           mode_t mode, dev_t rdev);

/**
 * Create a directory
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode with which to create the new file
 */
void FuseOpMkDir(fuse_req_t req, fuse_ino_t parent, const char *name,
           mode_t mode);

/**
 * Remove a file
 *
 * If the file's inode's lookup count is non-zero, the file
 * system is expected to postpone any removal of the inode
 * until the lookup count reaches zero (see description of the
 * forget function).
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to remove
 */
void FuseOpUnlink(fuse_req_t req, fuse_ino_t parent, const char *name);

/**
 * Remove a directory
 *
 * If the directory's inode's lookup count is non-zero, the
 * file system is expected to postpone any removal of the
 * inode until the lookup count reaches zero (see description
 * of the forget function).
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to remove
 */
void FuseOpRmDir(fuse_req_t req, fuse_ino_t parent, const char *name);

/**
 * Create a symbolic link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param link the contents of the symbolic link
 * @param parent inode number of the parent directory
 * @param name to create
 */
void FuseOpSymlink(fuse_req_t req, const char *link, fuse_ino_t parent,
         const char *name);

/** Rename a file
 *
 * If the target exists it should be atomically replaced. If
 * the target's inode's lookup count is non-zero, the file
 * system is expected to postpone any removal of the inode
 * until the lookup count reaches zero (see description of the
 * forget function).
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EINVAL, i.e. all
 * future bmap requests will fail with EINVAL without being
 * send to the filesystem process.
 *
 * *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
 * RENAME_NOREPLACE is specified, the filesystem must not
 * overwrite *newname* if it exists and return an error
 * instead. If `RENAME_EXCHANGE` is specified, the filesystem
 * must atomically exchange the two files, i.e. both must
 * exist and neither may be deleted.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the old parent directory
 * @param name old name
 * @param newparent inode number of the new parent directory
 * @param newname new name
 */
void FuseOpRename(fuse_req_t req, fuse_ino_t parent, const char *name,
        fuse_ino_t newparent, const char *newname,
        unsigned int flags);

/**
 * Create a hard link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the old inode number
 * @param newparent inode number of the new parent directory
 * @param newname new name to create
 */
void FuseOpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
          const char *newname);

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
void FuseOpOpen(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi);

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
void FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
          struct fuse_file_info *fi);

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
void FuseOpWrite(fuse_req_t req, fuse_ino_t ino, const char *buf,
           size_t size, off_t off, struct fuse_file_info *fi);

/**
 * Flush method
 *
 * This is called on each close() of the opened file.
 *
 * Since file descriptors can be duplicated (dup, dup2, fork), for
 * one open call there may be many flush calls.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * NOTE: the name of the method is misleading, since (unlike
 * fsync) the filesystem is not forced to flush pending writes.
 * One reason to flush data is if the filesystem wants to return
 * write errors during close.  However, such use is non-portable
 * because POSIX does not require [close] to wait for delayed I/O to
 * complete.
 *
 * If the filesystem supports file locking operations (setlk,
 * getlk) it should remove all locks belonging to 'fi->owner'.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to flush() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 *
 * [close]: http://pubs.opengroup.org/onlinepubs/9699919799/functions/close.html
 */
void FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
           struct fuse_file_info *fi);

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
void FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi);

/**
 * Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to fsync() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */
void FuseOpFsync(fuse_req_t req, fuse_ino_t ino, int datasync,
           struct fuse_file_info *fi);

/**
 * Open a directory
 *
 * Filesystem may store an arbitrary file handle (pointer, index,
 * etc) in fi->fh, and use this in other all other directory
 * stream operations (readdir, releasedir, fsyncdir).
 *
 * If this request is answered with an error code of ENOSYS and
 * FUSE_CAP_NO_OPENDIR_SUPPORT is set in `fuse_conn_info.capable`,
 * this is treated as success and future calls to opendir and
 * releasedir will also succeed without being sent to the filesystem
 * process. In addition, the kernel will cache readdir results
 * as if opendir returned FOPEN_KEEP_CACHE | FOPEN_CACHE_DIR.
 *
 * Valid replies:
 *   fuse_reply_open
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
void FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
         struct fuse_file_info *fi);

/**
 * Read directory
 *
 * Send a buffer filled using fuse_add_direntry(), with size not
 * exceeding the requested size.  Send an empty buffer on end of
 * stream.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * Returning a directory entry from readdir() does not affect
 * its lookup count.
 *
     * If off_t is non-zero, then it will correspond to one of the off_t
 * values that was previously returned by readdir() for the same
 * directory handle. In this case, readdir() should skip over entries
 * coming before the position defined by the off_t value. If entries
 * are added or removed while the directory handle is open, the filesystem
 * may still include the entries that have been removed, and may not
 * report the entries that have been created. However, addition or
 * removal of entries must never cause readdir() to skip over unrelated
 * entries or to report them more than once. This means
 * that off_t can not be a simple index that enumerates the entries
 * that have been returned but must contain sufficient information to
 * uniquely determine the next directory entry to return even when the
 * set of entries is changing.
 *
 * The function does not have to report the '.' and '..'
 * entries, but is allowed to do so. Note that, if readdir does
 * not return '.' or '..', they will not be implicitly returned,
 * and this behavior is observable by the caller.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum number of bytes to send
 * @param off offset to continue reading the directory stream
 * @param fi file information
 */
void FuseOpReadDir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
         struct fuse_file_info *fi);

/**
 * Release an open directory
 *
 * For every opendir call there will be exactly one releasedir
 * call (unless the filesystem is force-unmounted).
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */
void FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
            struct fuse_file_info *fi);

/**
 * Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the directory
 * contents should be flushed, not the meta data.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to fsyncdir() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */
void FuseOpFsyncDir(fuse_req_t req, fuse_ino_t ino, int datasync,
          struct fuse_file_info *fi);

/**
 * Get file system statistics
 *
 * Valid replies:
 *   fuse_reply_statfs
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number, zero means "undefined"
 */
void FuseOpStatFs(fuse_req_t req, fuse_ino_t ino);

/**
 * Set an extended attribute
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future setxattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 */
void FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino, const char *name,
          const char *value, size_t size, int flags);

/**
 * Get an extended attribute
 *
 * If size is zero, the size of the value should be sent with
 * fuse_reply_xattr.
 *
 * If the size is non-zero, and the value fits in the buffer, the
 * value should be sent with fuse_reply_buf.
 *
 * If the size is too small for the value, the ERANGE error should
 * be sent.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future getxattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_xattr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param name of the extended attribute
 * @param size maximum size of the value to send
 */
void FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino, const char *name,
          size_t size);

/**
 * List extended attribute names
 *
 * If size is zero, the total size of the attribute list should be
 * sent with fuse_reply_xattr.
 *
 * If the size is non-zero, and the null character separated
 * attribute list fits in the buffer, the list should be sent with
 * fuse_reply_buf.
 *
 * If the size is too small for the list, the ERANGE error should
 * be sent.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future listxattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_xattr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum size of the list to send
 */
void FuseOpListXattr(fuse_req_t req, fuse_ino_t ino, size_t size);

/**
 * Remove an extended attribute
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future removexattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param name of the extended attribute
 */
void FuseOpRemoveXattr(fuse_req_t req, fuse_ino_t ino, const char *name);

/**
 * Check file access permissions
 *
 * This will be called for the access() and chdir() system
 * calls.  If the 'default_permissions' mount option is given,
 * this method is not called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent success, i.e. this and all future access()
 * requests will succeed without being send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param mask requested access mode
 */
void FuseOpAccess(fuse_req_t req, fuse_ino_t ino, int mask);

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * See the description of the open handler for more
 * information.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * If this request is answered with an error code of ENOSYS, the handler
 * is treated as not implemented (i.e., for this and future requests the
 * mknod() and open() handlers will be called instead).
 *
 * Valid replies:
 *   fuse_reply_create
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode file type and mode with which to create the new file
 * @param fi file information
 */
void FuseOpCreate(fuse_req_t req, fuse_ino_t parent, const char *name,
        mode_t mode, struct fuse_file_info *fi);

/**
 * Test for a POSIX file lock
 *
 * Valid replies:
 *   fuse_reply_lock
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param lock the region/type to test
 */
void FuseOpGetLk(fuse_req_t req, fuse_ino_t ino,
           struct fuse_file_info *fi, struct flock *lock);

/**
 * Acquire, modify or release a POSIX file lock
 *
 * For POSIX threads (NPTL) there's a 1-1 relation between pid and
 * owner, but otherwise this is not always the case.  For checking
 * lock ownership, 'fi->owner' must be used.  The l_pid field in
 * 'struct flock' should only be used to fill in this field in
 * getlk().
 *
 * Note: if the locking methods are not implemented, the kernel
 * will still allow file locking to work locally.  Hence these are
 * only interesting for network filesystems and similar.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param lock the region/type to set
 * @param sleep locking operation may sleep
 */
void FuseOpSetLk(fuse_req_t req, fuse_ino_t ino,
           struct fuse_file_info *fi,
           struct flock *lock, int sleep);

/**
 * Map block index within file to block index within device
 *
 * Note: This makes sense only for block device backed filesystems
 * mounted with the 'blkdev' option
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure, i.e. all future bmap() requests will
 * fail with the same error code without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_bmap
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param blocksize unit of block index
 * @param idx block index within file
 */
void FuseOpBmap(fuse_req_t req, fuse_ino_t ino, size_t blocksize,
          uint64_t idx);

/**
 * Ioctl
 *
 * Note: For unrestricted ioctls (not allowed for FUSE
 * servers), data in and out areas can be discovered by giving
 * iovs and setting FUSE_IOCTL_RETRY in *flags*.  For
 * restricted ioctls, kernel prepares in/out data area
 * according to the information encoded in cmd.
 *
 * Valid replies:
 *   fuse_reply_ioctl_retry
 *   fuse_reply_ioctl
 *   fuse_reply_ioctl_iov
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param cmd ioctl command
 * @param arg ioctl argument
 * @param fi file information
 * @param flags for FUSE_IOCTL_* flags
 * @param in_buf data fetched from the caller
 * @param in_bufsz number of fetched bytes
 * @param out_bufsz maximum size of output data
 *
 * Note : the unsigned long request submitted by the application
 * is truncated to 32 bits.
 */
#if FUSE_USE_VERSION < 35
void FuseOpIoctl(fuse_req_t req, fuse_ino_t ino, int cmd,
           void *arg, struct fuse_file_info *fi, unsigned flags,
           const void *in_buf, size_t in_bufsz, size_t out_bufsz);
#else
void FuseOpIoctl(fuse_req_t req, fuse_ino_t ino, unsigned int cmd,
           void *arg, struct fuse_file_info *fi, unsigned flags,
           const void *in_buf, size_t in_bufsz, size_t out_bufsz);
#endif

/**
 * Poll for IO readiness
 *
 * Note: If ph is non-NULL, the client should notify
 * when IO readiness events occur by calling
 * fuse_lowlevel_notify_poll() with the specified ph.
 *
 * Regardless of the number of times poll with a non-NULL ph
 * is received, single notification is enough to clear all.
 * Notifying more times incurs overhead but doesn't harm
 * correctness.
 *
 * The callee is responsible for destroying ph with
 * fuse_pollhandle_destroy() when no longer in use.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as success (with a kernel-defined default poll-mask) and
 * future calls to pull() will succeed the same way without being send
 * to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_poll
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param ph poll handle to be used for notification
 */
void FuseOpPoll(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi,
          struct fuse_pollhandle *ph);

/**
 * Write data made available in a buffer
 *
 * This is a more generic version of the ->write() method.  If
 * FUSE_CAP_SPLICE_READ is set in fuse_conn_info.want and the
 * kernel supports splicing from the fuse device, then the
 * data will be made available in pipe for supporting zero
 * copy data transfer.
 *
 * buf->count is guaranteed to be one (and thus buf->idx is
 * always zero). The write_buf handler must ensure that
 * bufv->off is correctly updated (reflecting the number of
 * bytes read from bufv->buf[0]).
 *
 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
 * expected to reset the setuid and setgid bits.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param bufv buffer containing the data
 * @param off offset to write to
 * @param fi file information
 */
void FuseOpWriteBuf(fuse_req_t req, fuse_ino_t ino,
           struct fuse_bufvec *bufv, off_t off,
           struct fuse_file_info *fi);

/**
 * Callback function for the retrieve request
 *
 * Valid replies:
 *	fuse_reply_none
 *
 * @param req request handle
 * @param cookie user data supplied to fuse_lowlevel_notify_retrieve()
 * @param ino the inode number supplied to fuse_lowlevel_notify_retrieve()
 * @param offset the offset supplied to fuse_lowlevel_notify_retrieve()
 * @param bufv the buffer containing the returned data
 */
void FuseOpRetrieveReply(fuse_req_t req, void *cookie, fuse_ino_t ino,
            off_t offset, struct fuse_bufvec *bufv);

/**
 * Forget about multiple inodes
 *
 * See description of the forget function for more
 * information.
 *
 * Valid replies:
 *   fuse_reply_none
 *
 * @param req request handle
 */
void FuseOpForgetMulti(fuse_req_t req, size_t count,
              struct fuse_forget_data *forgets);

/**
 * Acquire, modify or release a BSD file lock
 *
 * Note: if the locking methods are not implemented, the kernel
 * will still allow file locking to work locally.  Hence these are
 * only interesting for network filesystems and similar.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param op the locking operation, see flock(2)
 */
void FuseOpFlock(fuse_req_t req, fuse_ino_t ino,
           struct fuse_file_info *fi, int op);

/**
 * Allocate requested space. If this function returns success then
 * subsequent writes to the specified range shall not fail due to the lack
 * of free space on the file system storage media.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future fallocate() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param offset starting point for allocated region
 * @param length size of allocated region
 * @param mode determines the operation to be performed on the given range,
 *             see fallocate(2)
 */
void FuseOpFallocate(fuse_req_t req, fuse_ino_t ino, int mode,
           off_t offset, off_t length, struct fuse_file_info *fi);

/**
 * Read directory with attributes
 *
 * Send a buffer filled using fuse_add_direntry_plus(), with size not
 * exceeding the requested size.  Send an empty buffer on end of
 * stream.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * In contrast to readdir() (which does not affect the lookup counts),
 * the lookup count of every entry returned by readdirplus(), except "."
 * and "..", is incremented by one.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum number of bytes to send
 * @param off offset to continue reading the directory stream
 * @param fi file information
 */
void FuseOpReadDirPlus(
        fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
        struct fuse_file_info *fi);

/**
 * Copy a range of data from one file to another
 *
 * Performs an optimized copy between two file descriptors without the
 * additional cost of transferring data through the FUSE kernel module
 * to user space (glibc) and then back into the FUSE filesystem again.
 *
 * In case this method is not implemented, glibc falls back to reading
 * data from the source and writing to the destination. Effectively
 * doing an inefficient copy of the data.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future copy_file_range() requests will fail with EOPNOTSUPP without
 * being send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino_in the inode number or the source file
 * @param off_in starting point from were the data should be read
 * @param fi_in file information of the source file
 * @param ino_out the inode number or the destination file
 * @param off_out starting point where the data should be written
 * @param fi_out file information of the destination file
 * @param len maximum size of the data to copy
 * @param flags passed along with the copy_file_range() syscall
 */
void FuseOpCopyFileRange(fuse_req_t req, fuse_ino_t ino_in,
             off_t off_in, struct fuse_file_info *fi_in,
             fuse_ino_t ino_out, off_t off_out,
             struct fuse_file_info *fi_out, size_t len,
             int flags);

/**
 * Find next data or hole after the specified offset
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure, i.e. all future lseek() requests will
 * fail with the same error code without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_lseek
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param off offset to start search from
 * @param whence either SEEK_DATA or SEEK_HOLE
 * @param fi file information
 */
void FuseOpLseek(fuse_req_t req, fuse_ino_t ino, off_t off, int whence,
           struct fuse_file_info *fi);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // CURVEFS_SRC_CLIENT_CURVE_FUSE_OP_H_
