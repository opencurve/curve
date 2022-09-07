/*
 *    This Cplant(TM) source code is the property of Sandia National
 *    Laboratories.
 *
 *    This Cplant(TM) source code is copyrighted by Sandia National
 *    Laboratories.
 *
 *    The redistribution of this Cplant(TM) source code is subject to the
 *    terms of the GNU Lesser General Public License
 *    (see cit/LGPL or http://www.gnu.org/licenses/lgpl.html)
 *
 *    Cplant(TM) Copyright 1998-2009 Sandia Corporation. 
 *    Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive
 *    license for use of this work by or on behalf of the US Government.
 *    Export of this program may require a license from the United States
 *    Government.
 */

/*
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 * Questions or comments about this library should be sent to:
 *
 * Lee Ward
 * Sandia National Laboratories, New Mexico
 * P.O. Box 5800
 * Albuquerque, NM 87185-1319
 *
 * lee@sandia.gov
 */

#define _GNU_SOURCE
/* never used
#ifdef __linux__
#define _BSD_SOURCE
#endif
*/

#include <stdio.h>					/* for NULL */
#include <stdlib.h>
#ifdef __linux__
#include <string.h>
#endif
#include <unistd.h>
#if !(defined(REDSTORM) || defined(MAX_IOVEC))
#include <limits.h>
#endif
#include <errno.h>
#include <assert.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/statfs.h>
#ifdef _HAVE_STATVFS
#include <sys/statvfs.h>
#endif
#include <utime.h>
#include <sys/uio.h>
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"
#include "native.h"
#include "fs.h"
#include "mount.h"
#include "inode.h"

#include "fs_native.h"

#ifdef REDSTORM
#include <sys/uio.h>
#endif

#undef DIR_CVT_64
#if defined(SYSIO_SYS_getdirentries)
#elif defined(SYSIO_SYS_getdents64)
#elif defined(SYSIO_SYS_getdents)
#if defined(_LARGEFILE64_SOURCE)
#define DIR_CVT_64
/*
 * Kernel version of directory entry.
 */
struct linux_dirent {
	unsigned long ld_ino;
	unsigned long ld_off;
	unsigned short ld_reclen;
	char	ld_name[1];
};
#include <dirent.h>
#endif /* defined(_LARGEFILE64_SOURCE) */
#else /* catch-none */
#error No usable directory fill entries interface available
#endif

/*
 * Native file system information we keep per FS.
 */
struct native_filesystem {
	time_t	nfs_atimo;				/* attr timeout (sec) */
};

/*
 * Given fs, return driver private part.
 */
#define FS2NFS(fs) \
	((struct native_filesystem *)(fs)->fs_private)

/*
 * Native file identifiers format.
 */
struct native_inode_identifier {
	dev_t	dev;					/* device number */
	ino_t	ino;					/* i-number */
#ifdef HAVE_GENERATION
	unsigned int gen;                               /* generation number */
#endif
};

/*
 * Driver-private i-node information we keep about local host file
 * system objects.
 */
struct native_inode {
	unsigned
		ni_seekok		: 1,		/* can seek? */
		ni_attrvalid		: 1,		/* cached attrs ok? */
		ni_resetfpos		: 1;		/* reset fpos? */
	struct native_inode_identifier ni_ident;	/* unique identifier */
	struct file_identifier ni_fileid;		/* ditto */
	int	ni_fd;					/* host fildes */
	int	ni_oflags;				/* flags, from open */
	unsigned ni_nopens;				/* soft ref count */
	_SYSIO_OFF_T ni_fpos;				/* current pos */
	time_t	ni_attrtim;				/* attrs expire time */
};

/*
 * Cached attributes usable?
 */
#define NATIVE_ATTRS_VALID(nino, t) \
	((nino)->ni_attrtim && (t) < (nino)->ni_attrtim)

/*
 * Native IO path arguments.
 */
struct native_io {
	char	nio_op;					/* 'r' or 'w' */
	struct native_inode *nio_nino;			/* native ino */
};

static int native_inop_lookup(struct pnode *pno,
			      struct inode **inop,
			      struct intent *intnt,
			      const char *path);
static int native_inop_getattr(struct pnode *pno,
			       struct intnl_stat *stbuf);
static int native_inop_setattr(struct pnode *pno,
			       unsigned mask,
			       struct intnl_stat *stbuf);
static ssize_t native_filldirentries(struct pnode *pno,
				     _SYSIO_OFF_T *posp,
				     char *buf,
				     size_t nbytes);
static int native_inop_mkdir(struct pnode *pno, mode_t mode);
static int native_inop_rmdir(struct pnode *pno);
static int native_inop_symlink(struct pnode *pno, const char *data);
static int native_inop_readlink(struct pnode *pno, char *buf, size_t bufsiz);
static int native_inop_open(struct pnode *pno, int flags, mode_t mode);
static int native_inop_close(struct pnode *pno);
static int native_inop_link(struct pnode *old, struct pnode *new);
static int native_inop_unlink(struct pnode *pno);
static int native_inop_rename(struct pnode *old, struct pnode *new);
static int native_inop_read(struct ioctx *ioctx);
static int native_inop_write(struct ioctx *ioctx);
static _SYSIO_OFF_T native_inop_pos(struct pnode *pno, _SYSIO_OFF_T off);
static int native_inop_iodone(struct ioctx *ioctx);
static int native_inop_fcntl(struct pnode *pno, int cmd, va_list ap, int *rtn);
static int native_inop_sync(struct pnode *pno);
static int native_inop_datasync(struct pnode *pno);
static int native_inop_ioctl(struct pnode *pno,
			     unsigned long int request,
			     va_list ap);
static int native_inop_mknod(struct pnode *pno, mode_t mode, dev_t dev);
#ifdef _HAVE_STATVFS
static int native_inop_statvfs(struct pnode *pno, struct intnl_statvfs *buf);
#endif
static void native_inop_gone(struct inode *ino);

static struct inode_ops native_i_ops = {
	native_inop_lookup,
	native_inop_getattr,
	native_inop_setattr,
	native_filldirentries,
	NULL,
	native_inop_mkdir,
	native_inop_rmdir,
	native_inop_symlink,
	native_inop_readlink,
	native_inop_open,
	native_inop_close,
	native_inop_link,
	native_inop_unlink,
	native_inop_rename,
	native_inop_read,
	native_inop_write,
	native_inop_pos,
	native_inop_iodone,
	native_inop_fcntl,
	NULL,
	native_inop_sync,
	NULL,
	native_inop_datasync,
	native_inop_ioctl,
	native_inop_mknod,
#ifdef _HAVE_STATVFS
	native_inop_statvfs,
#endif
	_sysio_p_generic_perms_check,
	native_inop_gone
};

static int native_fsswop_mount(const char *source,
			       unsigned flags,
			       const void *data,
			       struct pnode *tocover,
			       struct mount **mntp);

static struct fssw_ops native_fssw_ops = {
	native_fsswop_mount
};

static void native_fsop_gone(struct filesys *fs);

static struct filesys_ops native_inodesys_ops = {
	native_fsop_gone,
};

/*
 * This example driver plays a strange game. It maintains a private,
 * internal mount -- It's own separate, rooted, name space. The local
 * file system's entire name space is available via this tree.
 *
 * This simplifies the implementation. At mount time, we need to generate
 * a path-node to be used as a root. This allows us to look up the needed
 * node in the host name space and leverage a whole lot of support from
 * the system.
 */
static struct mount *native_internal_mount = NULL;

/*
 * Given i-node, return driver private part.
 */
#define I2NI(ino)	((struct native_inode *)((ino)->i_private))

/*
 * Simple stat of path or file.
 */
static int
native_simple_stat(const char *path, struct inode *ino, struct intnl_stat *buf)
{
	int	err;
	struct native_inode *nino;
	struct _sysio_native_stat stbuf;

	if (path)
		err = syscall(SYSIO_SYS_stat, path, &stbuf);
	else if ((nino = ino ? I2NI(ino) : NULL) && nino->ni_fd >= 0)
		err = syscall(SYSIO_SYS_fstat, nino->ni_fd, &stbuf);
	else
		abort();
	if (err)
		return -errno;
	SYSIO_COPY_STAT(&stbuf, buf);
	return 0;
}

/*
 * stat -- by path.
 */
static int
native_stat(const char *path,
	    struct inode *ino,
	    time_t t,
	    struct intnl_stat *buf)
{
	struct intnl_stat mybuf;
	int	err;

	if (!buf)
		buf = &mybuf;
	err = native_simple_stat(path, ino, buf);
	if (err)
		t = 0;
	else if (ino)
		ino->i_stbuf = *buf;
	if (ino)
		I2NI(ino)->ni_attrtim = t;
	return 0;
}

/*
 * Introduce an i-node to the system.
 */
static struct inode *
native_i_new(struct filesys *fs, time_t expiration, struct intnl_stat *buf)
{
	struct native_inode *nino;
	struct inode *ino;

	nino = malloc(sizeof(struct native_inode));
	if (!nino)
		return NULL;
	bzero(&nino->ni_ident, sizeof(nino->ni_ident));
	nino->ni_seekok = 0;
	nino->ni_attrvalid = 0;
	nino->ni_resetfpos = 0;
	nino->ni_ident.dev = buf->st_dev;
	nino->ni_ident.ino = buf->st_ino;
#ifdef HAVE_GENERATION
	nino->ni_ident.gen = buf->st_gen;
#endif
	nino->ni_fileid.fid_data = &nino->ni_ident;
	nino->ni_fileid.fid_len = sizeof(nino->ni_ident);
	nino->ni_fd = -1;
	nino->ni_oflags = 0;
	nino->ni_nopens = 0;
	nino->ni_fpos = 0;
	nino->ni_attrtim = expiration;
	ino =
	    _sysio_i_new(fs,
			 &nino->ni_fileid,
			 buf,
			 0,
			 &native_i_ops,
			 nino);
	if (!ino)
		free(nino);
	return ino;
}

/*
 * Initialize this driver.
 */
int
_sysio_native_init()
{

	/*
	 * Capture current process umask and reset our process umask to
	 * zero. All permission bits to open/creat/setattr are absolute --
	 * They've already had a umask applied, when appropriate.
	 */
#ifndef REDSTORM
	/*
	 * This is done is cstart on Red Storm. Saves a syscall from
	 * every node...
	 */
	_sysio_umask = syscall(SYSIO_SYS_umask, 0);
#endif

	return _sysio_fssw_register("native", &native_fssw_ops);
}

/*
 * Create private, internal, view of the hosts name space.
 */
static int
create_internal_namespace(const void *data)
{
	char	*opts;
	ssize_t	len;
	char	*cp;
	struct native_filesystem *nfs;
	int	err;
	struct mount *mnt;
	struct inode *rootino;
	struct filesys *fs;
	time_t	t;
	struct intnl_stat stbuf;
	unsigned long ul;
	static struct option_value_info v[] = {
		{ "atimo",	"30" },
		{ NULL,		NULL }
	};

	if (native_internal_mount) {
		/*
		 * Reentered!
		 */
		abort();
	}

	/*
	 * Get mount options.
	 */
	opts = NULL;
	if (data && (len = strlen((char *)data))) {
		opts = malloc(len + 1);
		if (!opts)
			return -ENOMEM;
		(void )strcpy(opts, data);
		if (_sysio_get_args(opts, v) - opts != (ssize_t )len)
			return -EINVAL;
	}
	ul = strtoul(v[0].ovi_value, &cp, 0);
	if (*cp != '\0' || ul >= UINT_MAX)
		return -EINVAL;
	if (opts) {
		free(opts);
		opts = NULL;
	}

	/*
	 * We maintain an artificial, internal, name space in order to
	 * have access to fully qualified path names in the various routines.
	 * Initialize that name space now.
	 */
	fs = NULL;
	mnt = NULL;
	rootino = NULL;
	/*
	 * This really should be per-mount. Hmm, but that's best done
	 * as proper sub-mounts in the core and not this driver. We reconcile
	 * now, here, by putting the mount options on the file system. That
	 * means they are global and only can be passed at the initial mount.
	 *
	 * Maybe do it right some day?
	 */
	nfs = malloc(sizeof(struct native_filesystem));
	if (!nfs) {
		err = -ENOMEM;
		goto error;
	}
	nfs->nfs_atimo = ul;
	if ((unsigned long)nfs->nfs_atimo != ul) {
		err = -EINVAL;
		goto error;
	}
	fs = _sysio_fs_new(&native_inodesys_ops, 0, nfs);
	if (!fs) {
		err = -ENOMEM;
		goto error;
	}

	/*
	 * Get root i-node.
	 */
	t = _SYSIO_LOCAL_TIME();
	err = native_stat("/", NULL, 0, &stbuf);
	if (err)
		goto error;
	rootino = native_i_new(fs, t + FS2NFS(fs)->nfs_atimo, &stbuf);
	if (!rootino) {
		err = -ENOMEM;
		goto error;
	}

	/*
	 * Mount it. This name space is disconnected from the
	 * rest of the system -- Only available within this driver.
	 */
	err = _sysio_mounti(fs, rootino, 0, NULL, &mnt);
	if (err)
		goto error;
	I_PUT(rootino);					/* drop ours */
	native_internal_mount = mnt;
	return 0;
error:
	if (mnt) {
		if (_sysio_do_unmount(mnt) != 0)
			abort();
		rootino = NULL;
		fs = NULL;
		nfs = NULL;
	}
	if (rootino)
		I_RELE(rootino);
	if (fs) {
		FS_RELE(fs);
		nfs = NULL;
	}
	if (nfs)
		free(nfs);
	if (opts)
		free(opts);

	return err;
}

static int
native_fsswop_mount(const char *source,
		    unsigned flags,
		    const void *data,
		    struct pnode *tocover,
		    struct mount **mntp)
{
	int	err;
	struct nameidata nameidata;
	struct mount *mnt;

	/*
	 * Caller must use fully qualified path names when specifying
	 * the source.
	 */
	if (*source != '/')
		return -ENOENT;

	if (!native_internal_mount) {
		err = create_internal_namespace(data);
		if (err)
			return err;
	} else if (data && *(char *)data)
		return -EINVAL;

	/*
	 * Lookup the source in the internally maintained name space.
	 */
	ND_INIT(&nameidata, 0, source, native_internal_mount->mnt_root, NULL);
	err = _sysio_path_walk(native_internal_mount->mnt_root, &nameidata);
	if (err)
		return err;

	/*
	 * Have path-node specified by the given source argument. Let the
	 * system finish the job, now.
	 */
	err =
	    _sysio_do_mount(native_internal_mount->mnt_fs,
			    nameidata.nd_pno->p_base,
			    flags,
			    tocover,
			    &mnt);
	/*
	 * Release the internal name space pnode and clean up any
	 * aliases we might have generated. We really don't need to cache them
	 * as they are only used at mount time..
	 */
	P_PUT(nameidata.nd_pno);
	(void )_sysio_p_prune(native_internal_mount->mnt_root);

	if (!err) {
		FS_REF(native_internal_mount->mnt_fs);
		*mntp = mnt;
	}
	return err;
}

static int
native_i_invalid(struct inode *inop, struct intnl_stat *stat)
{
	struct native_inode *nino;

	/*
	 * Validate passed in inode against stat struct info
	 */
	nino = I2NI(inop);
	
	if (!nino->ni_attrtim ||
	    (nino->ni_ident.dev != stat->st_dev ||
	     nino->ni_ident.ino != stat->st_ino ||
#ifdef HAVE_GENERATION
	     nino->ni_ident.gen != stat->st_gen ||
#endif
	     ((inop)->i_stbuf.st_mode & S_IFMT) != (stat->st_mode & S_IFMT)) ||
	    (((inop)->i_stbuf.st_rdev != stat->st_rdev) &&
	       (S_ISCHR((inop)->i_stbuf.st_mode) ||
	        S_ISBLK((inop)->i_stbuf.st_mode)))) {
		nino->ni_attrtim = 0;			/* invalidate attrs */
		return 1;
	}
	return 0;
}

static struct inode *
native_iget(struct filesys *fs, time_t expire, struct intnl_stat *stbp)
{
	struct inode *ino;
	struct native_inode_identifier ident;
	struct file_identifier fileid;

	bzero(&ident, sizeof(ident)); 
	ident.dev = stbp->st_dev;
	ident.ino = stbp->st_ino;
#ifdef HAVE_GENERATION
	ident.gen = stbp->st_gen;
#endif
	fileid.fid_data = &ident;
	fileid.fid_len = sizeof(ident);
	ino = _sysio_i_find(fs, &fileid);
	if (ino) {
		ino->i_stbuf = *stbp;
		I2NI(ino)->ni_attrtim = expire;
		return ino;
	}
	return native_i_new(fs, expire, stbp);
}

/*
 * Find, and validate, or create i-node by host-relative path. Returned i-node
 * is referenced.
 */
static int
native_ibind(struct filesys *fs,
	     char *path,
	     time_t t,
	     struct inode **inop)
{
	struct intnl_stat buf;
	int	err;
	struct inode *ino;

	err = native_simple_stat(path, *inop, &buf);
	if (err)
		return err;
	t += FS2NFS(fs)->nfs_atimo;
	if (*inop) {
		/* 
		 * Revalidate.
		 */
		if (!native_i_invalid(*inop, &buf)) {
			(*inop)->i_stbuf = buf;
			I2NI(*inop)->ni_attrtim = t;
			return 0;
		}
		_sysio_i_undead(*inop);
		*inop = NULL;
	}
	if (!(ino = native_iget(fs, t, &buf)))
		return -ENOMEM;
	*inop = ino;
	return 0;
}

static int
native_inop_lookup(struct pnode *pno,
		   struct inode **inop,
		   struct intent *intnt __IS_UNUSED,
		   const char *path __IS_UNUSED)
{
	time_t	t;
	char	*fqpath;
	struct filesys *fs;
	int	err;

	*inop = pno->p_base->pb_ino;

	/*
	 * Try to use the cached attributes unless the intent
	 * indicates we are looking up the last component and
	 * caller wants attributes. In that case, force a refresh.
	 */
	t = _SYSIO_LOCAL_TIME();
	if (*inop &&
	    (path || !intnt || (intnt->int_opmask & INT_GETATTR) == 0) &&
	    NATIVE_ATTRS_VALID(I2NI(*inop), t))
		return 0;

	/*
	 * Don't have an inode yet. Because we translate everything back to
	 * a single name space for the host, we will assume the object the
	 * caller is looking for has no existing alias in our internal
	 * name space. We don't see the same file on different mounts in the
	 * underlying host FS as the same file.
	 *
	 * The file identifier *will* be unique. It's got to have a different
	 * dev.
	 */
	fqpath = _sysio_pb_path(pno->p_base, '/');
	if (!fqpath)
		return -ENOMEM;
	fs = pno->p_mount->mnt_fs;
	err = native_ibind(fs, fqpath, t + FS2NFS(fs)->nfs_atimo, inop);
	free(fqpath);
	if (err)
		*inop = NULL;
	return err;
}

static int
native_inop_getattr(struct pnode *pno, struct intnl_stat *stat)
{
	int	err;
	char	*path;
	struct inode *ino;
	struct filesys *fs;
	time_t	t;

	/*
	 * We just cannot use the cached attributes when getattr is
	 * called. Had the caller felt those were sufficient then
	 * they could have (would have?) simply used what was cached
	 * after revalidating. In this case, there's a good chance the
	 * caller is looking for the current time stamps and/or size. Something
	 * pretty volatile anyway.
	 */
	err = 0;					/* compiler cookie */

	path = _sysio_pb_path(pno->p_base, '/');
	if (!path)
		return -ENOMEM;
	ino = pno->p_base->pb_ino;
	assert(ino);
	fs = pno->p_mount->mnt_fs;
	t = _SYSIO_LOCAL_TIME();
	err = native_stat(path, ino, t + FS2NFS(fs)->nfs_atimo, stat);
	free(path);
	
	return err;
}

#ifdef SYSIO_SYS_utime
static int
_ut(const char *path, time_t actime, time_t modtime)
{
	struct utimbuf ut;

	ut.actime = actime;
	ut.modtime = modtime;
	return syscall(SYSIO_SYS_utime, path, &ut);
}
#else
static int
_ut(const char *path, time_t actime, time_t modtime)
{
	struct timeval tv[2];

	tv[0].tv_sec = actime;
	tv[0].tv_usec = 0;
	tv[1].tv_sec = modtime;
	tv[1].tv_usec = 0;
	return syscall(SYSIO_SYS_utimes, path, &tv);
}
#endif

static int
native_inop_setattr(struct pnode *pno,
		    unsigned mask,
		    struct intnl_stat *stat)
{
	char	*path;
	struct inode *ino;
	struct native_inode *nino;
	int	fd;
	int	err;

	path = NULL;
	ino = pno->p_base->pb_ino;
	nino = ino ? I2NI(ino) : NULL;
	fd = -1;
	if (nino)
		fd = nino->ni_fd;
	if (fd < 0 || mask & (SETATTR_MTIME|SETATTR_ATIME)) {
		path = _sysio_pb_path(pno->p_base, '/');
		if (!path)
			return -ENOMEM;
	}

	/*
	 * Get current status for undo.
	 */
	err = native_stat(path, ino, 0, NULL);
	if (err)
		goto out;

	if (mask & SETATTR_MODE) {
		mode_t	mode;

		/*
		 * Alter permissions attribute.
		 */
		mode = stat->st_mode & 07777;
		err =
		    fd < 0
		      ? syscall(SYSIO_SYS_chmod, path, mode)
		      : syscall(SYSIO_SYS_fchmod, fd, mode);
		if (err)
			err = -errno;
	}
	if (err)
		mask &= ~SETATTR_MODE;
	else if (mask & (SETATTR_MTIME|SETATTR_ATIME)) {
		time_t	actime, modtime;

		/*
		 * Alter access and/or modify time attributes.
		 */
		actime  = ino->i_stbuf.st_atime;
		modtime  = ino->i_stbuf.st_mtime;
		if (mask & SETATTR_ATIME)
			actime = stat->st_atime;
		if (mask & SETATTR_MTIME)
			modtime = stat->st_mtime;
		if (_ut(path, actime, modtime) != 0)
			return -errno;
	}
	if (err)
		mask &= ~(SETATTR_MTIME|SETATTR_ATIME);
	else if (mask & (SETATTR_UID|SETATTR_GID)) {

		/*
		 * Alter owner and/or group identifiers.
		 */
		err =
		    fd < 0
		      ? syscall(SYSIO_SYS_chown,
				path,
				mask & SETATTR_UID
				  ? stat->st_uid
				  : (uid_t )-1,
				mask & SETATTR_GID
				  ? stat->st_gid
				  : (gid_t )-1)
		      : syscall(SYSIO_SYS_fchown,
				fd,
				mask & SETATTR_UID
				  ? stat->st_uid
				  : (uid_t )-1,
				mask & SETATTR_GID
				  ? stat->st_gid
				  : (gid_t )-1);
		if (err)
			err = -errno;
	}
	if (err)
		mask &= ~(SETATTR_UID|SETATTR_GID);
	else if (mask & SETATTR_LEN) {
		/*
		 * Do the truncate last. It can't be undone.
		 */
		 err = fd < 0
			   ? syscall(SYSIO_SYS_truncate, path, stat->st_size)
			   : syscall(SYSIO_SYS_ftruncate, fd, stat->st_size);
		if (err)
			err = -errno;
	}
	if (!err)
		goto out;
	/*
	 * Undo after error. Some or all of this might not work... We
	 * can but try.
	 */
	if (mask & (SETATTR_UID|SETATTR_GID)) {
		 (void )(fd < 0
			   ? syscall(SYSIO_SYS_chown,
				     path,
				     mask & SETATTR_UID
				       ? ino->i_stbuf.st_uid
				       : (uid_t )-1,
				     mask & SETATTR_GID
				       ? ino->i_stbuf.st_gid
				       : (gid_t )-1)
			   : syscall(SYSIO_SYS_fchown,
				     fd,
				     mask & SETATTR_UID
				       ? ino->i_stbuf.st_uid
				       : (uid_t )-1,
				     mask & SETATTR_GID
				       ? ino->i_stbuf.st_gid
				       : (gid_t )-1));
	}
	if (mask & (SETATTR_MTIME|SETATTR_ATIME))
		(void )_ut(path, ino->i_stbuf.st_atime, ino->i_stbuf.st_mtime);
	if (mask & SETATTR_MODE) {
		fd < 0
		  ? syscall(SYSIO_SYS_chmod, path, ino->i_stbuf.st_mode & 07777)
		  : syscall(SYSIO_SYS_fchmod, ino->i_stbuf.st_mode & 07777);
	}
out:
	/*
	 * We must refresh the cached attributes.
	 */
	if (!err &&
	    native_stat(path,
			ino,
			_SYSIO_LOCAL_TIME() + FS2NFS(ino->i_fs)->nfs_atimo,
			NULL) != 0)
		abort();
	if (path)
		free(path);
	return err;
}

static int
native_pos(int fd, _SYSIO_OFF_T *offset, int whence)
{
	_SYSIO_OFF_T off;

	assert(fd >= 0);
	assert(*offset >= 0);

	off = *offset;
#if defined(_LARGEFILE64_SOURCE) && defined(SYSIO_SYS__llseek)
	{
		int	err;
		err =
		    syscall(SYSIO_SYS__llseek,
			    (unsigned int)fd,
			    (unsigned int)(off >> 32),
			    (unsigned int)off,
			    &off,
			    whence);
		if (err == -1)
			return -errno;
	}
#else
	{
		off =
		    syscall(SYS_lseek,
			    fd,
			    off,
			    whence);
		if (off < 0)
			return -errno;
	}
#endif
	*offset = off;

	return 0;
}

static ssize_t
native_ifilldirentries(struct native_inode *nino,
		       _SYSIO_OFF_T *posp,
		       char *buf,
		       size_t nbytes)
{
	int	err;
	ssize_t	cc;
#if defined(SYSIO_SYS_getdirentries)
	_SYSIO_OFF_T	waste;
#endif

	if (*posp < 0)
		return -EINVAL;

	/*
	 * Stream-oriented access requires that we reposition prior to the
	 * fill call.
	 */
	assert(nino->ni_seekok);
	if (*posp != nino->ni_fpos || nino->ni_resetfpos) {
		nino->ni_fpos = *posp;
		err = native_pos(nino->ni_fd, &nino->ni_fpos, SEEK_SET);
		if (err) {
			nino->ni_resetfpos = 1;
			return err;
		}
		nino->ni_resetfpos = 0;
	}

#if defined(SYSIO_SYS_getdirentries)
	waste = *posp;
#endif
	cc =
#if defined(SYSIO_SYS_getdirentries)
	    syscall(SYSIO_SYS_getdirentries,
		    nino->ni_fd,
		    buf,
		    nbytes,
		    &waste);
#elif defined(SYSIO_SYS_getdents64)
	    syscall(SYSIO_SYS_getdents64, nino->ni_fd, buf, nbytes);
#elif defined(SYSIO_SYS_getdents)
	    syscall(SYSIO_SYS_getdents, nino->ni_fd, buf, nbytes);
#endif

	if (cc < 0)
		return -errno;
	/*
	 * Stream-oriented access requires that we discover where we are
	 * after the call.
	 */
	nino->ni_fpos = 0;
	if ((err = native_pos(nino->ni_fd, &nino->ni_fpos, SEEK_CUR)) != 0) {
		/*
		 * Leave the position at the old I suppose.
		 */
		nino->ni_resetfpos = 1;
		return err;
	}
	*posp = nino->ni_fpos;
	return cc;
}

static ssize_t
native_filldirentries(struct pnode *pno,
		      _SYSIO_OFF_T *posp,
		      char *buf,
		      size_t nbytes)
{
	struct inode *ino;
	struct native_inode *nino;
#ifdef DIR_CVT_64
	char	*bp;
	size_t	count;
	struct linux_dirent *ldp;
	struct dirent64 *d64p;
	size_t	namlen;
	size_t	reclen;
#else
#define bp buf
#define count nbytes
#endif
	ssize_t	cc;

	ino = pno->p_base->pb_ino;
	assert(ino);
	nino = I2NI(ino);
	assert(nino->ni_fd >= 0);

#ifdef DIR_CVT_64
	count = nbytes;
	while (!(bp = malloc(count))) {
		count /= 2;
		if (count < sizeof(struct dirent))
			return -ENOMEM;
	}
#endif
	cc = native_ifilldirentries(nino, posp, bp, count);
	if (cc < 0) {
#ifdef DIR_CVT_64
		free(bp);
#endif
		return cc;
	}
#ifdef DIR_CVT_64
	ldp = (struct linux_dirent *)bp;
	d64p = (struct dirent64 *)buf;
	while (cc) {
		namlen = strlen(ldp->ld_name);
		reclen = sizeof(*d64p) - sizeof(d64p->d_name) + namlen;
		if (nbytes <= reclen)
			break;
		d64p->d_ino = ldp->ld_ino;
		d64p->d_off = nino->ni_fpos = ldp->ld_off;
		d64p->d_reclen = 
		    (((reclen + sizeof(long))) / sizeof(long)) * sizeof(long);
		if (nbytes < d64p->d_reclen)
			d64p->d_reclen = reclen + 1;
		d64p->d_type = DT_UNKNOWN;		/* you lose -- sorry. */
		(void )memcpy(d64p->d_name, ldp->ld_name, namlen);
		/*
		 * Zero pad the rest.
		 */
		for (cp = d64p->d_name + namlen, n = d64p->d_reclen - reclen;
		     n;
		     n--)
			*cp++ = 0;
		cc -= ldp->ld_reclen;
		ldp = (struct linux_dirent *)((char *)ldp + ldp->ld_reclen);
		nbytes -= d64p->d_reclen;
		d64p = (struct dirent64 *)((char *)d64p + d64p->d_reclen);
	}
	free(bp);
	cc =
	    (d64p == (struct dirent64 *)buf && cc)
	      ? -EINVAL
	      : (char *)d64p - buf;
#else
#undef bp
#undef count
#endif
	return cc;
}

static int
native_inop_mkdir(struct pnode *pno, mode_t mode)
{
	char	*path;
	int	err;

	path = _sysio_pb_path(pno->p_base, '/');
	if (!path)
		return -ENOMEM;

	err = syscall(SYSIO_SYS_mkdir, path, mode);
	if (err != 0)
		err = -errno;
	free(path);
	return err;
}

static int
native_inop_rmdir(struct pnode *pno)
{
	char	*path;
	int	err;

	path = _sysio_pb_path(pno->p_base, '/');
	if (!path)
		return -ENOMEM;

	err = syscall(SYSIO_SYS_rmdir, path);
	if (err != 0)
		err = -errno;
	free(path);
	return err;
}

static int
native_inop_symlink(struct pnode *pno, const char *data)
{
	char	*path;
	int	err;

	path = _sysio_pb_path(pno->p_base, '/');
	if (!path)
		return -ENOMEM;

	err = syscall(SYSIO_SYS_symlink, data, path);
	if (err != 0)
		err = -errno;
	free(path);
	return err;
}

static int
native_inop_readlink(struct pnode *pno, char *buf, size_t bufsiz)
{
	char	*path;
	int	i;

	path = _sysio_pb_path(pno->p_base, '/');
	if (!path)
		return -ENOMEM;
	i = syscall(SYSIO_SYS_readlink, path, buf, bufsiz);
	if (i < 0)
		i = -errno;
	free(path);
	return i;
}

static int 
native_inop_open(struct pnode *pno, int flags, mode_t mode)
{
	struct native_inode *nino;
	char	*path;
	struct inode *ino;
	int	fd;

	path = _sysio_pb_path(pno->p_base, '/');
	if (!path)
		return -ENOMEM;

	/*
	 * Whether the file is already open, or not, makes no difference.
	 * Want to always give the host OS a chance to authorize in case
	 * something has changed underneath us.
	 */
	if (flags & O_WRONLY) {
		/*
		 * Promote write-only attempt to RW.
		 */
		flags &= ~O_WRONLY;
		flags |= O_RDWR;
	}
#ifdef O_LARGEFILE
	flags |= O_LARGEFILE;
#endif
	ino = pno->p_base->pb_ino;
	fd = syscall(SYSIO_SYS_open, path, flags, mode);
	if (!ino && fd >= 0) {
		struct filesys *fs;
		int	err;

		/*
		 * Success but we need to return an i-node.
		 */
		fs = pno->p_mount->mnt_fs;
		err =
		    native_ibind(fs,
				 path,
				 _SYSIO_LOCAL_TIME() + FS2NFS(fs)->nfs_atimo,
				 &ino);
		if (err) {
			(void )syscall(SYSIO_SYS_close, fd);
			if (err == -EEXIST)
				abort();
			fd = err;
		}
	} else
		I_GET(ino);
	free(path);
	if (fd < 0)
		return -errno;

	/*
	 * Remember this new open.
	 */
	nino = I2NI(ino);
	nino->ni_nopens++;
	assert(nino->ni_nopens);
	do {
		if (nino->ni_fd >= 0) {
			if ((nino->ni_oflags & O_RDWR) ||
			    (flags & (O_RDONLY|O_WRONLY|O_RDWR)) == O_RDONLY) {
				/*
				 * Keep existing.
				 */
				(void )syscall(SYSIO_SYS_close, fd);
				break;
			}
			(void )syscall(SYSIO_SYS_close, nino->ni_fd);
		}
		/*
		 * Invariant; First open. Must init.
		 */
		nino->ni_resetfpos = 0;
		nino->ni_fpos = 0;
		nino->ni_fd = fd;
		/*
		 * Need to know whether we can seek on this
		 * descriptor.
		 */
		nino->ni_seekok =
		    native_pos(nino->ni_fd, &nino->ni_fpos, SEEK_CUR) != 0
		      ? 0
		      : 1;
	} while (0);
	I_PUT(ino);

	return 0;
}

static int
native_inop_close(struct pnode *pno)
{
	struct inode *ino;
	struct native_inode *nino;
	int	err;

	ino = pno->p_base->pb_ino;
	assert(ino);
	nino = I2NI(ino);
	if (nino->ni_fd < 0)
		abort();

	assert(nino->ni_nopens);
	if (--nino->ni_nopens) {
		/*
		 * Hmmm. We really don't need anything else. However, some
		 * filesystems try to implement a sync-on-close semantic.
		 * As this appears now, that is lost. Might want to change
		 * it somehow in the future?
		 */
		return 0;
	}

	err = syscall(SYSIO_SYS_close, nino->ni_fd);
	if (err)
		return -errno;

	nino->ni_fd = -1;
	nino->ni_resetfpos = 0;
	nino->ni_fpos = 0;
	return 0;
}

static int
native_inop_link(struct pnode *old, struct pnode *new)
{
	int	err;
	char	*opath, *npath;

	err = 0;

	opath = _sysio_pb_path(old->p_base, '/');
	npath = _sysio_pb_path(new->p_base, '/');
	if (!(opath && npath)) {
		err = -ENOMEM;
		goto out;
	}

	err = syscall(SYSIO_SYS_link, opath, npath);
	if (err != 0)
		err = -errno;

out:
	if (opath)
		free(opath);
	if (npath)
		free(npath);
	return err;
}

static int
native_inop_unlink(struct pnode *pno)
{
	char	*path;
	int	err = 0;

	path = _sysio_pb_path(pno->p_base, '/');
	if (!path)
		return -ENOMEM;

	/*
	 * For this driver, unlink is easy with open files. Since the
	 * file remains open to the system, too, the descriptors are still
	 * valid.
	 *
	 * Other drivers will have some difficulty here as the entry in the
	 * file system name space must be removed without sacrificing access
	 * to the file itself. In NFS this is done with a mechanism referred
	 * to as a `silly delete'. The file is moved to a temporary name
	 * (usually .NFSXXXXXX, where the X's are replaced by the PID and some
	 * unique characters) in order to simulate the proper semantic.
	 */
	if (syscall(SYSIO_SYS_unlink, path) != 0)
		err = -errno;
	free(path);
	return err;
}

static int
native_inop_rename(struct pnode *old, struct pnode *new)
{
	int	err;
	char	*opath, *npath;

	opath = _sysio_pb_path(old->p_base, '/');
	npath = _sysio_pb_path(new->p_base, '/');
	if (!(opath && npath)) {
		err = -ENOMEM;
		goto out;
	}

	err = syscall(SYSIO_SYS_rename, opath, npath);
	if (err != 0)
		err = -errno;

out:
	if (opath)
		free(opath);
	if (npath)
		free(npath);
	return err;
}

static ssize_t
dopio(void *buf, size_t count, _SYSIO_OFF_T off, struct native_io *nio)
{
	ssize_t	cc;

	if (!nio->nio_nino->ni_seekok) {
		if (off != nio->nio_nino->ni_fpos) {
			/*
			 * They're trying to reposition. Can't
			 * seek on this descriptor so we err out now.
			 */
			errno = ESPIPE;
			return -1;
		}
		cc =
		    syscall(nio->nio_op == 'r'
			      ? SYSIO_SYS_read
			      : SYSIO_SYS_write,
			    nio->nio_nino->ni_fd,
			    buf,
			    count);
		if (cc > 0)
			nio->nio_nino->ni_fpos += cc;
	} else
		cc =
		    syscall((nio->nio_op == 'r'
			       ? SYSIO_SYS_pread
			       : SYSIO_SYS_pwrite),
			    nio->nio_nino->ni_fd,
			    buf,
			    count,
			    off);

	return cc;
}

static ssize_t
doiov(const struct iovec *iov,
      int count,
      _SYSIO_OFF_T off,
      ssize_t limit,
      struct native_io *nio)
{
	ssize_t	cc;

#if !(defined(REDSTORM) || defined(MAX_IOVEC))
#define MAX_IOVEC      INT_MAX
#endif


	if (count <= 0)
		return -EINVAL;

	/*
	 * Avoid the reposition call if we're already at the right place.
	 * Allows us to access pipes and fifos.
	 */
	if (off != nio->nio_nino->ni_fpos) {
		int	err;

		err = native_pos(nio->nio_nino->ni_fd, &off, SEEK_SET);
		if (err) {
			nio->nio_nino->ni_resetfpos = 1;
			return err;
		}
		nio->nio_nino->ni_resetfpos = 0;
		nio->nio_nino->ni_fpos = off;
	}

	/*
	 * The {read,write}v is safe as this routine is only ever called
	 * by _sysio_enumerate_extents() and that routine is exact. It never
	 * passes iovectors including tails.
	 */
	cc =
#ifndef REDSTORM
	    count <= MAX_IOVEC
	      ? syscall(nio->nio_op == 'r' ? SYSIO_SYS_readv : SYSIO_SYS_writev,
			nio->nio_nino->ni_fd,
			iov,
			count)
	      :
#endif
	        _sysio_enumerate_iovec(iov,
				       count,
				       off,
				       limit,
				       (ssize_t (*)(void *,
						    size_t,
						    _SYSIO_OFF_T,
						    void *))dopio,
				       nio);
	if (cc < 0)
		cc = -errno;
	else
		nio->nio_nino->ni_fpos += cc;
	return cc;

#if !(defined(REDSTORM) || defined(MAX_IOVEC))
#undef MAX_IOVEC
#endif
}

#if 0
static int
lockop_all(struct native_inode *nino,
	   struct intnl_xtvec *xtv,
	   size_t count,
	   short op)
{
	struct flock flock;
	int	err;

	if (!count)
		return -EINVAL;
	flock.l_type = op;
	flock.l_whence = SEEK_SET;
	while (count--) {
		flock.l_start = xtv->xtv_off;
		flock.l_len = xtv->xtv_len;
		xtv++;
		err =
		    syscall(SYSIO_SYS_fcntl,
			    nino->ni_fd,
			    F_SETLK,
			    &flock);
		if (err != 0)
			return -errno;
	}
	return 0;
}

static int
order_xtv(const struct intnl_xtvec *xtv1, const struct intnl_xtvec *xtv2)
{

	if (xtv1->xtv_off < xtv2->xtv_off)
		return -1;
	if (xtv1->xtv_off > xtv2->xtv_off)
		return 1;
	return 0;
}
#endif

static int
doio(char op, struct ioctx *ioctx)
{
	struct inode *ino;
	struct native_inode *nino;
#if 0
	int	dolocks;
	struct intnl_xtvec *oxtv;
	int	err;
#endif
	struct native_io arguments;
	ssize_t	cc;
#if 0
	struct intnl_xtvec *front, *rear, tmp;
#endif

	ino = ioctx->ioctx_pno->p_base->pb_ino;
	assert(ino);
	nino = I2NI(ino);
#if 0
	dolocks = ioctx->ioctx_xtvlen > 1 && nino->ni_seekok;
	if (dolocks) {
		/*
		 * Must lock the regions (in order!) since we can't do
		 * strided-IO as a single atomic operation.
		 */
		oxtv = malloc(ioctx->ioctx_xtvlen * sizeof(struct intnl_xtvec));
		if (!oxtv)
			return -ENOMEM;
		(void )memcpy(oxtv,
			      ioctx->ioctx_xtv, 
			      ioctx->ioctx_xtvlen * sizeof(struct intnl_xtvec));
		qsort(oxtv,
		      ioctx->ioctx_xtvlen,
		      sizeof(struct intnl_xtvec),
		      (int (*)(const void *, const void *))order_xtv);
		err =
	            lockop_all(nino,
			       oxtv, ioctx->ioctx_xtvlen,
			       op == 'r' ? F_RDLCK : F_WRLCK);
		if (err) {
			free(oxtv);
			return err;
		}
	}
#endif
	arguments.nio_op = op;
	arguments.nio_nino = nino;
	cc =
	    _sysio_enumerate_extents(ioctx->ioctx_xtv, ioctx->ioctx_xtvlen, 
				     ioctx->ioctx_iov, ioctx->ioctx_iovlen,
				     (ssize_t (*)(const struct iovec *,
						  int,
						  _SYSIO_OFF_T,
						  ssize_t,
						  void *))doiov,
				     &arguments);
#if 0
	if (dolocks) {
		/*
		 * Must unlock in reverse order.
		 */
		front = oxtv;
		rear = front + ioctx->ioctx_xtvlen - 1;
		while (front < rear) {
			tmp = *front;
			*front++ = *rear;
			*rear-- = tmp;
		}
		if (lockop_all(nino, oxtv, ioctx->ioctx_xtvlen, F_UNLCK) != 0)
			abort();
		free(oxtv);
	}
#endif
	if ((ioctx->ioctx_cc = cc) < 0) {
		ioctx->ioctx_errno = -ioctx->ioctx_cc;
		ioctx->ioctx_cc = -1;
	}
	return 0;
}

static int
native_inop_read(struct ioctx *ioctx)
{

	return doio('r', ioctx);
}

static int
native_inop_write(struct ioctx *ioctx)
{

	return doio('w', ioctx);
}

static _SYSIO_OFF_T
native_inop_pos(struct pnode *pno, _SYSIO_OFF_T off)
{
	struct inode *ino;
	struct native_inode *nino;
	int	err;

	ino = pno->p_base->pb_ino;
	assert(ino);
	nino = I2NI(ino);
	err = native_pos(nino->ni_fd, &off, SEEK_SET);
	return err < 0 ? err : off;
}

static int
native_inop_iodone(struct ioctx *ioctxp __IS_UNUSED)
{

	/*
	 * It's always done in this driver. It completed when posted.
	 */
	return 1;
}

static int
native_inop_fcntl(struct pnode *pno,
		  int cmd,
		  va_list ap,
		  int *rtn)
{
	struct inode *ino;
	struct native_inode *nino;
	long	arg;
	int	err;

	ino = pno->p_base->pb_ino;
	assert(ino);
	nino = I2NI(ino);
	if (nino->ni_fd < 0)
		abort();

	err = 0;
	switch (cmd) {
	case F_GETFD:
	case F_GETFL:
#ifdef F_GETOWN
	case F_GETOWN:
#endif
		*rtn = syscall(SYSIO_SYS_fcntl, nino->ni_fd, cmd);
		if (*rtn == -1)
			err = -errno;
		break;
	case F_DUPFD:
	case F_SETFD:
	case F_SETFL:
	case F_GETLK:
	case F_SETLK:
	case F_SETLKW:
#ifdef F_SETOWN
	case F_SETOWN:
#endif
		arg = va_arg(ap, long);
		*rtn = syscall(SYSIO_SYS_fcntl, nino->ni_fd, cmd, arg);
		if (*rtn == -1)
			err = -errno;
		break;
	default:
		*rtn = -1;
		err = -EINVAL;
	}
	return err;
}

static int
native_inop_mknod(struct pnode *pno __IS_UNUSED,
		  mode_t mode __IS_UNUSED,
		  dev_t dev __IS_UNUSED)
{

	return -ENOSYS;
}

#ifdef _HAVE_STATVFS
static int
native_inop_statvfs(struct pnode *pno,
		    struct intnl_statvfs *buf)
{
	char	*path;
	struct inode *ino;
	int    rc;
	struct statfs fs;

	path = NULL;
	ino = pno->p_base->pb_ino;
	if (!ino || I2NI(ino)->ni_fd < 0) {
		path = _sysio_pb_path(pno->p_base, '/');
		if (!path)
			return -ENOMEM;
	}

	/*
	 * The syscall interface does not support SYSIO_SYS_fstatvfs.
	 * Should possibly return ENOSYS, but thought it
	 * better to use SYSIO_SYS_fstatfs and fill in as much of
	 * the statvfs structure as possible.  This allows
	 * for more of a test of the sysio user interface.
	 */
	rc =
	    path
	      ? syscall(SYSIO_SYS_statfs, path, &fs)
	      : syscall(SYSIO_SYS_fstatfs, I2NI(ino)->ni_fd, &fs);
	if (path)
		free(path);
	if (rc < 0)
		return -errno;

	buf->f_bsize = fs.f_bsize;  /* file system block size */
	buf->f_frsize = fs.f_bsize; /* file system fundamental block size */
	buf->f_blocks = fs.f_blocks;
	buf->f_bfree = fs.f_bfree;
	buf->f_bavail = fs.f_bavail;
	buf->f_files = fs.f_files;  /* Total number serial numbers */
	buf->f_ffree = fs.f_ffree;  /* Number free serial numbers */
	buf->f_favail = fs.f_ffree; /* Ditto, for non-privileged though */
	buf->f_fsid = ino->i_fs->fs_id;
	buf->f_flag = 0;
	if (pno)
		buf->f_flag = pno->p_mount->mnt_flags;
	buf->f_namemax = fs.f_namelen;
	return 0;
}
#endif

static int
native_inop_sync(struct pnode *pno)
{
	struct inode *ino;
	int	err;

	ino = pno->p_base->pb_ino;
	assert(ino);
	assert(I2NI(ino)->ni_fd >= 0);

	err = syscall(SYSIO_SYS_fsync, I2NI(ino)->ni_fd);
	if (err)
		err = -errno;
	return err;
}

static int
native_inop_datasync(struct pnode *pno)
{
	struct inode *ino;
	struct native_inode *nino;
	int	err;

	ino = pno->p_base->pb_ino;
	assert(ino);
	nino = I2NI(ino);
	assert(nino->ni_fd >= 0);

#ifdef SYSIO_SYS_fdatasync
	err = syscall(SYSIO_SYS_fdatasync, I2NI(ino)->ni_fd);
#else
#if 0
#warning No fdatasync system call -- Using fsync instead!
#endif
	err = syscall(SYSIO_SYS_fsync, I2NI(ino)->ni_fd);
#endif
	if (err)
		err = -errno;
	return err;
}

static int
native_inop_ioctl(struct pnode *pno __IS_UNUSED,
		  unsigned long int request __IS_UNUSED,
		  va_list ap __IS_UNUSED)
{

	/*
	 * I'm lazy. Maybe implemented later.
	 */
	return -ENOTTY;
}

static void
native_inop_gone(struct inode *ino)
{
	struct native_inode *nino = I2NI(ino);

	if (nino->ni_fd >= 0)
		(void )syscall(SYSIO_SYS_close, nino->ni_fd);

	free(ino->i_private);
}

static void
native_fsop_gone(struct filesys *fs __IS_UNUSED)
{

	free(fs->fs_private);
	/*
	 * Do nothing. There is no private part maintained for the
	 * native file interface.
	 */
}
