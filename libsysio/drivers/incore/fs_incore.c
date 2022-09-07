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
 *    Cplant(TM) Copyright 1998-2003 Sandia Corporation. 
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
 * Albuquerque, NM 87185-1110
 *
 * lee@sandia.gov
 */

#define _GNU_SOURCE
/* never used
#ifdef __linux__
#define _BSD_SOURCE
#endif
*/

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#ifdef _HAVE_STATVFS
#include <sys/statvfs.h>
#endif
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"
#include "fs.h"
#include "mount.h"
#include "inode.h"
#include "dev.h"

#include "fs_incore.h"


/*
 * In-core file system pseudo-driver.
 */

/*
 * Pseudo-blocksize.
 */
#define INCORE_BLKSIZE		(8192)

/*
 * Format of an incore inode.
 */
struct incore_inode {
	LIST_ENTRY(incore_inode) ici_link;		/* i-nodes list link */
	struct intnl_stat ici_st;			/* attrs */
	struct file_identifier ici_fileid;		/* file ID */
	void	*ici_data;				/* file data */
};

/*
 * Given pointer to inode, return pointer to incore-inode.
 */
#define I2IC(ino)	((struct incore_inode *)(ino)->i_private)

struct incore_filesys {
	ino_t	icfs_nxtnum;				/* next inum */
	LIST_HEAD(, incore_inode) icfs_icinodes;	/* all i-nodes list */
};

#define ICFS_INIT(_icfs) \
	do  { \
		(_icfs)->icfs_nxtnum = 1; \
		LIST_INIT(&(_icfs)->icfs_icinodes); \
	} while (0)

/*
 * Given pointer to filesys, return pointer to incore-filesys.
 */
#define FS2ICFS(fs)	((struct incore_filesys *)(fs)->fs_private)

static int _sysio_incore_fsswop_mount(const char *source,
				      unsigned flags,
				      const void *data,
				      struct pnode *tocover,
				      struct mount **mntp);

static struct fssw_ops incore_fssw_ops = {
	        _sysio_incore_fsswop_mount
};

static void _sysio_incore_fsop_gone(struct filesys *fs);

static struct filesys_ops incore_fs_ops = {
	        _sysio_incore_fsop_gone,
};

static int _sysio_incore_dirop_lookup(struct pnode *pno,
				      struct inode **inop,
				      struct intent *intnt,
				      const char *path);
static int _sysio_incore_inop_getattr(struct pnode *pno,
				      struct intnl_stat *stbuf);
static int _sysio_incore_inop_setattr(struct pnode *pno,
				      unsigned mask,
				      struct intnl_stat *stbuf);
static ssize_t _sysio_incore_dirop_filldirentries(struct pnode *pno,
						  _SYSIO_OFF_T *posp,
						  char *buf,
						  size_t nbytes);
static int _sysio_incore_dirop_mkdir(struct pnode *pno, mode_t mode);
static int _sysio_incore_dirop_rmdir(struct pnode *pno);
static int _sysio_incore_inop_open(struct pnode *pno, int flags, mode_t mode);
static int _sysio_incore_inop_close(struct pnode *pno);
static int _sysio_incore_dirop_link(struct pnode *old, struct pnode *new);
static int _sysio_incore_dirop_unlink(struct pnode *pno);
static int _sysio_incore_dirop_rename(struct pnode *old, struct pnode *new);
static int _sysio_incore_filop_read(struct ioctx *ioctx);
static int _sysio_incore_filop_write(struct ioctx *ioctx);
static _SYSIO_OFF_T _sysio_incore_filop_pos(struct pnode *pno,
					    _SYSIO_OFF_T off);
static int _sysio_incore_filop_iodone(struct ioctx *ioctx);
static int _sysio_incore_filop_fcntl(struct pnode *pno, 
				     int cmd, va_list ap, int *rtn);
static int _sysio_incore_inop_sync(struct pnode *pno);
static int _sysio_incore_filop_ioctl(struct pnode *pno,
				    unsigned long int request,
				    va_list ap);
static int _sysio_incore_dirop_mknod(struct pnode *pno, mode_t mode, dev_t dev);
#ifdef _HAVE_STATVFS
static int _sysio_incore_inop_statvfs(struct pnode *pno,
				      struct intnl_statvfs *buf);
#endif
static void _sysio_incore_inop_gone(struct inode *ino);

#define _sysio_incore_dirop_symlink \
	(int (*)(struct pnode *, const char *))_sysio_do_enosys
#define _sysio_incore_dirop_readlink \
	(int (*)(struct pnode *, char *, size_t))_sysio_do_enosys
#define _sysio_incore_dirop_read \
	(int (*)(struct ioctx *))_sysio_do_eisdir
#define _sysio_incore_dirop_write \
	(int (*)(struct ioctx *))_sysio_do_eisdir
#define _sysio_incore_dirop_pos \
	(_SYSIO_OFF_T (*)(struct pnode *, \
			  _SYSIO_OFF_T))_sysio_do_eisdir
#define _sysio_incore_dirop_iodone \
	(int (*)(struct ioctx *))_sysio_do_illop
#define _sysio_incore_dirop_fcntl \
	(int (*)(struct pnode *, int, va_list, int *))_sysio_do_eisdir
#define _sysio_incore_dirop_ioctl \
	(int (*)(struct pnode *, \
		 unsigned long int, \
		 va_list))_sysio_do_eisdir

static struct inode_ops _sysio_incore_dir_ops = {
	_sysio_incore_dirop_lookup,
	_sysio_incore_inop_getattr,
	_sysio_incore_inop_setattr,
	_sysio_incore_dirop_filldirentries,
	NULL,
	_sysio_incore_dirop_mkdir,
	_sysio_incore_dirop_rmdir,
	_sysio_incore_dirop_symlink,
	_sysio_incore_dirop_readlink,
	_sysio_incore_inop_open,
	_sysio_incore_inop_close,
	_sysio_incore_dirop_link,
	_sysio_incore_dirop_unlink,
	_sysio_incore_dirop_rename,
	_sysio_incore_dirop_read,
	_sysio_incore_dirop_write,
	_sysio_incore_dirop_pos,
	_sysio_incore_dirop_iodone,
	_sysio_incore_dirop_fcntl,
	NULL,
	_sysio_incore_inop_sync,
	NULL,
	_sysio_incore_inop_sync,
	_sysio_incore_dirop_ioctl,
	_sysio_incore_dirop_mknod,
#ifdef _HAVE_STATVFS
	_sysio_incore_inop_statvfs,
#endif
	_sysio_p_generic_perms_check,
	_sysio_incore_inop_gone
};

static int _sysio_incore_filop_lookup(struct pnode *pno,
				      struct inode **inop,
				      struct intent *intnt,
				      const char *path);
#define _sysio_incore_filop_filldirentries \
	(ssize_t (*)(struct pnode *, \
		     _SYSIO_OFF_T *, \
		     char *, \
		     size_t))_sysio_do_illop
#define _sysio_incore_filop_mkdir \
	(int (*)(struct pnode *, mode_t))_sysio_do_illop
#define _sysio_incore_filop_rmdir \
	(int (*)(struct pnode *))_sysio_do_illop
#define _sysio_incore_filop_symlink \
	(int (*)(struct pnode *, const char *))_sysio_do_illop
#define _sysio_incore_symlinkop_readlink \
	(int (*)(struct pnode *, char *, size_t))_sysio_do_illop
#define _sysio_incore_filop_link \
	(int (*)(struct pnode *old, struct pnode *new))_sysio_do_illop
#define _sysio_incore_filop_unlink \
	(int (*)(struct pnode *pno))_sysio_do_illop
#define _sysio_incore_filop_rename \
	(int (*)(struct pnode *old, struct pnode *new))_sysio_do_illop
#define _sysio_incore_filop_mknod \
	(int (*)(struct pnode *pno, mode_t, dev_t))_sysio_do_illop

static struct inode_ops _sysio_incore_file_ops = {
	_sysio_incore_filop_lookup,
	_sysio_incore_inop_getattr,
	_sysio_incore_inop_setattr,
	_sysio_incore_filop_filldirentries,
	NULL,
	_sysio_incore_filop_mkdir,
	_sysio_incore_filop_rmdir,
	_sysio_incore_filop_symlink,
	_sysio_incore_symlinkop_readlink,
	_sysio_incore_inop_open,
	_sysio_incore_inop_close,
	_sysio_incore_filop_link,
	_sysio_incore_filop_unlink,
	_sysio_incore_filop_rename,
	_sysio_incore_filop_read,
	_sysio_incore_filop_write,
	_sysio_incore_filop_pos,
	_sysio_incore_filop_iodone,
	_sysio_incore_filop_fcntl,
	NULL,
	_sysio_incore_inop_sync,
	NULL,
	_sysio_incore_inop_sync,
	_sysio_incore_filop_ioctl,
	_sysio_incore_filop_mknod,
#ifdef _HAVE_STATVFS
	_sysio_incore_inop_statvfs,
#endif
	_sysio_p_generic_perms_check,
	_sysio_incore_inop_gone
};

static struct inode_ops _sysio_incore_dev_ops = {
	_sysio_incore_filop_lookup,
	_sysio_incore_inop_getattr,
	_sysio_incore_inop_setattr,
	_sysio_incore_filop_filldirentries,
	NULL,
	_sysio_incore_filop_mkdir,
	_sysio_incore_filop_rmdir,
	_sysio_incore_filop_symlink,
	_sysio_incore_symlinkop_readlink,
	_sysio_nodev_inop_open,
	_sysio_nodev_inop_close,
	_sysio_incore_filop_link,
	_sysio_incore_filop_unlink,
	_sysio_incore_filop_rename,
	_sysio_nodev_inop_read,
	_sysio_nodev_inop_write,
	_sysio_nodev_inop_pos,
	_sysio_nodev_inop_iodone,
	_sysio_incore_filop_fcntl,
	NULL,
	_sysio_incore_inop_sync,
	NULL,
	_sysio_nodev_inop_sync,
	_sysio_nodev_inop_ioctl,
	_sysio_incore_filop_mknod,
#ifdef _HAVE_STATVFS
	_sysio_incore_inop_statvfs,
#endif
	_sysio_p_generic_perms_check,
	_sysio_incore_inop_gone
};

typedef void *(*probe_ty)(void *data, size_t len, void *arg);

/*
 * Lookup data argument bundle record.
 */
struct lookup_data {
	struct qstr *name;				/* desired entry name */
	struct intnl_dirent *de;			/* last dirent */
	size_t	minsiz;					/* min hole needed */
	struct {
		void	*p;				/* best hole */
		size_t	len;				/* best hole len */
	} hole;
};

/*
 * Initialize lookup data argument bundle.
 */
#define INCORE_LD_INIT(ld, minsz, qs) \
	do { \
		(ld)->name = (qs); \
		(ld)->de = NULL; \
		(ld)->minsiz = (minsz); \
		(ld)->hole.p = NULL; \
		(ld)->hole.len = 0; \
	} while (0)

/*
 * Calculate size of a directory entry given length of the entry name.
 */
#define INCORE_D_RECLEN(namlen) \
	(((size_t )&((struct intnl_dirent *)0)->d_name + \
	  (namlen) + 1 + sizeof(void *)) & \
	 ~(sizeof(void *) - 1))

/*
 * Given mode bits, return directory entry type code.
 */
#define INCORE_D_TYPEOF(m)	(((m) & S_IFMT) >> 12)

static char incore_dir_template[INCORE_D_RECLEN(1) + INCORE_D_RECLEN(2)];
#if 0
static struct intnl_dirent incore_dir_template[] = {
	{
		0,
		INCORE_D_RECLEN(1),
		INCORE_D_RECLEN(1),
		INCORE_D_TYPEOF(S_IFDIR),
		{ '.', '\0' }
	},
	{
		0,
		INCORE_D_RECLEN(1) + INCORE_D_RECLEN(2),
		INCORE_D_RECLEN(2),
		INCORE_D_TYPEOF(S_IFDIR),
		{ '.', '.', '\0' }
	}
};
#endif

/*
 * Initialize this driver.
 */
int
_sysio_incore_init()
{
	struct intnl_dirent *de;
	off_t	off;

	/*
	 * Fill in the directory template.
	 */
	de = (struct intnl_dirent *)incore_dir_template;
#ifdef _DIRENT_HAVE_D_OFF
	de->d_off =
#endif
	    off = de->d_reclen = INCORE_D_RECLEN(1);
	de->d_type = INCORE_D_TYPEOF(S_IFDIR);
	de->d_name[0] = '.';
#ifdef _DIRENT_HAVE_D_NAMLEN
	de->d_namlen = 1;
#endif
	/*
	 * Move to entry for `..'
	 */
	de = (struct intnl_dirent *)((char *)de + off);
	de->d_reclen = INCORE_D_RECLEN(2);
#ifdef _DIRENT_HAVE_D_NAMLEN
	de->d_namlen = 2;
#endif
#ifdef _DIRENT_HAVE_D_OFF
	de->d_off =
#endif
	    off += de->d_reclen;
	de->d_type = INCORE_D_TYPEOF(S_IFDIR);
	de->d_name[0] = de->d_name[1] = '.';
	de->d_name[2] = ' ';

	return _sysio_fssw_register("incore", &incore_fssw_ops);
}

static ino_t
incore_inum_alloc(struct incore_filesys *icfs)
{

	assert(icfs->icfs_nxtnum);
	return icfs->icfs_nxtnum++;
}

static struct incore_inode *
incore_i_alloc(struct incore_filesys *icfs, struct intnl_stat *st)
{
	struct incore_inode *icino;

	assert(st->st_ino);
	assert(!st->st_size);

	icino = malloc(sizeof(struct incore_inode));
	if (!icino)
		return NULL;
	icino->ici_st = *st;
	icino->ici_fileid.fid_data = &icino->ici_st.st_ino;
	icino->ici_fileid.fid_len = sizeof(icino->ici_st.st_ino);
	icino->ici_data = NULL;

	LIST_INSERT_HEAD(&icfs->icfs_icinodes, icino, ici_link);

	return icino;
}

static int
incore_trunc(struct incore_inode *icino, _SYSIO_OFF_T size, int clear)
{
	_SYSIO_OFF_T n;
	void	*p;

	if (size < 0) 
		return -EINVAL;
	n = size;
	if (!size) {
		if (icino->ici_data) {
			free(icino->ici_data);
			icino->ici_data = NULL;
		}
		n = 0;
		goto out;
	}
	p = realloc(icino->ici_data, (size_t )n);
	if (!p)
		return -ENOSPC;
	icino->ici_data = p;
	if (clear && n > icino->ici_st.st_size)
		(void )memset((char *)icino->ici_data + icino->ici_st.st_size,
			      0,
			      (size_t )(n - icino->ici_st.st_size));
out:
	icino->ici_st.st_size = n;
	icino->ici_st.st_blocks =
	    (n + icino->ici_st.st_blksize - 1) / icino->ici_st.st_blksize;
	icino->ici_st.st_mtime = time(NULL);
	return 0;
}

static void
incore_i_destroy(struct incore_inode *icino)
{

	LIST_REMOVE(icino, ici_link);
	(void )incore_trunc(icino, 0, 0);
	free(icino);
}

static struct incore_inode *
incore_directory_new(struct incore_filesys *icfs,
		     struct incore_inode *parent,
		     struct intnl_stat *st)
{
	struct incore_inode *icino;
	int	err;
	struct intnl_dirent *de;

	icino = incore_i_alloc(icfs, st);
	if (!icino)
		return NULL;

	if (!parent)
		parent = icino;				/* root */

	/*
	 * Allocate and init directory data.
	 */
	err = incore_trunc(icino, sizeof(incore_dir_template), 1);
	if (err) {
		incore_i_destroy(icino);
		return NULL;
	}
	(void )memcpy(icino->ici_data,
		      &incore_dir_template,
		      sizeof(incore_dir_template));
	de = icino->ici_data;
	de->d_ino = st->st_ino;
	de =
	    (struct intnl_dirent *)((char *)de +
#ifdef _DIRENT_HAVE_D_OFF
				    de->d_off
#else
				    de->d_reclen
#endif
				    );
	de->d_ino = parent->ici_st.st_ino;

	/*
	 * Set creation time to modify time set by truncate.
	 */
	st->st_ctime = st->st_mtime;

	return icino;
}

static int
_sysio_incore_fsswop_mount(const char *source,
			   unsigned flags,
			   const void *data __IS_UNUSED,
			   struct pnode *tocover,
			   struct mount **mntp)
{
	char	*cp;
	unsigned long ul;
	long	l;
	mode_t	mode;
	uid_t	uid;
	gid_t	gid;
	int	err;
	dev_t	dev;
	struct intnl_stat stat;
	struct incore_filesys *icfs;
	ino_t	inum;
	struct incore_inode *icino;
	struct filesys *fs;
	struct inode *rooti;
	struct mount *mnt;

	/*
	 * Source is a specification for the root attributes of this
	 * new file system in the format:
	 *
	 * <permissions>[+<owner>][+<group>]
	 */
	ul = strtoul(source, &cp, 0);
	mode = (mode_t )ul & 07777;
	uid = getuid();					/* default */
	gid = getgid();					/* default */
	if (*cp != '\0') {
		/*
		 * Get user and/or group.
		 */
		if (*cp != '+' ||
		    (ul == ULONG_MAX && errno == ERANGE) ||
		    (unsigned long)mode != ul ||
		    mode > 07777)
			return -EINVAL;
		source = cp;
		l = strtol(source, &cp, 0);
		uid = (uid_t )l;
		if (((l == LONG_MIN || l == LONG_MAX) &&
		     errno == ERANGE) ||
		    (long )uid != l)
			return -EINVAL;
		if (*cp != '+')
			return -EINVAL;
		source = cp;
		l = strtol(source, &cp, 0);
		gid = (gid_t )l;
		if (((l == LONG_MIN || l == LONG_MAX) &&
		     errno == ERANGE) ||
		    (long )gid != l)
			return -EINVAL;
		if (*cp != '\0')
			return -EINVAL;
	}

	err = 0;

	dev = _sysio_dev_alloc();

	mnt = NULL;
	rooti = NULL;
	fs = NULL;
	icino = NULL;
	icfs = NULL;

	/*
	 * Create new FS.
	 */
	icfs = malloc(sizeof(struct incore_filesys));
	if (!icfs) {
		err = -ENOMEM;
		goto error;
	}
	ICFS_INIT(icfs);

	/*
	 * Create root i-node.
	 */
	(void )memset(&stat, 0, sizeof(stat));
	stat.st_dev = dev;
	inum = incore_inum_alloc(icfs);
#ifdef HAVE__ST_INO
	stat.__st_ino = inum; 
#endif
	stat.st_mode = S_IFDIR | (mode & 07777);
	stat.st_nlink = 2;
	stat.st_uid = uid;
	stat.st_gid = gid;
	stat.st_size = 0;
	stat.st_blksize = INCORE_BLKSIZE;
	stat.st_blocks = 0;
	stat.st_ctime = stat.st_mtime = stat.st_atime = 0;
	stat.st_ino = inum;
	icino = incore_directory_new(icfs, NULL, &stat);
	if (!icino)
		return -ENOSPC;
	icino->ici_st.st_atime = icino->ici_st.st_mtime;

	fs =
	    _sysio_fs_new(&incore_fs_ops,
			  (flags & MOUNT_F_RO) ? FS_F_RO : 0,
			  icfs);
	if (!fs) {
		err = -ENOMEM;
		goto error;
	}

	/*
	 * Create root for system.
	 *
	 * Persistent across remounts because we ask for immunity.
	 */
	rooti =
	    _sysio_i_new(fs,
			 &icino->ici_fileid,
			 &icino->ici_st,
			 1,
			 &_sysio_incore_dir_ops,
			 icino);
	if (!rooti) {
		err = -ENOMEM;
		goto error;
	}

	/*
	 * Have path-node specified by the given source argument. Let the
	 * system finish the job, now.
	 */
	mnt = NULL;
	err = _sysio_mounti(fs, rooti, flags, tocover, &mnt);
	I_PUT(rooti);
	rooti = NULL;
	if (err)
		goto error;

	*mntp = mnt;

	goto out;

error:
	if (mnt && _sysio_do_unmount(mnt) != 0)
			abort();
	if (rooti)
		I_RELE(rooti);
	if (fs) {
		FS_RELE(fs);
		goto out;
	}
	if (icino) {
		incore_i_destroy(icino);
		goto out;
	}
	if (icfs) {
		free(icfs);
		goto out;
	}

out:
	return err;
}

static void
_sysio_incore_fsop_gone(struct filesys *fs)
{
	struct incore_filesys *icfs;
	struct incore_inode *icino, *oicino;

	icfs = FS2ICFS(fs);

	/*
	 * Free up i-node resource associated with this file system.
	 */
	icino = icfs->icfs_icinodes.lh_first;
	while (icino) {
		oicino = icino;
		icino = icino->ici_link.le_next;
		incore_i_destroy(oicino);
	}

	/*
	 * Free the FS record.
	 */
	free(icfs);
}

/*
 * A directory search engine. Various functions are carried out by
 * supplying appropriate callback functions.
 *
 * The two arguments, entry and hole, are called, if not null, for each
 * directory entry and hole, respectively.
 */
static void *
incore_directory_probe(void *data,
		       size_t siz,
		       _SYSIO_OFF_T origin
#ifndef _DIRENT_HAVE_D_OFF
				__IS_UNUSED
#endif
		       ,
		       probe_ty entry,
		       probe_ty hole,
		       void *arg)
{
	struct intnl_dirent *de;
	void	*p;
	size_t	n;

	de = data;
	for (;;) {
#ifdef _DIRENT_HAVE_D_OFF
		assert(de->d_off);
#else
		assert(de->d_reclen);
#endif
		if (entry && (p = (*entry)(de, de->d_reclen, arg)))
			return p;
		n =
#ifdef _DIRENT_HAVE_D_OFF
		    de->d_off - origin;
#else
		    ((void *)de - data) + de->d_reclen;
#endif
		if (hole) {
			p = (*hole)((void *)de, de->d_reclen, arg);
			if (p)
				return p;
		}
		if (n >= siz)
			break;
		de = (struct intnl_dirent *)((char *)data + n);
	}

	return NULL;
}

static struct intnl_dirent *
incore_directory_match(struct intnl_dirent *de,
		       size_t reclen,
		       struct lookup_data *ld)
{
	size_t	len;

#if defined(BSD) || defined(REDSTORM)
	if (IFTODT(de->d_type) == DT_WHT)
		return NULL;
#endif
#ifdef _DIRENT_HAVE_D_NAMLEN
	len = de->d_namlen;
#else
	{
		const char *cp, *end;

		cp = de->d_name;
		end = (const char *)de + reclen;
		while (cp < end && *cp != '\0')
			cp++;
		len = cp - de->d_name;
	}
#endif
	if (ld->name->len == len &&
	    strncmp(de->d_name, ld->name->name, ld->name->len) == 0)
		return de;
	ld->de = de;
	return NULL;
}

static int
_sysio_incore_dirop_lookup(struct pnode *pno,
			   struct inode **inop,
			   struct intent *intnt __IS_UNUSED,
			   const char *path __IS_UNUSED)
{
	struct inode *ino;
	struct intnl_dirent *de;
	struct incore_inode *icino;
	struct lookup_data lookup_data;
	struct file_identifier fileid;
#ifdef notdef
	struct inode_ops *ops;
#endif

	/*
	 * Revalidate?
	 */
	if (*inop) {
		icino = I2IC(*inop);
		assert(icino);
		(*inop)->i_stbuf = icino->ici_st;
		return 0;
	}

	ino = pno->p_parent->p_base->pb_ino;
	icino = I2IC(ino);
	INCORE_LD_INIT(&lookup_data,
		       ULONG_MAX,
		       &pno->p_base->pb_key.pbk_name);
	de =
	    incore_directory_probe(icino->ici_data,
				   icino->ici_st.st_size,
				   0,
				   (probe_ty )incore_directory_match,
				   NULL,
				   &lookup_data);
	if (!de)
		return -ENOENT;

	fileid.fid_data = &de->d_ino;
	fileid.fid_len = sizeof(de->d_ino);
	ino =
	    _sysio_i_find(ino->i_fs, &fileid);
#ifdef notdef
	if (ino)
		goto out;
	icino->ici_fileid.fid_data = &icino->ici_st.st_ino;
	icino->ici_fileid.fid_len = sizeof(icino->ici_st.st_ino);
	ops = NULL;
	switch (icino->ici_st.st_mode & S_IFMT) {
	case S_IFDIR:
		ops = &_sysio_incore_dir_ops;
		break;
	case S_IFREG:
		ops = &_sysio_incore_file_ops;
		break;
	default:
		break;
	}
	if (!ops)
		abort();
	ino =
	    _sysio_i_new(ino->i_fs,
			 &icino->ici_fileid,
			 &icino->ici_st
			 1,
			 ops,
			 icino);
#endif
	if (!ino)
		return -ENOMEM;

#ifdef notdef
out:
#endif
	*inop = ino;
	return 0;
}

static int
_sysio_incore_filop_lookup(struct pnode *pno __IS_UNUSED,
			   struct inode **inop,
			   struct intent *intnt __IS_UNUSED,
			   const char *path __IS_UNUSED)
{
	struct incore_inode *icino;

	/*
	 * We revalidate only.
	 */
	if (!*inop)
		return -ENOTDIR;

	icino = I2IC(*inop);
	assert(icino);
	(*inop)->i_stbuf = icino->ici_st;
	return 0;
}

static int
_sysio_incore_inop_getattr(struct pnode *pno,
			   struct intnl_stat *stbuf)
{
	struct incore_inode *icino;

	assert(pno->p_base->pb_ino);
	icino = I2IC(pno->p_base->pb_ino);
	*stbuf = icino->ici_st;
	return 0;
}

static int
_sysio_incore_inop_setattr(struct pnode *pno,
			   unsigned mask,
			   struct intnl_stat *stbuf)
{
	struct inode *ino;
	struct incore_inode *icino;
	int	err;

	ino = pno->p_base->pb_ino;
	if (!ino)
		return -EBADF;
	icino = I2IC(ino);

	err = 0;
	if (mask & SETATTR_LEN) {
		err = incore_trunc(icino, stbuf->st_size, 1);
		if (err)
			goto out;
		mask &= ~SETATTR_LEN;
	}
	if (mask & SETATTR_MODE) {
		icino->ici_st.st_mode =
		    (icino->ici_st.st_mode & S_IFMT) | (stbuf->st_mode & 07777);
	}
	if (mask & SETATTR_MTIME)
		icino->ici_st.st_mtime = stbuf->st_mtime;
	if (mask & SETATTR_ATIME)
		icino->ici_st.st_atime = stbuf->st_atime;
	if (mask & SETATTR_UID)
		icino->ici_st.st_uid = stbuf->st_uid;
	if (mask & SETATTR_GID)
		icino->ici_st.st_gid = stbuf->st_gid;
	icino->ici_st.st_ctime = time(NULL);

	ino->i_stbuf = icino->ici_st;
out:
	return err;
}

static void *
incore_directory_position(struct intnl_dirent *de,
			  size_t reclen __IS_UNUSED,
			  void *p)
{

	return (void *)de >= p ? de : NULL;
}

struct copy_info {
	void	*data;
	size_t	nbytes;
	unsigned count;
};

/*
 * Eumeration callback.
 *
 * Note:
 * Whiteout entries are never returned.
 */
static void *
incore_directory_enumerate(struct intnl_dirent *de,
			   size_t reclen,
			   struct copy_info *cinfo) {

#ifdef DT_WHT
	if (de->d_type == DT_WHT) {
		/*
		 * Keep going  but skip the copy.
		 */
		return NULL;
	}
#endif
	cinfo->count++;
	if (reclen > cinfo->nbytes)
		return de;
	(void *)memcpy(cinfo->data, de, reclen);
	cinfo->data = (char *)cinfo->data + reclen;
	cinfo->nbytes -= reclen;
	return NULL;
}

static ssize_t
_sysio_incore_dirop_filldirentries(struct pnode *pno,
				   _SYSIO_OFF_T *posp,
				   char *buf,
				   size_t nbytes)
{
	struct incore_inode *icino;
	off_t	off;
	struct intnl_dirent *de;
	struct copy_info copy_info;

	assert(pno->p_base->pb_ino);
	icino = I2IC(pno->p_base->pb_ino);
	if (*posp >= icino->ici_st.st_size)
		return 0;

	de =
	    incore_directory_probe(icino->ici_data,
				   icino->ici_st.st_size,
				   *posp,
				   (probe_ty )incore_directory_position,
				   NULL,
				   (char *)icino->ici_data + *posp);
	if (!de) {
		/*
		 * Past EOF.
		 */
		return 0;
	}

	copy_info.data = buf;
	copy_info.nbytes = nbytes;
	copy_info.count = 0;
	off = (char *)de - (char *)icino->ici_data;
	de =
	    incore_directory_probe(de,
				   icino->ici_st.st_size - off,
				   off,
				   (probe_ty )incore_directory_enumerate,
				   NULL,
				   &copy_info);
	icino->ici_st.st_atime = time(NULL);
	if (nbytes == copy_info.nbytes && copy_info.count)
		return -EINVAL;
	nbytes -= copy_info.nbytes;
#if 0
	if (!nbytes)
		return -EOVERFLOW;
#endif
	*posp += nbytes;
	return (ssize_t )nbytes;
}

static struct intnl_dirent *
incore_directory_best_fit(void *data, size_t len, struct lookup_data *ld)
{

	if (!ld->hole.len || len < ld->hole.len) {
		ld->hole.p = data;
		ld->hole.len = len;
	}

	return NULL;
}

static int
incore_directory_insert(struct incore_inode *parent,
			struct qstr *name,
			ino_t inum,
			unsigned char type)
{
	size_t	reclen;
	struct lookup_data lookup_data;
	struct intnl_dirent *de;
	size_t	xt;
	size_t	n;
	size_t	r;

	reclen = INCORE_D_RECLEN(name->len);
	INCORE_LD_INIT(&lookup_data, reclen, name);
	de =
	    incore_directory_probe(parent->ici_data,
				   parent->ici_st.st_size,
				   0,
				   (probe_ty )incore_directory_match,
				   (probe_ty )incore_directory_best_fit,
				   &lookup_data);
	if (de)
		return -EEXIST;
	de = lookup_data.de;
	xt = (char *)lookup_data.de - (char *)parent->ici_data;
	n =
#ifdef _DIRENT_HAVE_D_OFF
	    de->d_off;
#else
	    xt + de->d_reclen;
#endif
	r =
#ifdef _DIRENT_HAVE_D_OFF
	    de->d_reclen;
#else
	    INCORE_D_RECLEN(de->d_namlen);
#endif
	if (!parent->ici_st.st_size ||
	    xt + r + reclen > (size_t )parent->ici_st.st_size) {
		int	err;

		err = incore_trunc(parent, xt + r + reclen, 1);
		if (err)
			return err;
		de = (struct intnl_dirent *)((char *)parent->ici_data + xt);
		n = parent->ici_st.st_size;
	}

#ifdef _DIRENT_HAVE_D_OFF
	de->d_off = xt + r;				/* trim */
#else
	de->d_reclen = r;
#endif
	de = (struct intnl_dirent *)((char *)de + r);				/* reposition */
	xt += r;

#ifndef _DIRENT_HAVE_D_OFF
	/*
	 * Will we split this hole or use all of it?
	 */
	if (lookup_data.hole.len - reclen &&
	    lookup_data.hole.len - reclen <= INCORE_D_RECLEN(1))
		reclen = lookup_data.hole.len;
#endif

	/*
	 * Insert new.
	 */
	de->d_ino = inum;
#ifdef _DIRENT_HAVE_D_OFF
	de->d_off = n;
#endif
	de->d_reclen = reclen;
	de->d_type = type;
	(void )memcpy(de->d_name, name->name, name->len);
#ifdef _DIRENT_HAVE_D_NAMLEN
	de->d_namlen = name->len;
#endif

#ifndef _DIRENT_HAVE_D_OFF
	xt += reclen;
	if (n - xt) {
		/*
		 * White-out remaining part of the hole.
		 */
		(void *)de += reclen;
		de->d_ino = 0;
		de->d_reclen = n - xt;
		de->d_type = DT_WHT;
		de->d_namlen = 0;
	}
#endif

	/*
	 * Update attributes to reflect the new entry.
	 */
	parent->ici_st.st_nlink++;
	assert(parent->ici_st.st_nlink);
	parent->ici_st.st_atime = parent->ici_st.st_mtime = time(NULL);

	return 0;
}

static int
_sysio_incore_dirop_mkdir(struct pnode *pno, mode_t mode)
{
	struct intnl_stat stat;
	struct incore_inode *icino, *parent;
	ino_t	inum;
	int	err;
	struct intnl_dirent *de = NULL;
	struct inode *ino;

	ino = pno->p_parent->p_base->pb_ino;
	parent = I2IC(ino);

	if (!S_ISDIR(parent->ici_st.st_mode))
		return -ENOTDIR;

	(void )memset(&stat, 0, sizeof(stat));
	stat.st_dev = pno->p_parent->p_base->pb_ino->i_fs->fs_dev;
	inum = incore_inum_alloc(FS2ICFS(ino->i_fs));
#ifdef HAVE__ST_INO
	stat.__st_ino = inum;
#endif
	stat.st_mode = S_IFDIR | (mode & 07777);
	stat.st_nlink = 2;
	stat.st_uid = getuid();
	stat.st_gid = getgid();
	stat.st_size = 0;
	stat.st_blksize = 4096;
	stat.st_blocks = 0;
	stat.st_ctime = stat.st_mtime = stat.st_atime = 0;
	stat.st_ino = inum;
	icino = incore_directory_new(FS2ICFS(ino->i_fs), parent, &stat);
	if (!icino)
		return -ENOSPC;

	/*
	 * Tell the system about the new inode.
	 *
	 * Persistent across remounts because we ask for immunity.
	 */
	ino =
	    _sysio_i_new(pno->p_parent->p_base->pb_ino->i_fs,
			 &icino->ici_fileid,
			 &stat,
			 1,
			 &_sysio_incore_dir_ops,
			 icino);
	if (!ino) {
		incore_i_destroy(icino);
		return -ENOMEM;
	}

	/*
	 * Insert into parent.
	 */
	err =
	    incore_directory_insert(parent,
				    &pno->p_base->pb_key.pbk_name,
				    stat.st_ino,
				    INCORE_D_TYPEOF(S_IFDIR));

	if (err) {
		de->d_ino = 0;				/* bad parent */
		I_RELE(ino);
		_sysio_i_gone(ino);
		return err;
	}

	I_PUT(ino);
	return 0;
}

static int
incore_unlink_entry(struct incore_inode *icino,
		    struct qstr *name)
{
	struct lookup_data lookup_data;
	struct intnl_dirent *de;
	size_t	reclen;
#ifdef _DIRENT_HAVE_D_OFF
	size_t	off;
#endif

	if (!S_ISDIR(icino->ici_st.st_mode))
		return -ENOTDIR;

	INCORE_LD_INIT(&lookup_data, 0, name);
	de =
	    incore_directory_probe(icino->ici_data,
				   icino->ici_st.st_size,
				   0,
				   (probe_ty )incore_directory_match,
				   NULL,
				   &lookup_data);
	if (!de)
		return -ENOENT;
	assert((size_t )((char *)de - (char *)icino->ici_data) >=
	       sizeof(incore_dir_template));
#ifndef _DIRENT_HAVE_D_OFF
	reclen = de->d_reclen;
#else
	off = de->d_off;
	reclen = off - ((char *)de - (char *)icino->ici_data);
#endif
	(void )memset(de, 0, reclen);
#ifndef _DIRENT_HAVE_D_OFF
	de->d_type = (__uint8_t )DTTOIF(DT_WHT);
	de->d_reclen = reclen;
#else
	lookup_data.de->d_off = off;
#endif

	/*
	 * Adjust link count.
	 */
	assert(icino->ici_st.st_nlink > 2);
	icino->ici_st.st_nlink--;

	return 0;
}

static int
_sysio_incore_dirop_rmdir(struct pnode *pno)
{
	struct inode *ino = pno->p_base->pb_ino;
	struct incore_inode *icino = I2IC(ino);
	int	err;

	if (!pno->p_base->pb_key.pbk_name.len ||
	    (pno->p_base->pb_key.pbk_name.name[0] == '.' &&
	     (pno->p_base->pb_key.pbk_name.len == 1 ||
	      (pno->p_base->pb_key.pbk_name.len == 2 &&
	       pno->p_base->pb_key.pbk_name.name[1] == '.'))))
		return -EINVAL;

	if (!S_ISDIR(icino->ici_st.st_mode))
		return -ENOTDIR;

	if (icino->ici_st.st_nlink > 2)
		return -ENOTEMPTY;

	err =
	    incore_unlink_entry(I2IC(pno->p_parent->p_base->pb_ino),
				&pno->p_base->pb_key.pbk_name);
	return err;
}

static int
incore_create(struct pnode *pno, struct intnl_stat *stat)
{
	struct inode *dino, *ino;
	struct incore_inode *icino;
	int	err;

	dino = pno->p_parent->p_base->pb_ino;
	assert(dino);

	icino = incore_i_alloc(FS2ICFS(dino->i_fs), stat);
	if (!icino)
		return -ENOSPC;

	/*
	 * Tell the system about the new inode.
	 */
	ino =
	    _sysio_i_new(dino->i_fs,
			 &icino->ici_fileid,
			 stat,
			 1,
			 S_ISREG(stat->st_mode)
			   ? &_sysio_incore_file_ops
			   : &_sysio_incore_dev_ops,
			 icino);
	if (!ino) {
		incore_i_destroy(icino);
		return -ENOMEM;
	}

	/*
	 * Insert into parent.
	 */
	err =
	    incore_directory_insert(I2IC(dino),
				    &pno->p_base->pb_key.pbk_name,
				    stat->st_ino,
				    INCORE_D_TYPEOF(icino->ici_st.st_mode));
	if (err) {
		I_RELE(ino);
		_sysio_i_gone(ino);
		return err;
	}

	I_PUT(ino);
	return 0;
}

static int
_sysio_incore_inop_open(struct pnode *pno, int flags __IS_UNUSED, mode_t mode)
{
	struct intnl_stat stat;
	ino_t	inum;

	/*
	 * File exists. Nothing to do.
	 */
	if (pno->p_base->pb_ino)
		return 0;

	/*
	 * Must create a new, regular, file.
	 */
	(void )memset(&stat, 0, sizeof(stat));
	stat.st_dev = pno->p_parent->p_base->pb_ino->i_fs->fs_dev;
	inum = incore_inum_alloc(FS2ICFS(pno->p_parent->p_base->pb_ino->i_fs));
#ifdef HAVE__ST_INO
	stat.__st_ino = inum;
#endif
	stat.st_mode = S_IFREG | (mode & 07777);
	stat.st_nlink = 1;
	stat.st_uid = getuid();
	stat.st_gid = getgid();
	stat.st_rdev = 0;
	stat.st_size = 0;
	stat.st_blksize = 4096;
	stat.st_blocks = 0;
	stat.st_ctime = stat.st_mtime = stat.st_atime = 0;
	stat.st_ino = inum;

	return incore_create(pno, &stat);
}

static int
_sysio_incore_inop_close(struct pnode *pno __IS_UNUSED)
{

	return 0;
}

static int
_sysio_incore_dirop_link(struct pnode *old, struct pnode *new)
{
	struct incore_inode *icino = I2IC(old->p_base->pb_ino);
	int	err;

	assert(!new->p_base->pb_ino);
	assert(!S_ISDIR(old->p_base->pb_ino->i_stbuf.st_mode));

	/*
	 * Can bump the link count?
	 */
	if (!(icino->ici_st.st_nlink + 1))
		return -EMLINK;
	/*
	 * Insert into parent.
	 */
	err =
	    incore_directory_insert(I2IC(new->p_parent->p_base->pb_ino),
				    &new->p_base->pb_key.pbk_name,
				    icino->ici_st.st_ino,
				    INCORE_D_TYPEOF(icino->ici_st.st_mode));
	if (err)
		return err;
	/*
	 * Bump the link count.
	 */
	icino->ici_st.st_nlink++;

	return 0;
}

static int
_sysio_incore_dirop_rename(struct pnode *old, struct pnode *new)
{
	int	err;
	struct incore_inode *icino = I2IC(old->p_base->pb_ino);

	if (new->p_base->pb_ino) {
		/*
		 * Have to kill off the target first.
		 */
		if (S_ISDIR(I2IC(new->p_base->pb_ino)->ici_st.st_mode) &&
		    I2IC(new->p_base->pb_ino)->ici_st.st_nlink > 2)
			return -ENOTEMPTY;
		err =
		    incore_unlink_entry(I2IC(new->p_parent->p_base->pb_ino),
					&new->p_base->pb_key.pbk_name);
		if (err)
			return err;
	}

	/*
	 * Insert into new parent.
	 */
	err =
	    incore_directory_insert(I2IC(new->p_parent->p_base->pb_ino),
				    &new->p_base->pb_key.pbk_name,
				    icino->ici_st.st_ino,
				    INCORE_D_TYPEOF(icino->ici_st.st_mode));
	if (err)
		abort();
	/*
	 * Remove from the old parent.
	 */
	err =
	    incore_unlink_entry(I2IC(old->p_parent->p_base->pb_ino),
				&old->p_base->pb_key.pbk_name);
	if (err)
		abort();

	if (S_ISDIR(icino->ici_st.st_mode)) {
		struct intnl_dirent *de;

		/*
		 * We moved a directory. The entry for `..' must be corrected.
		 */
		de = icino->ici_data;
		de++;
		assert(strcmp(de->d_name, "..") == 0);
		de->d_ino = I2IC(new->p_parent->p_base->pb_ino)->ici_st.st_ino;
	}
	return 0;
}

static int
_sysio_incore_dirop_unlink(struct pnode *pno)
{
	struct inode *ino = pno->p_base->pb_ino;
	struct incore_inode *icino = I2IC(ino);
	int	err;

	if (S_ISDIR(icino->ici_st.st_mode))
		return -EISDIR;

	err =
	    incore_unlink_entry(I2IC(pno->p_parent->p_base->pb_ino),
				&pno->p_base->pb_key.pbk_name);
	return err;
}

static int
doio(ssize_t (*f)(void *, size_t, _SYSIO_OFF_T, struct incore_inode *),
     struct ioctx *ioctx)
{

	ioctx->ioctx_cc =
	    _sysio_doio(ioctx->ioctx_xtv, ioctx->ioctx_xtvlen,
			ioctx->ioctx_iov, ioctx->ioctx_iovlen,
			(ssize_t (*)(void *, size_t, _SYSIO_OFF_T, void *))f,
			I2IC(ioctx->ioctx_pno->p_base->pb_ino));
	if (ioctx->ioctx_cc  < 0) {
		ioctx->ioctx_errno = -ioctx->ioctx_cc;
		ioctx->ioctx_cc = -1;
	}
	ioctx->ioctx_done = 1;

	return 0;
}

static ssize_t
incore_read(void *buf, size_t nbytes,
	    _SYSIO_OFF_T off,
	    struct incore_inode *icino)
{
	size_t	n;

	if (off < 0)
		return -EINVAL;
	if (!nbytes || off > icino->ici_st.st_size)
		return 0;
	n = icino->ici_st.st_size - (size_t )off;
	if (n > nbytes)
		n = nbytes;
	(void )memcpy(buf, (char *)icino->ici_data + off, (size_t )n);

	return (ssize_t )n;
}

static int
_sysio_incore_filop_read(struct ioctx *ioctx)
{
	

	return doio(incore_read, ioctx);
}

static ssize_t
incore_write(const void *buf, size_t nbytes,
	     _SYSIO_OFF_T off,
	     struct incore_inode *icino)
{
	_SYSIO_OFF_T pos;

	if (off < 0)
		return -EINVAL;
	if (!nbytes || off > icino->ici_st.st_size)
		return 0;
	pos = off + nbytes;
	if (off && pos <= off) {
		/*
		 * It's all or nothing. We won't write just part of
		 * the buffer.
		 */
		return -EFBIG;
	}
	if (pos > icino->ici_st.st_size) {
		int	err;

		err = incore_trunc(icino, (size_t )pos, 0);
		if (err)
			return err;
	}
	(void )memcpy((char *)icino->ici_data + off, buf, nbytes);

	return (ssize_t )nbytes;
}

static int
_sysio_incore_filop_write(struct ioctx *ioctx)
{

	return doio((ssize_t (*)(void *, size_t,
				 _SYSIO_OFF_T,
				 struct incore_inode *))incore_write,
		    ioctx);
}

static _SYSIO_OFF_T
_sysio_incore_filop_pos(struct pnode *pno __IS_UNUSED, _SYSIO_OFF_T off)
{

	return off;
}

static int
_sysio_incore_filop_iodone(struct ioctx *iocp __IS_UNUSED)
{

	/*
	 * It's always done in this driver. It completed when posted.
	 */
	return 1;
}

static int
_sysio_incore_filop_fcntl(struct pnode *pno __IS_UNUSED,
			  int cmd __IS_UNUSED,
			  va_list ap __IS_UNUSED,
			  int *rtn)
{

	/*
	 * No fcntl's supported.
	 */
	*rtn = -1;
	return -ENOTTY;
}

static int
_sysio_incore_inop_sync(struct pnode *pno __IS_UNUSED)
{

	/*
	 * With what?
	 */
	return 0;
}

static int
_sysio_incore_filop_ioctl(struct pnode *pno __IS_UNUSED,
			  unsigned long int request __IS_UNUSED,
			  va_list ap __IS_UNUSED)
{

	/*
	 * No ioctl's supported.
	 */
	return -ENOTTY;
}

static int
_sysio_incore_dirop_mknod(struct pnode *pno, mode_t mode, dev_t dev)
{
	mode_t	m;
	struct intnl_stat stat;
	ino_t	inum;

	assert(!pno->p_base->pb_ino);

	m = mode & S_IFMT;
	if (S_ISCHR(m))
		m &= ~S_IFCHR;
	else if (S_ISFIFO(m))
		m &= ~S_IFIFO;
	else if (S_ISBLK(m))
		m &= ~S_IFCHR;
	else
		return -EINVAL;
	if (m)
		return -EINVAL;

	/*
	 * Initialize attributes.
	 */
	(void )memset(&stat, 0, sizeof(stat));
	stat.st_dev = pno->p_parent->p_base->pb_ino->i_fs->fs_dev;
	inum = incore_inum_alloc(FS2ICFS(pno->p_parent->p_base->pb_ino->i_fs));
#ifdef HAVE__ST_INO
	stat.__st_ino = inum;
#endif
	stat.st_mode = mode;
	stat.st_nlink = 1;
	stat.st_uid = getuid();
	stat.st_gid = getgid();
	stat.st_rdev = dev;
	stat.st_size = 0;
	stat.st_blksize = 4096;
	stat.st_blocks = 0;
	stat.st_ctime = stat.st_mtime = stat.st_atime = 0;
	stat.st_ino = inum;

	return incore_create(pno, &stat);
}

#ifdef _HAVE_STATVFS
static int
_sysio_incore_inop_statvfs(struct pnode *pno,
			   struct intnl_statvfs *buf)
{
	struct filesys *fs;

	assert(pno);

	fs = pno->p_base->pb_ino->i_fs;

	(void )memset(buf, 0, sizeof(struct intnl_statvfs));

	/*
	 * Mostly, we lie.
	 */
	buf->f_bsize = getpagesize();
	buf->f_frsize = buf->f_bsize;
	buf->f_blocks = sizeof(char *) * ULONG_MAX;
	buf->f_blocks /= buf->f_bsize;
	buf->f_bfree = buf->f_blocks - 1;
	buf->f_bavail = buf->f_bfree;
	buf->f_files = buf->f_blocks;
	buf->f_ffree = buf->f_files - 1;
	buf->f_favail = buf->f_ffree;
	buf->f_fsid = fs->fs_id;
	buf->f_flag = pno->p_mount->mnt_flags;
	buf->f_namemax = 255;

	return 0;
}
#endif

void
_sysio_incore_inop_gone(struct inode *ino)
{
	struct incore_inode *icino = I2IC(ino);

	incore_i_destroy(icino);
}
