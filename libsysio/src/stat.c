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
 *    Cplant(TM) Copyright 1998-2007 Sandia Corporation. 
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

#include <string.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"

#include "sysio-symbols.h"

#if !defined(_STAT_VER)
#define _STAT_VER		0
#endif

static int
_sysio_fxstat(int ver, int fildes, struct intnl_stat *stat_buf)
{
	struct file *fil;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fxstat, "%d%d", ver, fildes);
	if (ver != _STAT_VER) {
		err = -ENOSYS;
		goto out;
	}

	err = 0;
	fil = _sysio_fd_find(fildes);
	if (!fil) {
		err = -EBADF;
		goto out;
	}
	/*
	 * Never use the attributes cached in the inode record. We
	 * want fresh ones.
	 */
	err = PNOP_GETATTR(fil->f_pno, stat_buf);
	FIL_PUT(fil);
out:
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, fxstat, "%d%sY", stat_buf);
}

static int
_sysio_xstatnd(int ver,
	       const char *filename,
	       struct intnl_stat *stat_buf,
	       unsigned ndflags)
{
	struct intent intent;
	int     err;
	struct pnode *pno;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(xstatnd, "%d%s%u", ver, filename, ndflags);
	if (ver != _STAT_VER) {
		err = -ENOSYS;
		goto out;
	}

	INTENT_INIT(&intent, INT_GETATTR, NULL, NULL);
	err = _sysio_namei(_sysio_cwd, filename, ndflags, &intent, &pno);
	if (err)
		goto out;
	/*
	 * Leverage the INT_GETATTR intent above. We are counting
	 * on the FS driver to either make sure the attributes cached in
	 * the inode are always correct or refresh them in the lookup, above.
	 */
	(void )memcpy(stat_buf,
		      &pno->p_base->pb_ino->i_stbuf,
		      sizeof(struct intnl_stat));
	P_PUT(pno);
out:
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, xstatnd, "%d%sY", stat_buf);
}

#ifdef _LARGEFILE64_SOURCE

#ifndef REDSTORM
#undef fstat64
#undef stat64
#undef lstat64
#endif

#undef __fxstat64
#undef __xstat64
#undef __lxstat64

int
PREPEND(__, SYSIO_INTERFACE_NAME(fxstat64))(int __ver,
					    int __fildes,
					    struct stat64 *__stat_buf)
{

	return _sysio_fxstat(__ver, __fildes, __stat_buf);
}

#ifndef REDSTORM
int
SYSIO_INTERFACE_NAME(fstat64)(int fd, struct stat64 *buf)
{

	return PREPEND(__, SYSIO_INTERFACE_NAME(fxstat64))(_STAT_VER, fd, buf);
}
#endif

int
PREPEND(__, SYSIO_INTERFACE_NAME(xstat64))(int __ver,
					   const char *__filename,
					   struct stat64 *__stat_buf)
{
	return _sysio_xstatnd(__ver, __filename, __stat_buf, 0);
}

#ifndef REDSTORM
int
SYSIO_INTERFACE_NAME(stat64)(const char *filename, struct stat64 *buf)
{

	return PREPEND(__, SYSIO_INTERFACE_NAME(xstat64))(_STAT_VER,
							  filename,
							  buf);
}
#endif

int
PREPEND(__, SYSIO_INTERFACE_NAME(lxstat64))(int __ver,
					    const char *__filename,
					    struct stat64 *__stat_buf)
{
	return _sysio_xstatnd(__ver, __filename, __stat_buf, ND_NOFOLLOW);
}

#ifndef REDSTORM
int
SYSIO_INTERFACE_NAME(lstat64)(const char *filename, struct stat64 *buf)
{

	return PREPEND(__, SYSIO_INTERFACE_NAME(lxstat64))(_STAT_VER,
							   filename,
							   buf);
}
#endif
#endif /* !_LARGEFILE64_SOURCE */

#ifndef REDSTORM
#undef fstat
#undef stat
#undef lstat
#endif

#undef __fxstat
#undef __xstat
#undef __lxstat

#ifdef _LARGEFILE64_SOURCE
static void
convstat(struct stat64 *st64_buf, struct stat *st_buf)
{

	st_buf->st_dev = st64_buf->st_dev;
	st_buf->st_ino = st64_buf->st_ino;
	st_buf->st_mode = st64_buf->st_mode;
	st_buf->st_nlink = st64_buf->st_nlink;
	st_buf->st_uid = st64_buf->st_uid;
	st_buf->st_gid = st64_buf->st_gid;
	st_buf->st_rdev = st64_buf->st_rdev;
	st_buf->st_size = st64_buf->st_size;
	st_buf->st_blksize = st64_buf->st_blksize;
	st_buf->st_blocks = st64_buf->st_blocks;
	st_buf->st_atime = st64_buf->st_atime;
	st_buf->st_mtime = st64_buf->st_mtime;
	st_buf->st_ctime = st64_buf->st_ctime;
}
#endif

int
PREPEND(__, SYSIO_INTERFACE_NAME(fxstat))(int __ver, 
					  int __fildes, 
					  struct stat *__stat_buf)
{
	struct intnl_stat *buf;
	int	err;
#ifdef _LARGEFILE64_SOURCE
	struct stat64 st64;
#endif

#ifdef _LARGEFILE64_SOURCE
	buf = &st64;
#else
	buf = __stat_buf;
#endif
	err = _sysio_fxstat(__ver, __fildes, buf);
#ifdef _LARGEFILE64_SOURCE
	if (!err)
		convstat(buf, __stat_buf);
#endif
	return err;
}

#ifdef REDSTORM
#undef _fxstat
sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(fxstat)), 
		     PREPEND(_, SYSIO_INTERFACE_NAME(fxstat)))
#endif

#ifndef REDSTORM
static int
PREPEND(__, SYSIO_INTERFACE_NAME(fstat))(int fd, struct stat *buf)
{

	return PREPEND(__, SYSIO_INTERFACE_NAME(fxstat))(_STAT_VER, 
							 fd, 
							 buf);
}

sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(fstat)), 
		     SYSIO_INTERFACE_NAME(fstat))

#ifdef BSD
#undef _fstat
sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(fstat)),
		     PREPEND(_, SYSIO_INTERFACE_NAME(fstat)))
#endif
#endif

int
PREPEND(__, SYSIO_INTERFACE_NAME(xstat))(int __ver, 
					 const char *__filename, 
					 struct stat *__stat_buf)
{
	struct intnl_stat *buf;
	int	err;
#ifdef _LARGEFILE64_SOURCE
	struct stat64 st64;
#endif

#ifdef _LARGEFILE64_SOURCE
	buf = &st64;
#else
	buf = __stat_buf;
#endif
	err = _sysio_xstatnd(__ver, __filename, buf, 0);
#ifdef _LARGEFILE64_SOURCE
	if (!err)
		convstat(buf, __stat_buf);
#endif
	return err;
}

#ifdef REDSTORM
#undef _xstat
sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(xstat)),
		     PREPEND(_, SYSIO_INTERFACE_NAME(xstat)))
#endif

#ifndef REDSTORM
static int
PREPEND(__, SYSIO_INTERFACE_NAME(stat))(const char *filename, 
				        struct stat *buf)
{

	return PREPEND(__, SYSIO_INTERFACE_NAME(xstat))(_STAT_VER, 
						        filename,
						        buf);
}

sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(stat)),
		     SYSIO_INTERFACE_NAME(stat))

#ifdef BSD
#undef _stat
sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(stat)),
		     PREPEND(_, SYSIO_INTERFACE_NAME(stat)))
#endif
#endif

int
PREPEND(__, SYSIO_INTERFACE_NAME(lxstat))(int __ver, 
					  const char *__filename, 
					  struct stat *__stat_buf)
{
	struct intnl_stat *buf;
	int	err;
#ifdef _LARGEFILE64_SOURCE
	struct stat64 st64;
#endif

#ifdef _LARGEFILE64_SOURCE
	buf = &st64;
#else
	buf = __stat_buf;
#endif
	err = _sysio_xstatnd(__ver, __filename, buf, ND_NOFOLLOW);
#ifdef _LARGEFILE64_SOURCE
	if (!err)
		convstat(buf, __stat_buf);
#endif
	return err;
}

#ifdef REDSTORM
#undef _lxstat
sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(lxstat)),
		     PREPEND(_, SYSIO_INTERFACE_NAME(lxstat)))
#endif

#ifndef REDSTORM
static int
PREPEND(__, SYSIO_INTERFACE_NAME(lstat))(const char *filename, struct stat *buf)
{
	return PREPEND(__, SYSIO_INTERFACE_NAME(lxstat))(_STAT_VER, 
							 filename,
							 buf);
}

sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(lstat)),
		     SYSIO_INTERFACE_NAME(lstat))

#ifdef BSD
#undef _lstat
sysio_sym_weak_alias(PREPEND(__, SYSIO_INTERFACE_NAME(lstat)),
		     PREPEND(_, SYSIO_INTERFACE_NAME(lstat)))
#endif
#endif
