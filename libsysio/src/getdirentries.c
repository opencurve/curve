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

#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"
#include "sysio-symbols.h"

#ifndef __GNUC__
#define __restrict
#endif
/*
 * Argument to filldir call-back function.
 */
struct filldir_info {
	char	*buf;					/* ptr to buf */
	size_t	remain;					/* remaining in buf */
};

#ifndef _rndup
#define	_rndup(_n, _boundary) \
	((((_n) + (_boundary) - 1 ) / (_boundary)) * (_boundary))
#endif

/*
 * Given directory entry type and length of name, without trailing
 * NUL, return the size of the directory entry.
 */
#define _dreclen(_ty, _namlen) \
        _rndup(OFFSETOF(_ty, d_name) + (_namlen) + 1, sizeof(void *))

/*
 * Fake the old method of filling the directory entries for drivers
 * that have not yet implemented the new.
 */
static int
helper(struct pnode *pno,
       _SYSIO_OFF_T * __restrict basep,
       filldir_t fill,
       void *info)
{
	size_t	nbytes = 16 * 1024;
	char	*buf;
	ssize_t	cc;
	struct dirent64 *d64p;
	int	res;

	/*
	 * Alloc scratch buffer.
	 */
	buf = malloc(nbytes);
	if (!buf)
		return -ENOMEM;

	/*
	 * Fill.
	 */
	cc = PNOP_FILLDIRENTRIES(pno, basep, buf, nbytes);

	/*
	 * Enumerate the entries in the scratch buffer, making the
	 * call-back for each.
	 */
	res = 0;
	d64p = (struct dirent64 *)buf;
	while (cc > 0) {
		res =
		    (*fill)(info,
			    d64p->d_ino,
			    d64p->d_off,
			    d64p->d_name,
			    strlen(d64p->d_name),
			    d64p->d_type);
		if (res != 0)
			break;
		*basep = d64p->d_off;
		if (*basep != d64p->d_off) {
			res = -EOVERFLOW;
			break;
		}
		cc -= d64p->d_reclen;
		d64p = (struct dirent64 *)((char *)d64p + d64p->d_reclen);
	}

	/*
	 * Clean up and return.
	 */
	free(buf);
	return res;
}

static ssize_t
_p_getdirentries(struct pnode *pno,
		 char *buf,
		 size_t nbytes,
		 _SYSIO_OFF_T * __restrict basep,
		 filldir_t fill)
{
	struct inode *ino;
	_SYSIO_OFF_T pos;
	struct filldir_info info;
	int	res;

	ino = pno->p_base->pb_ino;
	if (!ino)
		return -EBADF;
	if (!S_ISDIR(ino->i_stbuf.st_mode))
		return -ENOTDIR;
	pos = *basep;
	info.buf = buf;
	info.remain = nbytes;
	/*
	 * If new filldirentries op is advertised use it. Otherwise,
	 * fall back to the old.
	 */
	res =
	    (ino->i_ops.inop_filldirentries2
	       ? PNOP_FILLDIRENTRIES2(pno, &pos, fill, &info)
	       : helper(pno, &pos, fill, &info));
	if (info.buf == buf)
		return res;
	*basep = pos;
	return info.buf - buf;
}

static ssize_t
getdirentries_common(int fd,
		     char *buf,
		     size_t nbytes,
		     _SYSIO_OFF_T * __restrict basep,
		     filldir_t fill)
{
	struct file *fil;
	ssize_t	cc;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(getdirentries, "%d%zu%d", fd, nbytes, *basep);

	do {
		fil = _sysio_fd_find(fd);
		if (!(fil && FIL_FILEOK(fil))) {
			cc = -EBADF;
			break;
		}

		cc = _p_getdirentries(fil->f_pno, buf, nbytes, basep, fill);
	} while (0);

	FIL_PUT(fil);
	SYSIO_INTERFACE_RETURN(cc < 0 ? -1 : cc,
			       cc,
			       getdirentries,
			       "%zd%oY",
			       *basep);
}

/*
 * Fill in one directory entry.
 */
static int
filldir64(struct filldir_info *info,
	  ino_t inum,
	  _SYSIO_OFF_T off,
	  const char *name,
	  size_t namlen,
	  unsigned char type)
{
	size_t	len;
	struct dirent64 *d64p;
	char	*cp;

	len = _dreclen(dirent64, namlen);
	if (info->remain < len)
		return -EINVAL;
	d64p = (struct dirent64 *)info->buf;
	d64p->d_ino = inum;
	d64p->d_off = off;
	d64p->d_reclen = len;
	d64p->d_type = type;
	(void )memcpy(d64p->d_name, name, namlen);
	cp = d64p->d_name + namlen;

	info->buf += len;
	info->remain -= len;

	/*
	 * NUL pad remainder of d_name.
	 */
	while (cp < info->buf)
		*cp++ = '\0';
	return 0;
}

ssize_t
_sysio_p_filldirentries(struct pnode *pno,
			char *buf,
			size_t nbytes,
			_SYSIO_OFF_T * __restrict basep)
{

	return _p_getdirentries(pno, buf, nbytes, basep, (filldir_t )filldir64);
}

#ifdef _LARGEFILE64_SOURCE
static ssize_t
PREPEND(_, SYSIO_INTERFACE_NAME(getdirentries64))(int fd,
						  char *buf,
						  size_t nbytes,
						  off64_t * __restrict basep)
{
	_SYSIO_OFF_T ibase;
	ssize_t	cc;

	ibase = *basep;
	if (ibase != *basep) {
		errno = EINVAL;
		return -1;
	}

	cc =
	    getdirentries_common(fd,
				 buf,
				 nbytes,
				 &ibase,
				 (filldir_t )filldir64);
	if (cc >= 0)
		*basep = ibase;
	return cc;
}

#undef getdirentries64
sysio_sym_strong_alias(PREPEND(_, SYSIO_INTERFACE_NAME(getdirentries64)),
		       SYSIO_INTERFACE_NAME(getdirentries64))
#endif

#ifndef DIRENT64_IS_NATURAL
/*
 * Fill in one directory entry.
 */
static int
filldir(struct filldir_info *info,
	ino_t inum,
	_SYSIO_OFF_T off,
	const char *name,
	size_t namlen,
	unsigned char type)
{
	size_t	len;
	struct dirent *dp;
	char	*cp;

	len = _dreclen(dirent, namlen);
	if (info->remain < len)
		return -EINVAL;
	dp = (struct dirent *)info->buf;
	dp->d_ino = inum;
	if (dp->d_ino != inum)
		return -EOVERFLOW;
	dp->d_off = off;
	if (dp->d_off != off)
		return -EOVERFLOW;
	dp->d_reclen = len;
	dp->d_type = type;
	(void )memcpy(dp->d_name, name, namlen);
	cp = dp->d_name + namlen;

	info->buf += len;
	info->remain -= len;

	/*
	 * NUL pad remainder of d_name.
	 */
	while (cp < info->buf)
		*cp++ = '\0';
	return 0;
}

#ifndef BSD
ssize_t
SYSIO_INTERFACE_NAME(getdirentries)(int fd,
				    char *buf,
				    size_t nbytes,
				    off_t * __restrict basep)
#else
int
SYSIO_INTERFACE_NAME(getdirentries)(int fd,
				    char *buf,
				    int nbytes,
				    long * __restrict basep)
#endif
{
	_SYSIO_OFF_T pos;
	ssize_t	cc;

#ifndef BSD
#define _cast
#else
#define _cast		(int )
#endif

	pos = *basep;
	cc = getdirentries_common(fd, buf, nbytes, &pos, (filldir_t )filldir);
	if (cc > 0)
		*basep = pos;
	return _cast cc;

#undef _cast
}
#else /* !defined(DIRENT64_IS_NATURAL) */
sysio_sym_strong_alias(PREPEND(_, SYSIO_INTERFACE_NAME(getdirentries64),
		       SYSIO_INTERFACE_NAME(getdirentries)))
#endif

#ifdef REDSTORM
#undef __getdirentries
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(getdirentries),
		     PREPEND(__, SYSIO_INTERFACE_NAME(getdirentries)))
#endif
#if defined(BSD) || defined(REDSTORM)
#undef _getdirentries
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(getdirentries),
		     PREPEND(_, SYSIO_INTERFACE_NAME(getdirentries)))
#endif
