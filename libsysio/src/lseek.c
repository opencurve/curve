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
 * Albuquerque, NM 87185-1319
 *
 * lee@sandia.gov
 */

#define _GNU_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"

#include "sysio-symbols.h"

_SYSIO_OFF_T
_sysio_lseek_prepare(struct file *fil,
		     _SYSIO_OFF_T offset,
		     int whence,
		     _SYSIO_OFF_T max)
{
	_SYSIO_OFF_T off, pos;
	struct intnl_stat stbuf;

	off = -1;
	switch (whence) {
	
	case SEEK_SET:
		off = 0;
		break;
	case SEEK_CUR:
		off = fil->f_pos;
		break;
	case SEEK_END:
		{
			int	err;

			/*
			 * Don't blindly trust the attributes
			 * in the inode record for this. Give the
			 * driver a chance to refresh them.
			 */
			err = PNOP_GETATTR(fil->f_pno, &stbuf);
			if (err)
				return err;
	
		}
		off = stbuf.st_size;
		break;
	default:
		return -EINVAL;
	}
	pos = off + offset;
	if ((offset < 0 && -offset > off) || (offset > 0 && pos <= off))
		return -EINVAL;
	if (pos >= max)
		return -EOVERFLOW;
	return pos;
}

static _SYSIO_OFF_T
_sysio_lseek(struct file *fil,
	     _SYSIO_OFF_T offset,
	     int whence,
	     _SYSIO_OFF_T max)
{
	_SYSIO_OFF_T pos;

	pos = _sysio_lseek_prepare(fil, offset, whence, max);
	if (pos < 0)
		return pos;
	pos = PNOP_POS(fil->f_pno, pos);
	if (pos < 0)
		return pos;
	fil->f_pos = pos;
	return pos;
}

#ifdef _LARGEFILE64_SOURCE
#undef lseek64

extern off64_t
SYSIO_INTERFACE_NAME(lseek64)(int fd, off64_t offset, int whence)
{
	struct file *fil;
	off64_t	off;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(lseek64, "%d%loZ%d", fd, offset, whence);
	fil = _sysio_fd_find(fd);
	if (!fil)
		SYSIO_INTERFACE_RETURN((off64_t )-1,
				       -EBADF,
				       lseek64, "%loZ", 0);
	off = _sysio_lseek(fil, offset, whence, _SEEK_MAX(fil));
	FIL_PUT(fil);
	SYSIO_INTERFACE_RETURN(off < 0 ? (off64_t )-1 : off,
			       off < 0 ? (int )off : 0,
			       lseek64, "%loZ", 0);

}
#ifdef __GLIBC__
#undef __lseek64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(lseek64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(lseek64)))
#endif
#ifdef REDSTORM
#undef __libc_lseek64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(lseek64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_lseek64)))
#endif
#endif

#undef lseek

extern off_t
SYSIO_INTERFACE_NAME(lseek)(int fd, off_t offset, int whence)
{
	struct file *fil;
	_SYSIO_OFF_T off;
	off_t	rtn;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(lseek, "%d%oZ%d", fd, offset, whence);
	fil = _sysio_fd_find(fd);
	if (!fil)
		SYSIO_INTERFACE_RETURN((off_t )-1, -EBADF, lseek, "%oZ", 0);
	off = _sysio_lseek(fil, offset, whence, LONG_MAX);
	FIL_PUT(fil);
	if (off < 0)
		SYSIO_INTERFACE_RETURN((off_t )-1, (int )off, lseek, "%oZ", 0);
	rtn = (off_t )off;
	assert(rtn == off);
	SYSIO_INTERFACE_RETURN(rtn, 0, lseek, "%oZ", 0);
}

#ifdef __GLIBC__
#undef __lseek
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(lseek),
		     PREPEND(__, SYSIO_INTERFACE_NAME(lseek)))
#endif

#ifdef __linux__
#undef llseek
int
SYSIO_INTERFACE_NAME(llseek)(unsigned int fd __IS_UNUSED,
			     unsigned long offset_high __IS_UNUSED,
			     unsigned long offset_low __IS_UNUSED,
			     loff_t *result __IS_UNUSED,
			     unsigned int whence __IS_UNUSED)
{
	struct file *fil;
	loff_t	off;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	/*
	 * This is just plain goofy.
	 */
	SYSIO_INTERFACE_ENTER(llseek,
			      "%d%lu%lu%u",
			      fd,
			      offset_high, offset_low,
			      whence);
	fil = _sysio_fd_find(fd);
	if (!fil)
		SYSIO_INTERFACE_RETURN(-1, -EBADF, llseek, "%d", NULL);
#ifndef _LARGEFILE64_SOURCE
	if (offset_high) {
		/*
		 * We are using 32-bit internals. This just isn't
		 * going to work.
		 */
		SYSIO_INTERFACE_RETURN(-1, -EOVERFLOW, llseek, "%d", NULL);
	}
#else
	off = offset_high;
	off <<= 32;
	off |= offset_low;
#endif
	off = _sysio_lseek(fil, off, whence, _SEEK_MAX(fil));
	FIL_PUT(fil);
	if (off < 0)
		SYSIO_INTERFACE_RETURN(-1, (int )off, llseek, "%d", NULL);
	*result = off;
	/*
	 * Note: Need to add the result to the tracing info some day :(
	 */
	SYSIO_INTERFACE_RETURN(0, 0, llseek, "%d", 0);
}

#undef __llseek
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(llseek), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(llseek)))
#endif
