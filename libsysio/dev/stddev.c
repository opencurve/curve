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
 *    Cplant(TM) Copyright 1998-2008 Sandia Corporation. 
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

#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"
#include "inode.h"
#include "dev.h"

#include "stddev.h"

/*
 * Standard devices; null, zero, and full.
 */

static int stddev_open(struct pnode *pno, int flags, mode_t mode);
static int stddev_close(struct pnode *pno);
static int stddev_read(struct ioctx *ioctx);
static int stddev_write(struct ioctx *ioctx);
static int stddev_iodone(struct ioctx *ioctx);
static int stddev_datasync(struct pnode *pno);
static int stddev_ioctl(struct pnode *pno,
			unsigned long int request,
			va_list ap);

int
_sysio_stddev_init()
{
	struct inode_ops stddev_operations;
	int	mjr;

	stddev_operations = _sysio_nodev_ops;
	stddev_operations.inop_open = stddev_open;
	stddev_operations.inop_close = stddev_close;
	stddev_operations.inop_read = stddev_read;
	stddev_operations.inop_write = stddev_write;
	stddev_operations.inop_iodone = stddev_iodone;
	stddev_operations.inop_old_datasync = stddev_datasync;
	stddev_operations.inop_ioctl = stddev_ioctl;

	mjr =
	    _sysio_char_dev_register(SYSIO_C_STDDEV_MAJOR,
				     "stddev",
				     &stddev_operations);
	if (mjr >= 0)
		mjr = 0;
	return mjr;
}

static int
stddev_open(struct pnode *pno,
	    int flags __IS_UNUSED,
	    mode_t mode __IS_UNUSED)
{
	int	err;

	err = 0;
	switch (SYSIO_MINOR_DEV(pno->p_base->pb_ino->i_stbuf.st_rdev)) {
	case SYSIO_STDDEV_NULL_MINOR:
		/*
		 * Fall into...
		 */
	case SYSIO_STDDEV_ZERO_MINOR:
		/*
		 * Fall into...
		 */
	case SYSIO_STDDEV_FULL_MINOR:
		break;
	default:
		err = -ENODEV;
		break;
	}
	return err;
}

static int
stddev_close(struct pnode *pno __IS_UNUSED)
{

	return 0;
}

static ssize_t
doread(void *cp, size_t n, _SYSIO_OFF_T off __IS_UNUSED, struct ioctx *ioctx)
{
	struct inode *ino;;
	ssize_t	cc;

	ino = ioctx->ioctx_pno->p_base->pb_ino;
	assert(ino);
	switch (SYSIO_MINOR_DEV(ino->i_stbuf.st_rdev)) {
	case SYSIO_STDDEV_NULL_MINOR:
		cc = 0;
		break;
	case SYSIO_STDDEV_ZERO_MINOR:
		/*
		 * Fall into...
		 */
	case SYSIO_STDDEV_FULL_MINOR:
		(void )memset((char *)cp, '\0', n);
		cc = (ssize_t )n;
		break;
	default:
		cc = -ENODEV;
		break;
	}
	return cc;
}


static ssize_t
dowrite(void *cp __IS_UNUSED,
	size_t n,
	_SYSIO_OFF_T off __IS_UNUSED,
	struct ioctx *ioctx)
{
	struct inode *ino;
	ssize_t	cc;

	ino = ioctx->ioctx_pno->p_base->pb_ino;
	assert(ino);
	switch (SYSIO_MINOR_DEV(ino->i_stbuf.st_rdev)) {
	case SYSIO_STDDEV_NULL_MINOR:
		/*
		 * Fall into...
		 */
	case SYSIO_STDDEV_ZERO_MINOR:
		cc = (ssize_t )n;
		break;
	case SYSIO_STDDEV_FULL_MINOR:
		cc = -ENOSPC;
		break;
	default:
		cc = -ENODEV;
		break;
	}
	return cc;
}

static ssize_t
doio(struct ioctx *ioctx, 
     ssize_t (*f)(void *, size_t, _SYSIO_OFF_T, struct ioctx *))
{
	int	cc;

	cc =
	    _sysio_doio(ioctx->ioctx_xtv, ioctx->ioctx_xtvlen,
			ioctx->ioctx_iov, ioctx->ioctx_iovlen,
			(ssize_t (*)(void *,
				     size_t,
				     _SYSIO_OFF_T,
				     void *))f,
			ioctx);
	ioctx->ioctx_cc = cc;
	if (ioctx->ioctx_cc < 0) {
		ioctx->ioctx_errno = -ioctx->ioctx_cc;
		ioctx->ioctx_cc = -1;
	}
	return 0;
}

static int
stddev_read(struct ioctx *ioctx)
{

	return doio(ioctx, doread);
}

static int
stddev_write(struct ioctx *ioctx)
{

	return doio(ioctx, dowrite);
}

static int
stddev_iodone(struct ioctx *ioctx __IS_UNUSED)
{

	/*
	 * It's always done in this driver. It completed when posted.
	 */
	return 1;
}

static int
stddev_datasync(struct pnode *pno __IS_UNUSED)
{

	/*
	 * We don't buffer, so nothing to do.
	 */
	return 0;
}

static int
stddev_ioctl(struct pnode *pno __IS_UNUSED,
	    unsigned long int request __IS_UNUSED,
	    va_list ap __IS_UNUSED)
{

	return -ENOTTY;
}
