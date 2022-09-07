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
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"
#include "inode.h"
#include "file.h"

struct fake_args {
	int	(*f)(struct pnode *);
};

static int
fakeit(struct ioctx *ioctx)
{
	struct fake_args *f;

	f = ioctx->ioctx_args;
	ioctx->ioctx_cc = (*(f->f))(ioctx->ioctx_pno);
	ioctx->ioctx_done = 1;

	return 0;
}

static int
_sysio_p_sync(struct pnode *pno, struct ioctx **ioctxp)
{
	void	*args;
	struct fake_args fake;
	int	(*f)(struct ioctx *);
	int	err;

	args = NULL;
	if (pno->p_base->pb_ino->i_ops.inop_isync)
		f = PNOP_FUNC(pno, isync);
	else {
		f = fakeit;
		fake.f = PNOP_FUNC(pno, old_sync);
		args = &fake;
	}
	err =
	    _sysio_p_iiox(f,
			  pno,
			  -1,
			  NULL, 0, NULL,
			  NULL, 0, NULL,
			  NULL, NULL,
			  args,
			  ioctxp);
	return err;
}

ioid_t
SYSIO_INTERFACE_NAME(ifsync)(int fd)
{
	int	err;
	struct file *fil;
	struct ioctx *ioctx;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(ifsync, "%d", fd);
	err = 0;
	do {
		fil = _sysio_fd_find(fd);
		if (!FIL_FILEOK(fil))
			err = -EBADF;
		if (err)
			break;
		err = _sysio_p_sync(fil->f_pno, &ioctx);
		FIL_PUT(fil);
	} while (0);
	SYSIO_INTERFACE_RETURN(err ? IOID_FAIL : ioctx, err, ifsync, "%p", 0);
}

int
SYSIO_INTERFACE_NAME(fsync)(int fd)
{
	ioid_t	ioid;

	ioid = SYSIO_INTERFACE_NAME(ifsync)(fd);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait(ioid));
}

static int
_sysio_p_datasync(struct pnode *pno, struct ioctx **ioctxp)
{
	void	*args;
	struct fake_args fake;
	int	(*f)(struct ioctx *);
	int	err;

	args = NULL;
	if (pno->p_base->pb_ino->i_ops.inop_idatasync)
		f = PNOP_FUNC(pno, idatasync);
	else {
		f = fakeit;
		fake.f = PNOP_FUNC(pno, old_datasync);
		args = &fake;
	}
	err =
	    _sysio_p_iiox(f,
			  pno,
			  -1,
			  NULL, 0, NULL,
			  NULL, 0, NULL,
			  NULL, NULL,
			  args,
			  ioctxp);
	return err;
}

ioid_t
SYSIO_INTERFACE_NAME(ifdatasync)(int fd)
{
	int	err;
	struct file *fil;
	struct ioctx *ioctx;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(ifdatasync, "%d", fd);
	err = 0;
	do {
		fil = _sysio_fd_find(fd);
		if (!FIL_FILEOK(fil))
			err = -EBADF;
		if (err)
			break;
		err = _sysio_p_datasync(fil->f_pno, &ioctx);
		FIL_PUT(fil);
	} while (0);
	SYSIO_INTERFACE_RETURN(err ? IOID_FAIL : ioctx,
			       err,
			       ifdatasync,
			       "%p", 0);
}

int
SYSIO_INTERFACE_NAME(fdatasync)(int fd)
{
	ioid_t	ioid;

	ioid = SYSIO_INTERFACE_NAME(ifdatasync)(fd);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait(ioid));
}
