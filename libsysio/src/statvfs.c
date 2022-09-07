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

#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"

#include "sysio-symbols.h"

#ifdef _HAVE_STATVFS
static int
_sysio_statvfs(const char *path, struct intnl_statvfs *buf)
{
	int	err;
	struct pnode *pno;

	err = _sysio_namei(_sysio_cwd, path, 0, NULL, &pno);
	if (err)
		return err;

	err = PNOP_STATVFS(pno, buf);
	P_PUT(pno);
	return err;
}

static int
_sysio_fstatvfs(int fd, struct intnl_statvfs *buf)
{
	int	err;
	struct file *filp;

	err = 0;
	filp = _sysio_fd_find(fd);
	if (!filp)
		return -EBADF;

	err = PNOP_STATVFS(filp->f_pno, buf);
	FIL_PUT(filp);
	return err;
}

#if defined(_LARGEFILE64_SOURCE)
int
SYSIO_INTERFACE_NAME(statvfs64)(const char *path, struct statvfs64 *buf)
{
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(statvfs64, "%s", path);
	err = _sysio_statvfs(path, buf);
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, statvfs64, "%d%lvY", buf);
}

#ifdef REDSTORM
#undef __statvfs64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(statvfs64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(statvfs64)))
#endif

int
SYSIO_INTERFACE_NAME(fstatvfs64)(int fd, struct statvfs64 *buf)
{
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fstatvfs64, "%d", fd);
	err = _sysio_fstatvfs(fd, buf);
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, fstatvfs64, "%d%lvY", buf);
}

#ifdef REDSTORM
#undef __fstatvfs64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(fstatvfs64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(fstatvfs64)))
#endif
#endif /* defined(_LARGEFILE64_SOURCE) */

#undef statvfs
#undef fstatvfs

#if defined(_LARGEFILE64_SOURCE)
static void
convstatvfs(struct statvfs *stvfsbuf, struct intnl_statvfs *istvfsbuf)
{
	stvfsbuf->f_bsize = istvfsbuf->f_bsize;
	stvfsbuf->f_frsize = istvfsbuf->f_frsize;
	stvfsbuf->f_blocks = (unsigned long )istvfsbuf->f_blocks;
	stvfsbuf->f_bfree = (unsigned long )istvfsbuf->f_bfree;
	stvfsbuf->f_bavail = (unsigned long )istvfsbuf->f_bavail;
	stvfsbuf->f_files = (unsigned long )istvfsbuf->f_files;
	stvfsbuf->f_ffree = (unsigned long )istvfsbuf->f_ffree;
	stvfsbuf->f_favail = (unsigned long )istvfsbuf->f_favail;
	stvfsbuf->f_fsid = istvfsbuf->f_fsid;
	stvfsbuf->f_flag = istvfsbuf->f_flag;
	stvfsbuf->f_namemax = istvfsbuf->f_namemax;
}
#endif /* defined(_LARGEFILE64_SOURCE) */

int
SYSIO_INTERFACE_NAME(statvfs)(const char *path, struct statvfs *buf)
{
	int	err;
#if defined(_LARGEFILE64_SOURCE)
	struct intnl_statvfs _call_buffer;
	struct intnl_statvfs *_call_buf = &_call_buffer;
#else
#define _call_buf buf
#endif /* defined(_LARGEFILE64_SOURCE) */
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(statvfs, "%s", path);
	err = _sysio_statvfs(path, _call_buf);
#if defined(_LARGEFILE64_SOURCE)
	if (!err)
		convstatvfs(buf, _call_buf);
#undef _call_buf
#endif /* defined(_LARGEFILE64_SOURCE) */
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, statvfs, "%d%vY", buf);
}

#ifdef REDSTORM
#undef __statvfs
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(statvfs),
		     PREPEND(__, SYSIO_INTERFACE_NAME(statvfs)))
#endif

int
SYSIO_INTERFACE_NAME(fstatvfs)(int fd, struct statvfs *buf)
{
	int	err;
#if defined(_LARGEFILE64_SOURCE)
	struct intnl_statvfs _call_buffer;
	struct intnl_statvfs *_call_buf = &_call_buffer;
#else
#define _call_buf buf
#endif /* defined(_LARGEFILE64_SOURCE) */
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fstatvfs, "%d", fd);
	err = _sysio_fstatvfs(fd, _call_buf);
#ifndef INTNL_STATVFS_IS_NATURAL
	if (!err)
		convstatvfs(buf, _call_buf);
#undef _call_buf
#endif
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, fstatvfs, "%d%vY", buf);
}

#ifdef REDSTORM
#undef __fstatvfs
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(fstatvfs),
		     PREPEND(__, SYSIO_INTERFACE_NAME(fstatvfs)))
#endif
#endif
