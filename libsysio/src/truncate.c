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

#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"
#include "fs.h"
#include "mount.h"

#include "sysio-symbols.h"

/*
 * Truncate file, given path (alias) or index node.
 */
static int
do_truncate(struct pnode *pno, _SYSIO_OFF_T length)
{
	struct intnl_stat stbuf;
	unsigned mask;

	CURVEFS_DPRINTF("do_truncate mode=%d, isdir=%d, isreg=%d\n", pno->p_base->pb_ino->i_stbuf.st_mode, S_ISDIR(pno->p_base->pb_ino->i_stbuf.st_mode), 
		S_ISREG(pno->p_base->pb_ino->i_stbuf.st_mode));
	if (length < 0)
		return -EINVAL;

	if (S_ISDIR(pno->p_base->pb_ino->i_stbuf.st_mode)) {
		/* for others too? */
		return -EISDIR;
	}
	if (!S_ISREG(pno->p_base->pb_ino->i_stbuf.st_mode))
		return -EINVAL;

	(void )memset(&stbuf, 0, sizeof(stbuf));
	stbuf.st_size = length;
	mask = SETATTR_LEN;
	return _sysio_p_setattr(pno, mask, &stbuf);
}

static int
PREPEND(_, SYSIO_INTERFACE_NAME(truncate))(const char *path, 
					   _SYSIO_OFF_T length)
{
	int	err;
	struct pnode *pno;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(truncate, "%s%oZ", path, length);
	err = _sysio_namei(_sysio_cwd, path, 0, NULL, &pno);
	if (err)
		goto out;
	err = do_truncate(pno, length);
	P_PUT(pno);

out:
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, truncate, "%d", 0);
}

#ifdef _LARGEFILE64_SOURCE
#undef truncate64
sysio_sym_weak_alias(PREPEND(_, SYSIO_INTERFACE_NAME(truncate)),
		     SYSIO_INTERFACE_NAME(truncate64))

#undef truncate
int
SYSIO_INTERFACE_NAME(truncate)(const char *path, off_t length)
{

	return PREPEND(_, SYSIO_INTERFACE_NAME(truncate))(path, length);
}
#else
#undef truncate
sysio_sym_weak_alias(PREPEND(_, SYSIO_INTERFACE_NAME(truncate)),
		     SYSIO_INTERFACE_NAME(truncate))
#endif

static int
PREPEND(_, SYSIO_INTERFACE_NAME(ftruncate))(int fd, _SYSIO_OFF_T length)
{
	int	err;
	struct file *fil;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(ftruncate, "%d%oZ", fd, length);

	CURVEFS_DPRINTF("comeing into the ftruncate fd=%d length=%ld\n", fd, length);

	err = 0;
	fil = _sysio_fd_find(fd);
	if (!(fil && FIL_CHKRW(fil, 'w'))) {
		err = -EBADF;
		goto out;
	}
	CURVEFS_DPRINTF("ftruncate the fil=%p\n", fil);
	err = do_truncate(fil->f_pno, length);
out:
	if (fil)
		FIL_PUT(fil);
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, ftruncate, "%d", 0);
}

#ifdef _LARGEFILE64_SOURCE
#undef ftruncate64
sysio_sym_weak_alias(PREPEND(_, SYSIO_INTERFACE_NAME(ftruncate)), 
		     SYSIO_INTERFACE_NAME(ftruncate64))

#undef ftruncate
int
SYSIO_INTERFACE_NAME(ftruncate)(int fd, off_t length)
{

	return PREPEND(_, SYSIO_INTERFACE_NAME(ftruncate))(fd, length);
}
#else
#undef ftruncate
sysio_sym_weak_alias(PREPEND(_, SYSIO_INTERFACE_NAME(ftruncate)), 
		     SYSIO_INTERFACE_NAME(ftruncate))
#endif
