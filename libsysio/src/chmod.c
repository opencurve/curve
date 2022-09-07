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

static int
do_chmod(struct pnode *pno, mode_t mode)
{
	int	err;
	struct intnl_stat stbuf;
	unsigned mask;

	err = _sysio_permitted(pno, W_OK);
	if (err)
		return err;

	(void )memset(&stbuf, 0, sizeof(struct intnl_stat));
	stbuf.st_mode = mode & 07777;
	mask = SETATTR_MODE;
	err = _sysio_p_setattr(pno, mask, &stbuf);
	return err;
}

int
SYSIO_INTERFACE_NAME(chmod)(const char *path, mode_t mode)
{
	int	err;
	struct pnode *pno;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(chmod, "%s%mZ", path, mode);
	err = _sysio_namei(_sysio_cwd, path, 0, NULL, &pno);
	if (err)
		goto out;
	err = do_chmod(pno, mode);
	P_PUT(pno);
out:
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, chmod, "%d", 0);
}

#ifdef REDSTORM
#undef __chmod
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(chmod), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(chmod)))
#endif

int
SYSIO_INTERFACE_NAME(fchmod)(int fd, mode_t mode)
{
	int	err;
	struct file *fil;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fchmod, "%d%mZ", fd, mode);
	err = 0;
	fil = _sysio_fd_find(fd);
	if (!fil) {
		err = -EBADF;
		goto out;
	}

	err = do_chmod(fil->f_pno, mode);
	FIL_PUT(fil);
out:
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, fchmod, "%d", 0);
}

#ifdef REDSTORM
#undef __fchmod
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(fchmod),
		     PREPEND(__, SYSIO_INTERFACE_NAME(fchmod)))
#endif
