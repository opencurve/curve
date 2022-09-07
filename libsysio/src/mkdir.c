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

#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "fs.h"
#include "mount.h"
#include "sysio-symbols.h"

int
_sysio_mkdir(struct pnode *pno, mode_t mode)
{
	int	err;

	if (pno->p_base->pb_ino) 
		return -EEXIST;

	err = _sysio_permitted(pno->p_parent, W_OK);
	if (err)
		return err;

	mode |= S_IFDIR;
	return PNOP_MKDIR(pno, mode);
}

int
SYSIO_INTERFACE_NAME(mkdir)(const char *path, mode_t mode)
{
	int	err;
	struct intent intent;
	struct pnode *pno;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(mkdir, "%s%mZ", path, mode);
	INTENT_INIT(&intent, INT_CREAT, &mode, NULL);
	err =
	    _sysio_namei(_sysio_cwd,
			 path,
			 ND_NEGOK|ND_WANTPARENT,
			 &intent,
			 &pno);
	if (err)
		goto out;

	mode &= ~(_sysio_umask & 0777);			/* apply umask */
	err = _sysio_mkdir(pno, mode);
	P_PUT(pno->p_parent);
	P_PUT(pno);
out:
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, mkdir, "%d", 0);
}

#ifdef REDSTORM
#undef __mkdir
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(mkdir),
		     PREPEND(__, SYSIO_INTERFACE_NAME(mkdir)))
#endif
