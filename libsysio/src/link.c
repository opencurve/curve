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

#include <string.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/queue.h>

#include "sysio.h"
#include "mount.h"
#include "inode.h"
#include "sysio-symbols.h"

/*
 * Perform link operation; old to new.
 */
int
_sysio_p_link(struct pnode *old, struct pnode *new)
{
	int	err;

	if (S_ISDIR(old->p_base->pb_ino->i_stbuf.st_mode))
		return -EPERM;
	if (new->p_base->pb_ino)
		return -EEXIST;
	/*
	 * Make sure they aren't trying to link across a mount-point.
	 *
	 * NB: Arguably, should check that the root pnodes are the same.
	 * However, that would allow linking across bound mounts. I'm thinking
	 * we don't want to allow that. Though, I don't really know why not.
	 * I'm a pedant?
	 */
	if (old->p_mount != new->p_mount)
		return -EXDEV;
	if (IS_RDONLY(new->p_parent))
		return -EROFS;
	err = PNOP_LINK(old, new);
	if (err)
		return err;
	PB_SET_ASSOC(new->p_base, old->p_base->pb_ino);
	I_GET(new->p_base->pb_ino);
	return 0;
}

int
SYSIO_INTERFACE_NAME(link)(const char *oldpath, const char *newpath)
{
	struct intent intent;
	int	err;
	struct pnode *old, *new;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(link, "%s%s", oldpath, newpath);
	old = new = NULL;
	err = 0;
	do {
		INTENT_INIT(&intent, 0, NULL, NULL);
		err =
		    _sysio_namei(_sysio_cwd, oldpath, 0, &intent, &old);
		if (err)
			break;
		INTENT_INIT(&intent, INT_UPDPARENT, NULL, NULL);
		err =
		    _sysio_namei(_sysio_cwd,
				 newpath,
				 ND_NEGOK|ND_WANTPARENT,
				 &intent,
				 &new);
		if (err)
			break;
		if (new->p_base->pb_ino) {
			err = -EEXIST;
			break;
		}
		err = _sysio_p_link(old, new);
	} while (0);
	if (new) {
		P_PUT(new->p_parent);
		P_PUT(new);
	}
	if (old)
		P_PUT(old);
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, link, "%d", 0);
}

#ifdef REDSTORM
#undef __link
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(link), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(link)))
#endif
