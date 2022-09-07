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

#include <stdlib.h>
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

/*
 * Perform rename operation on some pnode.
 */
int
_sysio_p_rename(struct pnode *old, struct pnode *new)
{
	struct pnode_base *nxtpb, *pb;
	int	err;

	/*
	 * Check for rename to self.
	 */
	if (old == new)
		return 0;

	/*
	 * No xdev renames please.
	 */
	if (old->p_mount->mnt_fs != new->p_mount->mnt_fs)
		return -EXDEV;

	/*
	 * Must not be a read-only mount.
	 *
	 * NB: Invariant old->p_mount->mnt_fs == new->p_mount->mnt_fs.
	 */
	if (IS_RDONLY(new))
		return -EROFS;

	/*
	 * Don't allow mount points to move.
	 */
	if (old->p_mount->mnt_root == old ||
	    old->p_cover ||
	    new->p_mount->mnt_root == new)
		return -EBUSY;

	/*
	 * Make sure the old pnode can't be found in the ancestor chain
	 * for the new. If it can, they are trying to move into a subdirectory
	 * of the old.
	 */
	nxtpb = new->p_base;
	do {
		pb = nxtpb;
		nxtpb = pb->pb_key.pbk_parent;
		if (pb == old->p_base)
			return -EINVAL;
	} while (nxtpb);

	if (new->p_base->pb_ino) {
		/*
		 * Existing entry. We're replacing the new. Make sure that's
		 * ok.
		 */
		if (S_ISDIR(new->p_base->pb_ino->i_stbuf.st_mode)) {
			if (!S_ISDIR(old->p_base->pb_ino->i_stbuf.st_mode))
				return -EISDIR;
		} else if (S_ISDIR(old->p_base->pb_ino->i_stbuf.st_mode))
			return -ENOTDIR;
	}

	/*
	 * Give the op a try.
	 */
	err = PNOP_RENAME(old, new);
	if (err)
		return err;
	/*
	 * Disconnect the old.
	 */
	_sysio_pb_disconnect(old->p_base);
	/*
	 * Disconnect the new if positive. We want new lookups
	 * to find the just renamed entity.
	 */
	if (new->p_base->pb_ino)
		_sysio_pb_disconnect(new->p_base);
	return 0;
}

int
SYSIO_INTERFACE_NAME(rename)(const char *oldpath, const char *newpath)
{
	struct intent intent;
	int	err;
	struct pnode *old, *new;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(rename, "%s%s", oldpath, newpath);

	/*
	 * Neither old nor new may be the empty string.
	 */
	if (*oldpath == '\0' || *newpath == '\0')
		SYSIO_INTERFACE_RETURN(-1, -ENOENT, rename, "%d", 0);

	old = new = NULL;
	do {
		/*
		 * Resolve oldpath to a path node.
		 */
		INTENT_INIT(&intent, INT_UPDPARENT, NULL, NULL);
		err =
		    _sysio_namei(_sysio_cwd,
				 oldpath,
				 ND_NOFOLLOW|ND_WANTPARENT,
				 &intent,
				 &old);
		if (err) {
			old = NULL;
			break;
		}

		/*
		 * Resolve newpath to a path node.
		 */
		INTENT_INIT(&intent, INT_UPDPARENT, NULL, NULL);
		err =
		    _sysio_namei(_sysio_cwd,
				 newpath,
				 ND_NOFOLLOW|ND_NEGOK|ND_WANTPARENT,
				 &intent,
				 &new);
		if (err) {
			new = NULL;
			break;
		}

		err = _sysio_p_rename(old, new);
	} while (0);

	if (new) {
		P_PUT(new->p_parent);
		P_PUT(new);
	}
	if (old) {
		P_PUT(old->p_parent);
		P_PUT(old);
	}

	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, rename, "%d", 0);
}
