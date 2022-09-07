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

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "fs.h"
#include "inode.h"

/*
 * File system abstractipon support.
 */

/*
 * The "file system switch".
 */
static LIST_HEAD(, fsswent) fsswitch = { NULL };

/*
 * Lookup named entry in the switch.
 */
struct fsswent *
_sysio_fssw_lookup(const char *name)
{
	struct fsswent *fssw;

	if (!fsswitch.lh_first)
		return NULL;

	fssw = fsswitch.lh_first;
	do {
		if (strcmp(fssw->fssw_name, name) == 0)
			return fssw;
		fssw = fssw->fssw_link.le_next;
	} while (fssw);
	return NULL;
}

/*
 * Register driver.
 */
int
_sysio_fssw_register(const char *name, struct fssw_ops *ops)
{
	struct fsswent *fssw;

	fssw = _sysio_fssw_lookup(name);
	if (fssw)
		return -EEXIST;

	fssw = malloc(sizeof(struct fsswent) + strlen(name) + 1);
	if (!fssw)
		return -ENOMEM;
	fssw->fssw_name = (char *)fssw + sizeof(struct fsswent);
	(void )strcpy((char *)fssw->fssw_name, name);
	fssw->fssw_ops = *ops;

	LIST_INSERT_HEAD(&fsswitch, fssw, fssw_link);

	return 0;
}

#ifdef ZERO_SUM_MEMORY
/*
 * Shutdown
 */
void
_sysio_fssw_shutdown()
{
	struct fsswent *fssw;

	while ((fssw = fsswitch.lh_first)) {
		LIST_REMOVE(fssw, fssw_link);
		free(fssw);
	}
}
#endif

/*
 * Allocate and initialize a new file system record.
 */
struct filesys *
_sysio_fs_new(struct filesys_ops *ops, unsigned flags, void *private)
{
	struct filesys *fs;

	fs = malloc(sizeof(struct filesys));
	if (!fs)
		return NULL;
	FS_INIT(fs, flags, ops, private);
	return fs;
}

/*
 * Dispose of given file system record.
 */
void
_sysio_fs_gone(struct filesys *fs)
{
	struct inode *ino;

	if (fs->fs_ref)
		abort();
	/*
	 * Destroy all remaining i-nodes in the per-FS cache.
	 */
	while (fs->fs_icache) {
		ino = TREE_ENTRY(fs->fs_icache, inode, i_tnode);
		I_LOCK(ino);
		I_GONE(ino);
	}

	(*fs->fs_ops.fsop_gone)(fs);
	free(fs);
}
