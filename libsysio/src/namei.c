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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "mount.h"
#include "inode.h"

/*
 * Parse next component in path.
 */
#ifndef AUTOMOUNT_FILE_NAME
static
#endif
void
_sysio_next_component(const char *path, struct qstr *name)
{
	while (*path == PATH_SEPARATOR)
		path++;
	name->name = path;
	name->len = 0;
	name->hashval = 0;
	while (*path && *path != PATH_SEPARATOR) {
		name->hashval =
		    37 * name->hashval + *path++;
		name->len++;
	}
}

/*
 * Given parent, look up component.
 */
static int
lookup(struct pnode *parent,
       struct qstr *name,
       unsigned flags,
       struct pnode **pnop,
       struct intent *intnt,
       const char *path)
{
	int	err;
	struct pnode *pno;

	CURVEFS_DPRINTF("lookup parent=%p p_parent=%p dir=%d\n", parent, parent->p_parent, S_ISDIR(parent->p_base->pb_ino->i_stbuf.st_mode));

	assert(parent != NULL &&
	       parent->p_parent &&
	       S_ISDIR(parent->p_base->pb_ino->i_stbuf.st_mode));

	/*
	 * Sometimes we don't want to check permissions. At initialization
	 * time, for instance.
	 */
	CURVEFS_DPRINTF("lookup  -------------------------------------- path=%s\n", path);
	if (!(flags & ND_NOPERMCHECK)) {
		err = _sysio_permitted(parent, X_OK);
		if (err)
			return err;
	}
	/*
	 * Short-circuit `.' and `..'; We don't cache those.
	 */
	pno = NULL;
	if (name->len == 1 && name->name[0] == '.') {
		pno = parent;
		P_GET(pno);
	} else if (name->len == 2 &&
		 name->name[0] == '.' &&
		 name->name[1] == '.') {
		int parent_is_locked;
		struct pnode *child;

		parent_is_locked = 1;
		/*
		 * Careful not to enter deadly embrace. Only
		 * one node in any given type locked while we're doing this.
		 */
		pno = parent;
		if (!(flags & ND_NXMNT) &&
		    pno == pno->p_parent &&
		    pno->p_cover != pno) {

			/*
			 * This node loops on itself and is not the root
			 * of some name-space. It's an interior mount-point.
			 */
			P_REF(pno);			/* ref passed parent */
			do {
				struct mount *mnt;

				/*
				 * Only the mount record has the pointer
				 * to the covered path node.
				 */
				mnt = pno->p_mount;
				MNT_GET(mnt);
				assert(pno == mnt->mnt_root &&
				       pno != mnt->mnt_covers);
				P_PUT(pno);
				parent_is_locked = 0;
				pno = mnt->mnt_covers;
				P_GET(pno);
				MNT_PUT(mnt);
			} while (pno == pno->p_parent && pno->p_cover != pno);
		}
		/*
		 * Now, move to parent of this node.
		 */
		child = pno;
		if (parent_is_locked)
			P_REF(child);
		pno = pno->p_parent;
		P_PUT(child);
		P_GET(pno);
		if (!parent_is_locked)
			child = parent;
		P_GET(child);
		P_RELE(child);
	} else {
		/*
		 * Get cache entry then.
		 */
		err = _sysio_p_find_alias(parent, name, &pno);
		if (err)
			return err;
		if (!(flags & ND_NXMNT)) {
			/*
			 * While covered, move to the covering node.
			 */
			while (pno->p_cover && pno->p_cover != pno) {
				struct pnode *cover;
	
				cover = pno->p_cover;
				P_GET(cover);
				P_PUT(pno);
				pno = cover;
			}
		}
	}

	CURVEFS_DPRINTF("lookup come here --------------path=%s\n", path);
	*pnop = pno;

	/*
	 * (Re)validate the pnode.
	 */
	return _sysio_p_validate(pno, intnt, path);
}

/*
 * The meat. Walk an absolute or relative path, looking up each
 * component. Various flags in the nameidata argument govern actions
 * and return values/state. They are:
 *
 * ND_NOFOLLOW		symbolic links are not followed
 * ND_NEGOK		if terminal/leaf does not exist, return
 * 			 path node (alias) anyway.
 * ND_NOPERMCHECK	do not check permissions
 * ND_NXMNT		do not cross mount points
 */
int
_sysio_path_walk(struct pnode *parent, struct nameidata *nd)
{
	int	err;
	const char *path;
	struct qstr this, next;
	struct inode *ino;
	int	done;

	/*
	 * NULL path?
	 */
	if (!nd->nd_path)
		return -EFAULT;

	/*
	 * Empty path?
	 */
	if (!*nd->nd_path)
		return -ENOENT;

	/*
	 * Leading slash?
	 */
	if (*nd->nd_path == PATH_SEPARATOR) {
		/*
		 * Make parent the root of the name space.
		 */
		parent = nd->nd_root;
	}

#ifdef DEFER_INIT_CWD
	if (!parent) {
		const char *icwd;

		if (!_sysio_init_cwd && !nd->nd_root)
			abort();

		/*
		 * Finally have to set the current working directory. We can
		 * not tolerate errors here or else risk leaving the process
		 * in a very unexpected location. We abort then unless all goes
		 * well.
		 */
		icwd = _sysio_init_cwd;
		_sysio_init_cwd = NULL;
		parent = nd->nd_root;
		if (!parent)
			abort();
		(void )_sysio_namei(nd->nd_root,
				    icwd,
				    nd->nd_flags & ND_NXMNT,
				    NULL,
				    &parent);
		if (_sysio_p_chdir(parent) != 0)
			abort();
		P_PUT(parent);
	}
#endif

	/*
	 * (Re)Validate the parent.
	 */
	P_GET(parent);
	err = _sysio_p_validate(parent, NULL, NULL);
	if (err) {
		P_PUT(parent);
		return err;
	}
	CURVEFS_DPRINTF("_sysio_path_walk after _sysio_p_validate\n");
	/*
	 * Prime everything for the loop. Will need another reference to the
	 * initial directory. It'll be dropped later.
	 */
	nd->nd_pno = parent;
	_sysio_next_component(nd->nd_path, &next);
	path = next.name;
	parent = NULL;
	err = 0;
	done = 0;
	CURVEFS_DPRINTF("_sysio_path_walk after _sysio_next_component\n");
	/*
	 * Derecurse the path tree-walk.
	 */
	for (;;) {
		CURVEFS_DPRINTF(" _sysio_path_walk into for pno=%p, ino=%p\n", nd->nd_pno, nd->nd_pno->p_base->pb_ino);
		CURVEFS_DPRINTF(" _sysio_path_walk mode=%d flags=%d len=%ld\n", ino->i_stbuf.st_mode, nd->nd_flags, next.len);
		ino = nd->nd_pno->p_base->pb_ino;
		if (S_ISLNK(ino->i_stbuf.st_mode) &&
		    (next.len || !(nd->nd_flags & ND_NOFOLLOW))) {
			char	*lpath;
			ssize_t	cc;
			struct nameidata nameidata;

			CURVEFS_DPRINTF(" _sysio_path_walk into if\n");
			if (parent) {
				P_PUT(parent);
				parent = NULL;
			}

			if (nd->nd_slicnt >= MAX_SYMLINK) {
				err = -ELOOP;
				break;
			}

			/*
			 * Follow symbolic link.
			 */
			lpath = malloc(MAXPATHLEN + 1);
			if (!lpath) {
				err = -ENOMEM;
				break;
			}
			CURVEFS_DPRINTF("before PNOP_READLINK\n");
			cc = PNOP_READLINK(nd->nd_pno, lpath, MAXPATHLEN);
			if (cc < 0) {
				free(lpath);
				err = (int )cc;
				break;
			}
			lpath[cc] = '\0';			/* NUL term */
			/*
			 * Handle symbolic links with recursion. Yuck!
			 */
			P_REF(nd->nd_pno);
			P_PUT(nd->nd_pno);
			ND_INIT(&nameidata,
				nd->nd_flags,
				lpath,
				nd->nd_root,
				!next.len ? nd->nd_intent : NULL);
			nameidata.nd_slicnt = nd->nd_slicnt + 1;
			err =
			    _sysio_path_walk(nd->nd_pno->p_parent, &nameidata);
			P_GET(nd->nd_pno);
			P_RELE(nd->nd_pno);
			free(lpath);
			if (err)
				break;
			P_PUT(nd->nd_pno);
			nd->nd_pno = nameidata.nd_pno;
			if (nd->nd_flags & ND_WANTPARENT) {
				parent = nd->nd_pno->p_parent;
				assert(P_ISLOCKED(parent) && parent->p_ref);
			}
			ino = nd->nd_pno->p_base->pb_ino;
		}
#ifdef AUTOMOUNT_FILE_NAME
		else if (!(nd->nd_flags & ND_NXMNT) &&
			 ino &&
			 nd->nd_amcnt < MAX_MOUNT_DEPTH &&
			 S_ISDIR(ino->i_stbuf.st_mode) &&
			 ino->i_stbuf.st_mode & S_ISUID &&
			 (nd->nd_pno->p_mount->mnt_flags & MOUNT_F_AUTO)) {
			struct mount *mnt;
			struct pnode *pno;

			CURVEFS_DPRINTF(" _sysio_path_walk into if elseif \n");
			/*
			 * Handle directories that hint they might
			 * be automount-points.
			 */
			mnt = nd->nd_pno->p_mount;
			MNT_GET(mnt);
			err =
			    nd->nd_pno->p_mount->mnt_flags & MOUNT_F_AUTO
			      ? 0
			      : -EACCES;
			pno = NULL;
			if (!err) {
				err =
				    lookup(nd->nd_pno,
					   &_sysio_mount_file_name,
					   0,
					   &pno,
					   NULL,
					   NULL);
			}
			if (!err && (err = _sysio_automount(pno)) == 0) {
				struct pnode *covered;
				/*
				 * All went well. Need to switch
				 * parent pno and ino to the
				 * root of the newly mounted sub-tree.
				 *
				 * NB:
				 * We don't recursively retry these
				 * things. It's OK to have the new root
				 * be an automount-point but it's going
				 * to take another lookup to accomplish it.
				 * The alternative could get us into an
				 * infinite loop.
				 */
				covered = nd->nd_pno;
				nd->nd_pno = covered->p_cover;
				P_GET(nd->nd_pno);
				P_PUT(covered);
				ino = nd->nd_pno->p_base->pb_ino;
				assert(ino);

				/*
				 * Must send the intent-path again.
				 */
				path = nd->nd_path;
				nd->nd_amcnt++;

			}
			if (pno)
				P_PUT(pno);
			MNT_PUT(mnt);
			if (!err) {
				/*
				 * Must go back top and retry with this
				 * new pnode as parent.
				 */
				if (parent) {
					P_PUT(parent);
					parent = NULL;
				}
				continue;
			}
			err = 0;			/* it never happened */
		}
#endif

		CURVEFS_DPRINTF("come here set up for next component path=%s\n", path);
		/*
		 * Set up for next component.
		 */
		this = next;
		if (path)
			path = this.name;
		if (!this.len)
			break;
		if (!ino) {
			/*
			 * Should only be here if final component was
			 * target of a symlink.
			 */
			nd->nd_path = this.name + this.len;
			err = -ENOENT;
			break;
		}
		nd->nd_path = this.name + this.len;
		CURVEFS_DPRINTF("come here set up for next component nd_path=%s\n ", nd->nd_path);
		_sysio_next_component(nd->nd_path, &next);
		if (parent)
			P_PUT(parent);
		parent = nd->nd_pno;
		nd->nd_pno = NULL;
		CURVEFS_DPRINTF("come here after _sysio_next_component\n");
		/*
		 * Parent must be a directory.
		 */
		if (ino && !S_ISDIR(ino->i_stbuf.st_mode)) {
			err = -ENOTDIR;
			break;
		}
		CURVEFS_DPRINTF("Parent must be a directory name=%s\n", this.name);
		/*
		 * The extra path arg is passed only on the first lookup in the
		 * walk as we cross into each file system, anew. The intent is
		 * passed both on the first lookup and when trying to look up
		 * the final component -- Of the original path, not on the
		 * file system.
		 *
		 * Confused? Me too and I came up with this weirdness. It's
		 * hints to the file system drivers. Read on.
		 *
		 * The first lookup will give everything one needs to ready
		 * everything for the entire operation before the path is
		 * walked. The file system driver knows it's the first lookup
		 * in the walk because it has both the path and the intent.
		 *
		 * Alternatively, one could split the duties; The first lookup
		 * can be used to prime the file system inode cache with the
		 * interior nodes we'll want in the path-walk. Then, when
		 * looking up the last component, ready everything for the
		 * operations(s) to come. The file system driver knows it's
		 * the last lookup in the walk because it has the intent,
		 * again, but without the path.
		 *
		 * One special case; If we were asked to look up a single
		 * component, we treat it as the last component. The file
		 * system driver never sees the extra path argument. It should
		 * be noted that the driver always has the fully qualified
		 * path, on the target file system, available to it for any
		 * node it is looking up, including the last, via the base
		 * path node and it's ancestor chain.
		 */
		err =
		    lookup(parent,
			   &this,
			   (nd->nd_flags & (ND_NOPERMCHECK|ND_NXMNT)),
			   &nd->nd_pno,
			   (path || !next.len)
			     ? nd->nd_intent
			     : NULL,
			   (path && next.len) ? path : NULL);
		if (err) {
			done = 1;
			if (err == -ENOENT &&
			    !next.len &&
			    (nd->nd_flags & ND_NEGOK))
				err = 0;
			else
				break;
		}
		CURVEFS_DPRINTF("after Parent must be a directory\n");
		path = NULL;				/* Stop that! */
		/*
		 * Next check requires no further locks. We are preventing
		 * the destruction of the mount records by holding locks
		 * on the two path nodes and the FS field is immutable.
		 */
		if (!(parent->p_mount == nd->nd_pno->p_mount ||
		      parent->p_mount->mnt_fs == nd->nd_pno->p_mount->mnt_fs)) {
			/*
			 * Crossed into a new fs. We'll want the next lookup
			 * to include the path again.
			 */
			path = nd->nd_path;
			/*
			 * Also, the parent isn't really the parent.
			 */
			P_PUT(parent);
			parent = NULL;
		}
		if (done)
			break;
	}

	/*
	 * Trailing separators cause us to break from the loop with
	 * a parent set but no pnode. Check for that.
	 */
	if (!nd->nd_pno) {
		nd->nd_pno = parent;
		parent = NULL;
		/*
		 * Make sure the last processed component was a directory. The
		 * trailing slashes are illegal behind anything else.
		 */
		if (!(err ||
		      S_ISDIR(nd->nd_pno->p_base->pb_ino->i_stbuf.st_mode)))
			err = -ENOTDIR;
	}

	if (parent) {
		if (err || !(nd->nd_flags & ND_WANTPARENT)) {
			/*
			 * Put the parent if present. Either we have a dup of
			 * the original parent or an intermediate reference.
			 */
			P_PUT(parent);
			parent = NULL;
		}
	} else if (!err && (nd->nd_flags & ND_WANTPARENT)) {
		parent = nd->nd_pno->p_parent;
		P_GET(parent);
	}

	/*
	 * On error, we will want to drop the current
	 * path node if at end.
	 */
	if (err && nd->nd_pno) {
		P_PUT(nd->nd_pno);
		nd->nd_pno = NULL;
	}

	return err;
}

/*
 * Expanded form of the path-walk routine, with the common arguments, builds
 * the nameidata bundle and calls path-walk.
 */
int
_sysio_namei(struct pnode *parent,
	     const char *path,
	     unsigned flags,
	     struct intent *intnt,
	     struct pnode **pnop)
{
	struct nameidata nameidata;
	int	err;
	CURVEFS_DPRINTF ("_sysio_namei _sysio_root=%p path=%s nd_pno=%p\n", _sysio_root, path, nameidata.nd_pno);
	ND_INIT(&nameidata, flags, path, _sysio_root, intnt);
	err = _sysio_path_walk(parent, &nameidata);
#if 0
	CURVEFS_DPRINTF ("_sysio_namei nd_pno=%p pb=%p inode=%p\n", nameidata.nd_pno, nameidata.nd_pno->p_base, nameidata.nd_pno->p_base->pb_ino);
#endif
	if (!err)
		*pnop = nameidata.nd_pno;
	return err;
}
