/*
P_REF(mnt->mnt_covers);
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
 * Albuquerque, NM 87185-1110
 *
 * lee@sandia.gov
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef AUTOMOUNT_FILE_NAME
#include <fcntl.h>
#include <sys/uio.h>
#endif
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"
#include "fs.h"
#include "mount.h"
#include "inode.h"

/*
 * File system and volume mount support.
 */

#ifdef AUTOMOUNT_FILE_NAME
/*
 * Name of autmount specification file in a directory with
 * the sticky-bit set.
 */
struct qstr _sysio_mount_file_name = { "", 0, 0 };
#endif

/*
 * Active mounts.
 */
LIST_HEAD(, mount) mounts;

static int _sysio_sub_fsswop_mount(const char *source,
				   unsigned flags,
				   const void *data,
				   struct pnode *tocover,
				   struct mount **mntp);

static struct fssw_ops _sysio_sub_fssw_ops = {
	_sysio_sub_fsswop_mount
};

/*
 * Initialization. Must be called before any other routine in this module.
 */
int
_sysio_mount_init()
{
	int	err;

	LIST_INIT(&mounts);
#ifdef AUTOMOUNT_FILE_NAME
	_sysio_next_component(AUTOMOUNT_FILE_NAME, &_sysio_mount_file_name);
#endif

	/*
	 * Register the sub-trees "file system" driver.
	 */
	err = _sysio_fssw_register("sub", &_sysio_sub_fssw_ops);
	if (err)
		return err;

	return 0;
}

/*
 * Mount rooted sub-tree somewhere in the existing name space.
 */
int
_sysio_do_mount(struct filesys *fs,
		struct pnode_base *rootpb,
		unsigned flags,
		struct pnode *tocover,
		struct mount **mntp)
{
	struct mount *mnt;
	int	err;

	/*
	 * Directories only, please.
	 */
	if ((tocover &&
	     !(tocover->p_base->pb_ino &&
	       S_ISDIR(tocover->p_base->pb_ino->i_stbuf.st_mode))) ||
	    !(rootpb->pb_ino &&
	      S_ISDIR(rootpb->pb_ino->i_stbuf.st_mode)))
		return -ENOTDIR;

	/*
	 * It's really poor form to allow the new root to be a
	 * descendant of the pnode being covered.
	 */
	if (tocover) {
		struct pnode_base *pb;

		for (pb = rootpb;
		     pb && pb != tocover->p_base;
		     pb = pb->pb_key.pbk_parent)
			;
		if (pb == tocover->p_base)
			return -EBUSY;
	}

	/*
	 * Alloc
	 */
	mnt = malloc(sizeof(struct mount));
	if (!mnt)
		return -ENOMEM;
	err = 0;
	/*
	 * Init enough to make the mount record usable to the path node
	 * generation routines.
	 */
	mnt->mnt_fs = fs;
	if (fs->fs_flags & FS_F_RO) {
		/*
		 * Propagate the read-only flag -- Whether they set it or not.
		 */
		flags |= MOUNT_F_RO;
	}
	mnt->mnt_flags = flags;
	/*
	 * Get alias for the new root.
	 */
	mnt->mnt_root = mnt->mnt_covers = NULL;
	mnt->mnt_root =
	    _sysio_p_new_alias(NULL, rootpb, mnt);
	if (!mnt->mnt_root) {
		err = -ENOMEM;
		goto error;
	}
	/*
	 * Need ref for the mount record but drop the locks now.
	 */
	P_REF(mnt->mnt_root);
	P_PUT(mnt->mnt_root);

	/*
	 * Cover up the mount point.
	 */
	mnt->mnt_covers = tocover;
	if (!mnt->mnt_covers) {
		/*
		 * New graph; It covers itself.
		 */
		mnt->mnt_covers = tocover = mnt->mnt_root;
	}
	P_REF(mnt->mnt_covers);
	assert(!tocover->p_cover);
	tocover->p_cover = mnt->mnt_root;

	LIST_INSERT_HEAD(&mounts, mnt, mnt_link);

#ifdef P_DEBUG
	_sysio_p_show("DO_MOUNT", mnt->mnt_root);
#endif
	*mntp = mnt;
	return 0;

error:
	free(mnt);
	return err;
}

/*
 * Mount unrooted sub-tree somewhere in the existing name space.
 */
int
_sysio_mounti(struct filesys *fs,
	      struct inode *rootino,
	      unsigned flags,
	      struct pnode *tocover,
	      struct mount **mntp)
{
	static struct qstr noname = { NULL, 0, 0 };
	struct pnode_base *rootpb;
	int	err;

	rootpb = _sysio_pb_new(&noname, NULL, rootino);
	if (!rootpb)
		return -ENOMEM;
	err = _sysio_do_mount(fs, rootpb, flags, tocover, mntp);
	if (err)
		_sysio_pb_gone(rootpb);
	else
		PB_PUT(rootpb);
	return err;
}

/*
 * Remove mounted sub-tree from the system.
 */
int
_sysio_do_unmount(struct mount *mnt)
{
	struct pnode *root;
	struct pnode_base *rootpb;
	struct filesys *fs;

	root = mnt->mnt_root;
	P_LOCK(root);
	assert(root->p_ref);
	if (root->p_cover && root->p_cover != root) {
		/*
		 * Active mount.
		 */
		P_UNLOCK(root);
		return -EBUSY;
	}
#ifdef P_DEBUG
	_sysio_p_show("DO_UNMOUNT", mnt->mnt_root);
#endif
	assert(mnt->mnt_covers->p_cover == root);
	if (_sysio_p_prune(root) != 1) {
		/*
		 * Active aliases.
		 */
		P_UNLOCK(root);
		return -EBUSY;
	}
	/*
	 * We're committed.
	 *
	 * Drop ref of covered pnode and break linkage in name space.
	 */
	P_RELE(mnt->mnt_covers);
	mnt->mnt_covers->p_cover = NULL;
	LIST_REMOVE(mnt, mnt_link);
	/*
	 * Kill the root.
	 */
	P_RELE(root);
	root->p_cover = NULL;
	rootpb = root->p_base;
	PB_LOCK(rootpb);
	_sysio_p_gone(root);
	if (!(rootpb->pb_aliases.lh_first || rootpb->pb_children))
		_sysio_pb_gone(rootpb);
	else
		PB_UNLOCK(rootpb);
	/*
	 * Release mount record resource.
	 */
	fs = mnt->mnt_fs;
	free(mnt);
	FS_RELE(fs);

	return 0;
}

/*
 * Helper function to find FS type and call FS-specific mount routine.
 */
static int
_sysio_fs_mount(const char *source,
		const char *fstype,
		unsigned status,
		unsigned flags,
		const void *data,
		struct pnode *tocover,
		struct mount **mntp)
{
	struct fsswent *fssw;
	int	err;

	assert(((status & MOUNT_ST_IFST) == status) &&
	       !(flags & MOUNT_ST_IFST));
	flags |= status & MOUNT_ST_IFST;
	fssw = _sysio_fssw_lookup(fstype);
	if (!fssw)
		return -ENODEV;
	assert(fssw->fssw_ops.fsswop_mount);
	err =
	    (*fssw->fssw_ops.fsswop_mount)(source, flags, data, tocover, mntp);
	return err;
}

/*
 * Establish the system name space.
 */
int
_sysio_mount_root(const char *source,
		  const char *fstype,
		  unsigned flags,
		  const void *data)
{
	int	err;
	struct mount *mnt;

	if (_sysio_root)
		return -EBUSY;

	mnt = NULL;
	err = _sysio_fs_mount(source, fstype, 0, flags, data, NULL, &mnt);
	if (err)
		return err;

	_sysio_root = mnt->mnt_root;
	P_REF(_sysio_root);
#ifndef DEFER_INIT_CWD
	/*
	 * It is very annoying to have to set the current working directory.
	 * So... If it isn't set, make it the root now.
	 */
	if (!_sysio_cwd) {
		_sysio_cwd = _sysio_root;
		P_REF(_sysio_cwd);
	}
#endif

	return 0;
}

int
_sysio_mount(struct pnode *cwd,
	     const char *source,
	     const char *target,
	     const char *filesystemtype,
	     unsigned long mountflags,
	     const void *data)
{
	int	err;
	struct intent intent;
	struct pnode *tocover;
	struct mount *mnt;

	CURVEFS_DPRINTF("into _sysio_mount\n");

	/*
	 * Look up the target path node.
	 */
        INTENT_INIT(&intent, INT_GETATTR, NULL, NULL);
	err = _sysio_namei(cwd, target, 0, &intent, &tocover);

	CURVEFS_DPRINTF("after _sysio_namei\n");
	if (err)
		return err;

	if (tocover == _sysio_root) {
		/*
		 * Attempting to mount over root.
		 */
		err = -EBUSY;
	} else {
		/*
		 * Do the deed.
		 */
		err =
		    _sysio_fs_mount(source,
				    filesystemtype,
				    0,
				    mountflags,
				    data,
				    tocover,
				    &mnt);
	}
	P_PUT(tocover);
	return err;
}

int
SYSIO_INTERFACE_NAME(mount)(const char *source,
			    const char *target,
			    const char *filesystemtype,
			    unsigned long mountflags,
			    const void *data)
{
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(mount,
			      "%s%s%s%lu",
			      source, target,
			      filesystemtype,
			      mountflags);
	err =
	    _sysio_mount(_sysio_cwd,
			 source,
			 target,
			 filesystemtype,
			 mountflags,
			 data);
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, mount, "%d", 0);
}

int
SYSIO_INTERFACE_NAME(umount)(const char *target)
{
	int	err;
	struct pnode *pno;
	struct mount *mnt;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(umount, "%s", target);
	/*
	 * Look up the target path node.
	 */
	err = _sysio_namei(_sysio_cwd, target, 0, NULL, &pno);
	if (err)
		goto out;
	mnt = pno->p_mount;
	if (!err && mnt->mnt_root != pno)
		err = -EINVAL;
	P_PUT(pno);					/* was ref'd */
	if (err)
		goto out;

	/*
	 * Do the deed.
	 */
#if 0	
	if (!pno->p_cover) {
		err = -EINVAL;
		goto error;
	}
#endif
	err = _sysio_do_unmount(mnt);

out:
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, umount, "%d", 0);
}

/*
 * Unmount all file systems -- Usually as part of shutting everything down.
 */
int
_sysio_unmount_all()
{
	int	err;
	struct mount *mnt;
	struct pnode *pno;

	err = 0;
	while ((mnt = mounts.lh_first)) {
		pno = mnt->mnt_root;
		if (pno == _sysio_root) {
			P_RELE(_sysio_root);
			_sysio_root = NULL;
		}
		/*
		 * If this is an automount generated mount, the root
		 * has no reference. We can accomplish the dismount with a
		 * simple prune.
		 */
		if (!_sysio_p_prune(pno))
			continue;
		err = _sysio_do_unmount(mnt);
		if (err)
			break;
	}

	return err;
}

static int
_sysio_sub_fsswop_mount(const char *source,
			unsigned flags,
			const void *data __IS_UNUSED,
			struct pnode *tocover,
			struct mount **mntp)
{
	int	err;
	struct nameidata nameidata;
	struct mount *mnt;

	/*
	 * How can we make a sub-mount from nothing?
	 */
	if (!_sysio_root)
		return -EBUSY;

	/*
	 * Lookup the source.
	 */
	ND_INIT(&nameidata, 0, source, _sysio_root, NULL);
	err = _sysio_path_walk(_sysio_root, &nameidata);
	if (err)
		return err;

	/*
	 * Mount the rooted sub-tree at the given position.
	 */
	err =
	    _sysio_do_mount(nameidata.nd_pno->p_mount->mnt_fs,
			    nameidata.nd_pno->p_base,
			    nameidata.nd_pno->p_mount->mnt_flags & flags,
			    tocover,
			    &mnt);

	/*
	 * Clean up and return.
	 */
	if (!err) {
		FS_REF(nameidata.nd_pno->p_mount->mnt_fs);
		*mntp = mnt;
	}
	P_PUT(nameidata.nd_pno);
	return err;
}

#ifdef AUTOMOUNT_FILE_NAME
/*
 * Parse automount specification formatted as:
 *
 * <fstype>:<source>[[ \t]+<comma-separated-mount-options>]
 *
 * NB:
 * The buffer sent is (almost) always modified.
 */
static int
parse_automount_spec(char *s, char **fstyp, char **srcp, char **optsp)
{
	int	err;
	char	*cp;
	char	*fsty, *src, *opts;

	err = 0;

	/*
	 * Eat leading white.
	 */
	while (*s && *s == ' ' && *s == '\t')
		s++;
	/*
	 * Get fstype.
	 */
	fsty = cp = s;
	while (*cp &&
	       *cp != ':' &&
	       *cp != ' ' &&
	       *cp != '\t' &&
	       *cp != '\r' &&
	       *cp != '\n')
		cp++;
	if (fsty == cp || *cp != ':')
		goto error;
	*cp++ = '\0';

	s = cp;
	/*
	 * Eat leading white.
	 */
	while (*s && *s == ' ' && *s == '\t')
		s++;
	/*
	 * Get source.
	 */
	src = cp = s;
	while (*cp &&
	       *cp != ' ' &&
	       *cp != '\t' &&
	       *cp != '\r' &&
	       *cp != '\n')
		cp++;
	if (src == cp)
		goto error;
	if (*cp)
		*cp++ = '\0';

	s = cp;
	/*
	 * Eat leading white.
	 */
	while (*s && *s == ' ' && *s == '\t')
		s++;
	/*
	 * Get opts.
	 */
	opts = cp = s;
	while (*cp &&
	       *cp != ' ' &&
	       *cp != '\t' &&
	       *cp != '\r' &&
	       *cp != '\n')
		cp++;
	if (opts == cp)
		opts = NULL;
	if (*cp)
		*cp++ = '\0';

	if (*cp)
		goto error;

	*fstyp = fsty;
	*srcp = src;
	*optsp = opts;
	return 0;

error:
	return -EINVAL;
}

/*
 * Parse (and strip) system mount options.
 */
static char *
parse_opts(char *opts, unsigned *flagsp)
{
	unsigned flags;
	char	*src, *dst;
	char	*cp;

	flags = 0;
	src = dst = opts;
	for (;;) {
		cp = src;
		while (*cp && *cp != ',')
			cp++;
		if (src + 2 == cp && strncmp(src, "rw", 2) == 0) {
			/*
			 * Do nothing. This is the default.
			 */
			src += 2;
		} else if (src + 2 == cp && strncmp(src, "ro", 2) == 0) {
			/*
			 * Read-only.
			 */
			flags |= MOUNT_F_RO;
			src += 2;
		}
		else if (src + 4 == cp && strncmp(src, "auto", 4) == 0) {
			/*
			 * Enable automounts.
			 */
			flags |= MOUNT_F_AUTO;
			src += 4;
		}
		if (src < cp) {
			/*
			 * Copy what we didn't consume.
			 */
			if (dst != opts)
				*dst++ = ',';
			do
				*dst++ = *src++;
			while (src != cp);
		}
		if (!*src)
			break;
		*dst = '\0';
		src++;					/* skip comma */
	}
	*dst = '\0';

	*flagsp = flags;
	return opts;
}

/*
 * Attempt automount over the given directory.
 */
int
_sysio_automount(struct pnode *mntpno)
{
	int	err;
	struct inode *ino;
	char	*buf;
	struct ioctx *ioctx;
	ssize_t	cc;
	char	*fstype, *source, *opts;
	unsigned flags;
	struct mount *mnt;

	/*
	 * Revalidate -- Paranoia.
	 */
	err = _sysio_p_validate(mntpno, NULL, NULL);
	if (err)
		return err;

	/*
	 * Read file content.
	 */
	ino = mntpno->p_base->pb_ino;
	if (ino->i_stbuf.st_size > 64 * 1024) {
		/*
		 * Let's be reasonable.
		 */
		return -EINVAL;
	}
	buf = malloc(ino->i_stbuf.st_size + 1);
	if (!buf)
		return -ENOMEM;
	err = _sysio_open(mntpno, O_RDONLY, 0);
	if (err)
		goto out;
	err = _sysio_p_aread(mntpno, 0, buf, ino->i_stbuf.st_size, &ioctx);
	if (!err) {
		cc = _sysio_ioctx_wait(ioctx);
		if (cc < 0)
			err = (int )cc;
	}
	(void )PNOP_CLOSE(mntpno);
	if (err)
		goto out;
	buf[cc] = '\0';					/* NUL terminate */

	/*
	 * Parse.
	 */
	err = parse_automount_spec(buf, &fstype, &source, &opts);
	if (err)
		goto out;
	flags = 0;
	if (opts)
		opts = parse_opts(opts, &flags);

	/*
	 * Do the deed.
	 */
	assert(mntpno->p_parent->p_ref);
	err =
	    _sysio_fs_mount(source,
			    fstype,
			    MOUNT_ST_IFAUTO,
			    flags,
			    opts,
			    mntpno->p_parent,
			    &mnt);

out:
	if (buf)
		free(buf);
	return err;
}
#endif
