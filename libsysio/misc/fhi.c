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
#include <unistd.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/queue.h>

#include "sysio.h"
#include "fs.h"
#include "mount.h"
#include "inode.h"
#include "xtio.h"
#include "fhi.h"

/*
 * An exported sub-tree.
 *
 * These do not cross mount points within the sysio library.
 */
struct file_handle_info_export {
	struct tree_node fhiexp_tnode;
	struct file_handle_info_export_key {
		const char *fhiexpk_data;
		size_t	fhiexpk_size;
	}  fhiexp_key;
	struct mount *fhiexp_mount;			/* associated mount */
};

/*
 * Exports, ordered by key.
 */
static struct tree_node *exports = NULL;

static int
compar_fhiexpk(const struct file_handle_info_export_key *k1,
	       const struct file_handle_info_export_key *k2)
{

	if (k1->fhiexpk_size < k2->fhiexpk_size)
		return -1;
	if (k1->fhiexpk_size > k2->fhiexpk_size)
		return 1;
	return memcmp(k1->fhiexpk_data, k2->fhiexpk_data, k2->fhiexpk_size);
}

int
SYSIO_INTERFACE_NAME(fhi_export)(const void *key,
				 size_t keylen,
				 const char *source,
				 unsigned flags,
				 struct file_handle_info_export **fhiexpp)
{
	struct file_handle_info_export *fhiexp, fhiexpbuf;
	struct pnode *pno;
	struct intent intent;
	int	err;
	struct tree_node *tn;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_export,
			      "%p%zu%s%u%p",
			      key, keylen,
			      source,
			      flags,
			      *fhiexpp);

	fhiexp = &fhiexpbuf;
	do {
		/*
		 * Key must be unique.
		 */
		fhiexp->fhiexp_key.fhiexpk_data = key;
		fhiexp->fhiexp_key.fhiexpk_size = keylen;
		if (_sysio_tree_find(&fhiexp->fhiexp_key,
				     &exports,
				     (int (*)(const void *,
					      const void *))compar_fhiexpk)) {
			err = -EBUSY;
			break;
		}

		fhiexp = malloc(sizeof(struct file_handle_info_export));
		if (!fhiexp) {
			err = -ENOSPC;
			break;
		}
		fhiexp->fhiexp_key = fhiexpbuf.fhiexp_key;
		fhiexp->fhiexp_tnode.tn_key = &fhiexp->fhiexp_key;

		pno = NULL;
		INTENT_INIT(&intent, 0, NULL, NULL);
		err = _sysio_namei(_sysio_cwd, source, 0, &intent, &pno);
		if (err)
			break;

		/*
		 * Adjust flags.
		 *
		 * Inherit read-only and, since we restrict all pathnames to
		 * reside only in the exported volume, automounts are
		 * useless.
		 */
		flags |=
		    pno->p_mount->mnt_flags & MOUNT_F_RO;
#ifdef MOUNT_F_AUTO
		flags &= ~MOUNT_F_AUTO;
#endif
		FS_REF(pno->p_base->pb_ino->i_fs);
		err =
		    _sysio_do_mount(pno->p_base->pb_ino->i_fs,
				    pno->p_base,
				    flags,
				    NULL,
				    &fhiexp->fhiexp_mount);
		P_PUT(pno);
		if (err) {
			FS_RELE(pno->p_base->pb_ino->i_fs);
			break;
		}

		tn =
		    _sysio_tree_search(&fhiexp->fhiexp_tnode,
				       &exports,
				       (int (*)(const void *, 
						const void *))compar_fhiexpk);
		if (tn != &fhiexp->fhiexp_tnode)
			abort();
		} while (0);
	if (err) {
		if (fhiexp == &fhiexpbuf)
			free(fhiexp);
	} else
		*fhiexpp = fhiexp;
	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_export,
			       "%d%p",
			       *fhiexpp,
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_unexport)(struct file_handle_info_export *fhiexp)
{
	struct tree_node *tn;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_unexport, "%p", fhiexp);

	do {
		tn =
		    _sysio_tree_find(&fhiexp->fhiexp_key,
				     &exports,
				     (int (*)(const void *,
					      const void *))compar_fhiexpk);
		if (tn != &fhiexp->fhiexp_tnode) {
			err = -ENOENT;
			break;
		}

		err = _sysio_do_unmount(fhiexp->fhiexp_mount);
		if (err)
			break;
		fhiexp->fhiexp_mount = NULL;

		tn =
		    _sysio_tree_delete(&fhiexp->fhiexp_key,
				       &exports,
				       (int (*)(const void *,
						const void *))compar_fhiexpk);
		assert(tn == &fhiexp->fhiexp_tnode);
		free(fhiexp);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_unexport,
			       "%d",
			       0);
}

/*
 * Build file handle information in passed buffer given export and path node.
 *
 * Note: The file handle information buffer is value-result. The caller
 * should set the size of the opaque data buffer within and the returned size
 * indicates the actual size. If the actual size is greater than the caller's
 * supplied size, the opaque data buffer will not have been updated.
 */
static size_t
fhi_load(struct file_handle_info_export *fhiexp,
	 struct pnode *pno,
	 struct file_handle_info *fhi)
{
	size_t	len;

	assert(pno->p_base->pb_ino);
	len = pno->p_base->pb_ino->i_fid->fid_len;
	if (fhi->fhi_handle_len >= len) {
		fhi->fhi_handle_len = len;
		(void )memcpy(fhi->fhi_handle,
			      pno->p_base->pb_ino->i_fid->fid_data,
			      len);
		fhi->fhi_export = fhiexp;
	}
	return len;
}

ssize_t
SYSIO_INTERFACE_NAME(fhi_root_of)(struct file_handle_info_export *fhiexp,
				  struct file_handle_info *fhi)
{
	ssize_t	len;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_root_of,
			      "%p%hY",
			      fhiexp,
			      fhi);
	len = fhi_load(fhiexp, fhiexp->fhiexp_mount->mnt_root, fhi);
	SYSIO_INTERFACE_RETURN(len,
			       0,
			       fhi_root_of,
			       "%zd%hY",
			       *fhi,
			       0);
}

/*
 * Get completion of referenced asynchronous IO identifier.
 *
 * NB: Unlike the internal iowait, we will not actually wait. Instead,
 * in such a case, we return -EWOULDBLOCK.
 */
ssize_t
SYSIO_INTERFACE_NAME(fhi_iowait)(ioid_t ioid)
{
	struct ioctx *ioctx;
	struct pnode *pno;
	ssize_t	cc;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_iowait,
			      "%p",
			      ioid);

	do {
		ioctx = _sysio_ioctx_find(ioid);
		if (!ioctx) {
			cc = -EINVAL;
			break;
		}
		if (!_sysio_ioctx_done(ioctx)) {
			cc = -EWOULDBLOCK;
			break;
		}
		pno = ioctx->ioctx_pno;
		P_GET(pno);
		cc = _sysio_ioctx_wait(ioctx);
		err = PNOP_CLOSE(pno);
		if (err && cc >= 0)
			cc = err;
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(cc,
			       (int )(cc <= 0 ? cc : 0),
			       fhi_iowait,
			       "%zd",
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_iodone)(ioid_t ioid)
{

	return SYSIO_INTERFACE_NAME(iodone)(ioid);
}

/*
 * Return a usable path node for the given file identifier within the
 * given mounts directory-tree.
 */
static struct pnode *
_sysio_find_alias_by_fid(struct mount *mnt, struct file_identifier *fid)
{
	struct inode *ino;
	struct intent intent;
	struct pnode_base *nextpb, *pb, *this;
	size_t	len;
	struct pnode *nextpno, *pno;

	/*
	 * Find inode with given identifier.
	 */
	ino = _sysio_i_find(mnt->mnt_fs, fid);
	if (!ino)
		return NULL;
	INTENT_INIT(&intent, 0, NULL, NULL);
	pno = NULL;
	/*
	 * Search list of associated names...
	 */
	nextpb = ino->i_assoc.lh_first;
	while ((pb = nextpb)) {
		nextpb = pb->pb_alinks.le_next;
		this = pb;
		while (this) {
			len = this->pb_key.pbk_name.len;
			if (!len && this->pb_key.pbk_parent)
				break;
			this = this->pb_key.pbk_parent;
		}
		if (this)
			continue;
		/*
		 * Search list of aliases associated with the name to find one
		 * associated with this mount...
		 */
		nextpno = pb->pb_aliases.lh_first;
		while ((pno = nextpno)) {
			nextpno = pno->p_links.le_next;
			if (pno->p_mount == mnt) {
				P_GET(pno);
				if (_sysio_p_validate(pno,
						      &intent,
						      NULL) == 0) {
					break;
				}
				P_PUT(pno);
			}
		}
		if (pno)
			break;
	}
	I_PUT(ino);
	return pno;
}

/*
 * Given pointer to file handle info record, return related, usable, path node.
 */
static int
find_alias(struct file_handle_info *fhi, struct pnode **pnop)
{
	struct pnode *pno;
	struct file_identifier fileid;

	fileid.fid_data = fhi->fhi_handle;
	fileid.fid_len = fhi->fhi_handle_len;
	pno =
	    _sysio_find_alias_by_fid(fhi->fhi_export->fhiexp_mount, &fileid);
	if (!pno)
		return -ESTALE;
	*pnop = pno;
	return 0;
}

int
SYSIO_INTERFACE_NAME(fhi_getattr)(struct file_handle_info *fhi,
				  struct stat64 *buf)
{
	struct pnode *pno;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_getattr,
			      "%hY",
			      fhi);

	err = 0;
	do {
		err = find_alias(fhi, &pno);
		if (err) {
			err = -ESTALE;
			break;
		}

		(void )memset(buf, 0, sizeof(struct stat64));
		buf->st_dev = pno->p_base->pb_ino->i_stbuf.st_dev;
		buf->st_ino = pno->p_base->pb_ino->i_stbuf.st_ino;
		buf->st_mode = pno->p_base->pb_ino->i_stbuf.st_mode;
		buf->st_nlink = pno->p_base->pb_ino->i_stbuf.st_nlink;
		buf->st_uid = pno->p_base->pb_ino->i_stbuf.st_uid;
		buf->st_gid = pno->p_base->pb_ino->i_stbuf.st_gid;
		buf->st_rdev = pno->p_base->pb_ino->i_stbuf.st_rdev;
		buf->st_size = pno->p_base->pb_ino->i_stbuf.st_size;
		buf->st_blksize = pno->p_base->pb_ino->i_stbuf.st_blksize;
		buf->st_blocks = pno->p_base->pb_ino->i_stbuf.st_blocks;
		buf->st_atime = pno->p_base->pb_ino->i_stbuf.st_atime;
		buf->st_mtime = pno->p_base->pb_ino->i_stbuf.st_mtime;
		buf->st_ctime = pno->p_base->pb_ino->i_stbuf.st_ctime;

		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_getattr,
			       "%d%sY",
			       buf,
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_setattr)(struct file_handle_info *fhi,
				  struct file_handle_info_sattr *fhisattr)
{

	struct pnode *pno;
	int	err;
	unsigned mask;
	struct intnl_stat stbuf;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_setattr,
			      "%hY%p",
			      fhi,
			      fhisattr);

	do {
		err = find_alias(fhi, &pno);
		if (err) {
			err = -ESTALE;
			break;
		}

		mask = 0;
		if (fhisattr->fhisattr_mode_set) {
			mask |= SETATTR_MODE;
			stbuf.st_mode = fhisattr->fhisattr_mode;
		}
		if (fhisattr->fhisattr_uid_set) {
			mask |= SETATTR_UID;
			stbuf.st_uid = fhisattr->fhisattr_uid;
		}
		if (fhisattr->fhisattr_gid_set) {
			mask |= SETATTR_GID;
			stbuf.st_gid = fhisattr->fhisattr_gid;
		}
		if (fhisattr->fhisattr_size_set) {
			mask |= SETATTR_LEN;
			stbuf.st_size = fhisattr->fhisattr_size;
		}
		if (fhisattr->fhisattr_atime_set) {
			mask |= SETATTR_ATIME;
			stbuf.st_atime = fhisattr->fhisattr_atime;
		}
		if (fhisattr->fhisattr_mtime_set) {
			mask |= SETATTR_MTIME;
			stbuf.st_mtime = fhisattr->fhisattr_mtime;
		}
		err = _sysio_p_setattr(pno, mask, &stbuf);

		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_setattr,
			       "%d%p",
			       fhisattr,
			       0);
}

/*
 * Lookup path node given parent file handle information and path string.
 */
static int
fhi_namei(struct file_handle_info *parent_fhi,
	  const char *path,
	  unsigned flags,
	  struct intent *intnt,
	  struct pnode **pnop)
{
	struct pnode *parent;
	int	err;
	struct nameidata nameidata;

	err = find_alias(parent_fhi, &parent);
	if (err)
		return err;
	P_REF(parent);
	P_PUT(parent);
	ND_INIT(&nameidata,
		(flags | ND_NXMNT),
		path,
		parent_fhi->fhi_export->fhiexp_mount->mnt_root,
		intnt);
	err = _sysio_path_walk(parent, &nameidata);
	P_RELE(parent);
	if (err)
		return err;
	*pnop = nameidata.nd_pno;
	return 0;
}

ssize_t
SYSIO_INTERFACE_NAME(fhi_lookup)(struct file_handle_info *parent_fhi,
				 const char *path,
				 unsigned iopmask,
				 struct file_handle_info *result)
{
	struct intent intent;
	struct pnode *pno;
	size_t	len;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_lookup,
			      "%hY%s%u%hY",
			      parent_fhi,
			      path,
			      iopmask,
			      result);

	len = 0;
	do {
		INTENT_INIT(&intent, iopmask, NULL, NULL);
		err = fhi_namei(parent_fhi, path, ND_NOFOLLOW, &intent, &pno);
		if (err)
			break;
		len = fhi_load(parent_fhi->fhi_export, pno, result);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err < 0 ? err : (ssize_t )len,
			       err,
			       fhi_lookup,
			       "%d%p",
			       result,
			       0);
}

ssize_t
SYSIO_INTERFACE_NAME(fhi_readlink)(struct file_handle_info *fhi,
				   char *buf,
				   size_t bufsiz)
{
	struct pnode *pno;
	ssize_t	cc;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_readlink,
			      "%hY%p%zu",
			      fhi,
			      buf,
			      bufsiz);

	do {
		if (find_alias(fhi, &pno) != 0) {
			cc = -ESTALE;
			break;
		}
		cc = _sysio_p_readlink(pno, buf, bufsiz);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(cc,
			       (int )(cc <= 0 ? cc : 0),
			       fhi_readlink,
			       "%d%*s",
			       buf,
			       0);
}

/*
 * Post 64-bit asynchronous, xtio read/write.
 */
static int
iio64x(int (*f)(struct ioctx *),
       struct pnode *pno,
       int flags,
       const struct iovec *iov, size_t iov_count,
       const struct intnl_xtvec *xtv, size_t xtv_count,
       struct ioctx **ioctxp)
{
	int	err;

	do {
		err = _sysio_open(pno, flags, 0);
		if (err)
			break;
		err =
		    _sysio_p_iiox(f,
				  pno,
				  _SYSIO_OFF_T_MAX,
				  iov, iov_count, NULL,
				  xtv, xtv_count, NULL,
				  NULL, NULL,
				  NULL,
				  ioctxp);
		if (err) {
			if (err == -EBADF)
				err = -ESTALE;
			if (PNOP_CLOSE(pno) != 0)
				abort();
			break;
		}
		/*
		 * Otherwise, the close is called in the iowait function.
		 */
	} while (0);
	return err;
}

int
SYSIO_INTERFACE_NAME(fhi_iread64x)(struct file_handle_info *fhi,
				   const struct iovec *iov, size_t iov_count,
				   const struct xtvec64 *xtv, size_t xtv_count,
				   ioid_t *ioidp)
{
	struct pnode *pno;
	int	err;
	struct ioctx *ioctx;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_read64x,
			      "%hY%*iZ%*xY",
			      fhi,
			      iov_count, iov,
			      xtv_count, xtv);

	do {
		err = find_alias(fhi, &pno);
		if (err) {
			err = -ESTALE;
			break;
		}
		err =
		    iio64x(PNOP_FUNC(pno, read),
			   pno,
			   O_RDONLY,
			   iov, iov_count,
			   xtv, xtv_count,
			   &ioctx);
		P_PUT(pno);
	} while (0);
	if (!err)
		*ioidp = ioctx;

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_read64x,
			       "%d%p",
			       *ioidp,
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_iwrite64x)(struct file_handle_info *fhi,
				    const struct iovec *iov, size_t iov_count,
				    const struct xtvec64 *xtv, size_t xtv_count,
				    ioid_t *ioidp)
{
	struct pnode *pno;
	int	err;
	struct ioctx *ioctx;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_write64x,
			      "%hY%*iZ%*xY",
			      fhi,
			      iov_count, iov,
			      xtv_count, xtv);

	do {
		err = find_alias(fhi, &pno);
		if (err) {
			err = -ESTALE;
			break;
		}
		err =
		    iio64x(PNOP_FUNC(pno, write),
			   pno,
			   O_WRONLY,
			   iov, iov_count,
			   xtv, xtv_count,
			   &ioctx);
		P_PUT(pno);
	} while (0);
	if (!err)
		*ioidp = ioctx;

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_write64x,
			       "%d%p",
			       *ioidp,
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_create)(struct file_handle_info_dirop_args *where,
				 mode_t mode)
{
	struct intent intent;
	struct pnode *pno;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_create,
			      "%hY%s%mZ",
			      where->fhida_dir, where->fhida_path,
			      mode);

	do {
		INTENT_INIT(&intent, INT_CREAT, NULL, NULL);
		err =
		    fhi_namei(where->fhida_dir,
			      where->fhida_path,
			      ND_NEGOK|ND_WANTPARENT,
			      &intent,
			      &pno);
		if (err)
			break;
		do {
			err = _sysio_open(pno, O_CREAT|O_WRONLY|O_TRUNC, mode);
			if (err)
				break;
			if (PNOP_CLOSE(pno) != 0)
				abort();
		} while (0);
		P_PUT(pno->p_parent);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_create,
			       "%d",
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_unlink)(struct file_handle_info_dirop_args *where)
{
	struct intent intent;
	struct pnode *pno;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_unlink,
			      "%hY%s",
			      where->fhida_dir, where->fhida_path);

	do {
		INTENT_INIT(&intent, INT_UPDPARENT, NULL, NULL);
		err =
		    fhi_namei(where->fhida_dir,
			      where->fhida_path,
			      ND_NOFOLLOW|ND_WANTPARENT,
			      &intent,
			      &pno);
		if (err)
			break;
		err = _sysio_p_unlink(pno);
		P_PUT(pno->p_parent);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_unlink,
			       "%d",
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_rename)(struct file_handle_info_dirop_args *from,
				 struct file_handle_info_dirop_args *to)
{
	struct pnode *old, *new;
	struct intent intent;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_rename,
			      "%hY%s%hY%s",
			      from->fhida_dir, from->fhida_path,
			      to->fhida_dir, to->fhida_path);

	old = new = NULL;
	do {
		INTENT_INIT(&intent, INT_UPDPARENT, NULL, NULL);
		err =
		    fhi_namei(from->fhida_dir,
			      from->fhida_path,
			      ND_WANTPARENT,
			      &intent,
			      &old);
		if (err)
			break;
		INTENT_INIT(&intent, INT_CREAT, NULL, NULL);
		err =
		    fhi_namei(to->fhida_dir,
			      to->fhida_path,
			      ND_NEGOK|ND_WANTPARENT,
			      &intent,
			      &new);
		if (err)
			break;
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

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_rename,
			       "%d",
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_link)(struct file_handle_info_dirop_args *from,
			       struct file_handle_info_dirop_args *to)
{
	struct pnode *old, *new;
	struct intent intent;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_link,
			      "%hY%s%hY%s",
			      from->fhida_dir, from->fhida_path,
			      to->fhida_dir, to->fhida_path);

	old = new = NULL;
	do {
		INTENT_INIT(&intent, 0, NULL, NULL);
		err =
		    fhi_namei(from->fhida_dir,
			      from->fhida_path,
			      0,
			      &intent,
			      &old);
		if (err)
			break;
		INTENT_INIT(&intent, INT_CREAT, NULL, NULL);
		err =
		    fhi_namei(to->fhida_dir,
			      to->fhida_path,
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

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_link,
			       "%d",
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_symlink)(const char *from,
				  struct file_handle_info_dirop_args *to)
{
	struct intent intent;
	struct pnode *pno;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_symlink,
			      "%s%hY%s",
			      from,
			      to->fhida_dir, to->fhida_path);

	do {
		INTENT_INIT(&intent, INT_CREAT, NULL, NULL);
		err =
		    fhi_namei(to->fhida_dir,
			      to->fhida_path,
			      ND_NEGOK|ND_WANTPARENT,
			      &intent,
			      &pno);
		if (err)
			break;
		err = _sysio_p_symlink(from, pno);
		P_PUT(pno->p_parent);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_symlink,
			       "%d",
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_mkdir)(struct file_handle_info_dirop_args *where,
				mode_t mode)
{
	struct intent intent;
	struct pnode *pno;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_mkdir,
			      "%hY%s%mZ",
			      where->fhida_dir, where->fhida_path,
			      mode);

	do {
		INTENT_INIT(&intent, INT_CREAT, NULL, NULL);
		err =
		    fhi_namei(where->fhida_dir,
			      where->fhida_path,
			      ND_NEGOK|ND_WANTPARENT,
			      &intent,
			      &pno);
		if (err)
			break;
		err = _sysio_mkdir(pno, mode);
		P_PUT(pno->p_parent);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_mkdir,
			       "%d",
			       0);
}

int
SYSIO_INTERFACE_NAME(fhi_rmdir)(struct file_handle_info_dirop_args *where)
{
	struct intent intent;
	struct pnode *pno;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_rmdir,
			      "%hY%s%",
			      where->fhida_dir, where->fhida_path);

	do {
		INTENT_INIT(&intent, INT_UPDPARENT, NULL, NULL);
		err =
		    fhi_namei(where->fhida_dir,
			      where->fhida_path,
			      ND_WANTPARENT,
			      &intent,
			      &pno);
		if (err)
			break;
		err = _sysio_p_rmdir(pno);
		P_PUT(pno->p_parent);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_rmdir,
			       "%d",
			       0);
}

ssize_t
SYSIO_INTERFACE_NAME(fhi_getdirentries64)(struct file_handle_info *fhi,
					  char *buf,
					  size_t nbytes,
					  off64_t * __restrict basep)
{
	_SYSIO_OFF_T ibase;
	struct pnode *pno;
	int	err;
	ssize_t	cc;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_getdirentries64,
			      "%hY%zu%oZ",
			      fhi,
			      nbytes,
			      basep);

	do {
		ibase = *basep;
		if (ibase != *basep) {
			cc =- EINVAL;
			break;
		}
		err = find_alias(fhi, &pno);
		if (err) {
			cc = -ESTALE;
			break;
		}

		cc = _sysio_open(pno, O_RDONLY, 0);
		if (cc)
			break;
		cc = _sysio_p_filldirentries(pno, buf, nbytes, &ibase);
		if (PNOP_CLOSE(pno) != 0)
			abort();
		P_PUT(pno);
		if (cc < 0)
			break;
		*basep = ibase;
	} while (0);

	SYSIO_INTERFACE_RETURN(cc,
			       (int )(cc <= 0 ? (int )-cc : 0),
			       fhi_getdirentries64, "%d%oZ", basep);
}

int
SYSIO_INTERFACE_NAME(fhi_statvfs64)(struct file_handle_info *fhi,
				    struct statvfs64 *buf)
{
	struct pnode *pno;
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(fhi_statvfs64,
			      "%hY",
			      fhi);

	do {
		err = find_alias(fhi, &pno);
		if (err)
			break;
		err = PNOP_STATVFS(pno, buf);
		P_PUT(pno);
	} while (0);

	SYSIO_INTERFACE_RETURN(err,
			       err,
			       fhi_statvfs64,
			       "%d%lvY",
			       buf,
			       0);
}
