/*
 *   This Cplant(TM) source code is the property of Sandia National
 *   Laboratories.
 *
 *   This Cplant(TM) source code is copyrighted by Sandia National
 *   Laboratories.
 *
 *   The redistribution of this Cplant(TM) source code is subject to the
 *   terms of the GNU Lesser General Public License
 *   (see cit/LGPL or http://www.gnu.org/licenses/lgpl.html)
 *
 *   Cplant(TM) Copyright 1998-2009 Sandia Corporation. 
 *   Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive
 *   license for use of this work by or on behalf of the US Government.
 *   Export of this program may require a license from the United States
 *   Government.
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

#include "smp.h"
#include "tree.h"

#if defined(AUTOMOUNT_FILE_NAME) && !defined(MAX_MOUNT_DEPTH)
/*
 *Maximum number of automounts to attempt in path traversal.
 */
#define MAX_MOUNT_DEPTH		64
#endif

/*
 * Each i-node is uniquely identified by a file identifier, supplied by
 * the relevant file system driver. The i-node number returned in the getattrs
 * call is not always enough.
 */
struct file_identifier {
	void	*fid_data;
	size_t	fid_len;
};

struct pnode;
struct inode;
struct intent;
struct intnl_dirent;
struct intnl_stat;
#ifdef _HAVE_STATVFS
struct intnl_statvfs;
#endif
struct io_arguments;
struct ioctx;

/*
 *Fill directory entry routines.
 */
typedef int (*filldir_t)(void *,
			 ino_t,
			 _SYSIO_OFF_T,
			 const char *, size_t, unsigned char);

/*
 *Operations on i-nodes.
 *
 *Should this be split up into file and name space operations?
 */
struct inode_ops {
	int	(*inop_lookup)(struct pnode *pno,
			       struct inode **ino,
			       struct intent *intnt,
			       const char *path);
	int	(*inop_getattr)(struct pnode *pno, struct intnl_stat *stbuf);
	int	(*inop_setattr)(struct pnode *pno,
				unsigned mask,
				struct intnl_stat *stbuf);
	ssize_t	(*inop_filldirentries)(struct pnode *pno,
				       _SYSIO_OFF_T *posp,
				       char *buf,
				       size_t nbytes);
	int	(*inop_filldirentries2)(struct pnode *pno,
					_SYSIO_OFF_T *,
					filldir_t,
					void *);
	int	(*inop_mkdir)(struct pnode *pno, mode_t mode);
	int	(*inop_rmdir)(struct pnode *pno);
	int	(*inop_symlink)(struct pnode *pno, const char *data);
	int	(*inop_readlink)(struct pnode *pno, char *buf, size_t bufsiz);
	int	(*inop_open)(struct pnode *pno, int flags, mode_t mode);
	int	(*inop_close)(struct pnode *pno);
	int	(*inop_link)(struct pnode *old, struct pnode* _new);
	int	(*inop_unlink)(struct pnode *pno);
	int	(*inop_rename)(struct pnode *old, struct pnode* _new);
	int	(*inop_read)(struct ioctx *ioctx);
	int	(*inop_write)(struct ioctx *ioctx);
	_SYSIO_OFF_T \
		(*inop_pos)(struct pnode *pno, _SYSIO_OFF_T off);
	int	(*inop_iodone)(struct ioctx *ioctx);
	int	(*inop_fcntl)(struct pnode *pno, int cmd, va_list ap, int *rtn);
	int	(*inop_isync)(struct ioctx *ioctx);
	int	(*inop_old_sync)(struct pnode *pno);
	int	(*inop_idatasync)(struct ioctx *ioctx);
	int	(*inop_old_datasync)(struct pnode *pno);
	int	(*inop_ioctl)(struct pnode *pno,
			      unsigned long int request,
			      va_list ap);
	int	(*inop_mknod)(struct pnode *pno, mode_t mode, dev_t dev);
#ifdef _HAVE_STATVFS
	int	(*inop_statvfs)(struct pnode *pno, struct intnl_statvfs *buf);
#endif
	int	(*inop_perms_check)(struct pnode *pno,
				    int amode,
				    int effective);
	void	(*inop_gone)(struct inode *ino);
};

/*
 *Values for the mask to inop_setattr.
 */
#if 0
#define SETATTR_MODE		0x01
#define SETATTR_MTIME		0x02
#define SETATTR_ATIME		0x04
#define SETATTR_UID		0x08
#define SETATTR_GID		0x10
#define SETATTR_LEN		0x20

#define FUSE_SET_ATTR_MODE	(1 << 0)
#define FUSE_SET_ATTR_UID	(1 << 1)
#define FUSE_SET_ATTR_GID	(1 << 2)
#define FUSE_SET_ATTR_SIZE	(1 << 3)
#define FUSE_SET_ATTR_ATIME	(1 << 4)
#define FUSE_SET_ATTR_MTIME	(1 << 5)
#define FUSE_SET_ATTR_ATIME_NOW	(1 << 7)
#define FUSE_SET_ATTR_MTIME_NOW	(1 << 8)
#define FUSE_SET_ATTR_CTIME	(1 << 10)

#endif

#define SETATTR_MODE		(1 << 0)
#define SETATTR_UID			(1 << 1)
#define SETATTR_GID			(1 << 2)
#define SETATTR_LEN			(1 << 3)
#define SETATTR_ATIME		(1 << 4)
#define SETATTR_MTIME		(1 << 5)

/*
 *An i-node record is maintained for each file object in the system.
 */
struct inode {
	struct file_identifier *i_fid;			/* file ident */
	struct tree_node i_tnode;			/* cache tnode record */
	mutex_t	i_mutex;				/* record mutex */
	unsigned i_lckcnt;				/* # recursive locks */
	unsigned
		i_immune:1,				/* immune from GC */
		i_zombie:1;				/* stale inode */
	unsigned i_ref;					/* soft ref counter */
	struct inode_ops i_ops;				/* operations */
	struct intnl_stat i_stbuf;			/* attrs */
	struct filesys *i_fs;				/* file system ptr */
#ifdef I_ASSOCIATIONS
	LIST_HEAD(, pnode_base) i_assoc;		/* assoc'd pbnodes */
#endif
	void	*i_private;				/* driver data */
	TAILQ_ENTRY(inode) i_nodes;			/* all i-nodes link */
};

#ifdef I_ASSOCIATIONS
#define _I_INIT_MORE(_ino) \
	do { \
		LIST_INIT(&(_ino)->i_assoc); \
	} while (0)
#else
#define _I_INIT_MORE(_ino)
#endif

/*
 * Check association; Logical true if so, otherwise false.
 *
 * NB: The given i-node should be locked or otherwise protected from
 * manipulation by others.
 */
#ifdef I_ASSOCIATIONS
#define I_ISASSOC(_ino) \
	((_ino)->i_assoc.lh_first)
#else
#define I_ISASSOC(_ino) \
	(0)
#endif

/*
 * Init an i-node record.
 */
#define I_INIT(ino, fs, stat, ops, fid, immunity, private) \
	do { \
		(ino)->i_fid = (fid); \
		(ino)->i_tnode.tn_key = (fid); \
		(ino)->i_tnode.tn_left = (ino)->i_tnode.tn_right = NULL; \
		mutex_init(&(ino)->i_mutex, MUTEX_RECURSIVE); \
		(ino)->i_lckcnt = 0; \
		(ino)->i_immune = (immunity) ? 1 : 0; \
		(ino)->i_zombie = 0; \
		(ino)->i_ref = 0; \
		(ino)->i_ops = *(ops); \
		(ino)->i_stbuf = *(stat); \
		(ino)->i_fs = (fs); \
		(ino)->i_private = (private); \
		_I_INIT_MORE(ino); \
	} while (0)

#define I_ISLOCKED(_ino) \
	((_ino)->i_lckcnt)

#ifdef LOCK_DEBUG
#define _I_CHECK_LOCK(_ino, _test) \
	(assert(((_test) && I_ISLOCKED(_ino)) || \
		!((_test) || I_ISLOCKED(_ino))))
#else
#define _I_CHECK_LOCK(_ino, _test)
#endif

/*
 * Attempt to kill an inode.
 */
#define I_GONE(_ino) \
	do { \
		_I_CHECK_LOCK((_ino), 1); \
		_sysio_i_gone(_ino); \
	} while (0)

#define I_LOCK(_ino) \
	do { \
		mutex_lock(&(_ino)->i_mutex); \
		(_ino)->i_lckcnt++; \
		_I_CHECK_LOCK(_ino, 1); \
	} while (0)

#define I_UNLOCK(_ino) \
	do { \
		_I_CHECK_LOCK(_ino, 1); \
		--(_ino)->i_lckcnt; \
		mutex_unlock(&(_ino)->i_mutex); \
	} while (0)

/*
 * Take soft reference to i-node.
 */
#define _I_REF_NO_LOCK(_ino) \
	do { \
		_I_CHECK_LOCK((_ino), 1); \
		(_ino)->i_ref++; \
		assert((_ino)->i_ref); \
	} while (0)

#define I_REF(_ino) \
	do { \
		I_LOCK(_ino); \
		_I_REF_NO_LOCK(_ino); \
		I_UNLOCK(_ino); \
	} while (0)

/*
 * Release soft reference to i-node, destroying it if last reference is
 * removed.
 */
#define _I_RELE_NO_LOCK(_ino) \
	do { \
		_I_CHECK_LOCK((_ino), 1); \
		assert((_ino)->i_ref); \
		if (!--(_ino)->i_ref && (_ino)->i_zombie) \
		I_GONE(_ino); \
	} while (0)

#define I_RELE(_ino) \
	do { \
		I_LOCK(_ino); \
		_I_RELE_NO_LOCK(_ino); \
		I_UNLOCK(_ino); \
	} while (0)

/*
 * Lock and reference i-node.
 */
#define I_GET(_ino) \
	do { \
		I_LOCK(_ino); \
		_I_REF_NO_LOCK(_ino); \
	} while (0)

/*
 * Unlock and drop reference to i-node.
 */
#define I_PUT(_ino) \
	do { \
		_I_RELE_NO_LOCK(_ino); \
		I_UNLOCK(_ino); \
	} while (0)

/*
 * The "quick string" record (inspired by the structure of the same name
 * from Linux) is used to pass a string without delimiters as well as useful
 * information about the string.
 */
struct qstr {
	const char *name;
	size_t	len;
	unsigned hashval;
};

/*
 * A path-base node is an entry in a directory. It may have many aliases, one
 * for each name space in which it occurs. This record holds the
 * common information.
 */
struct pnode_base {
	mutex_t	pb_mutex;				/* record mutex */
	unsigned pb_lckcnt;				/* # recursive locks */
	struct tree_node *pb_ncache;			/* names cache */
	struct tree_node pb_tentry;			/* cache node entry */
	struct pnode_base_key {
		struct qstr pbk_name;			/* entry name */
		struct pnode_base *pbk_parent;		/* parent */
	} pb_key;
	struct inode *pb_ino;				/* inode */
	struct tree_node *pb_children;			/* children if a dir */
	struct tree_node pb_sib;			/* sibling tree rec */
	LIST_HEAD(, pnode) pb_aliases;			/* aliases */
#ifdef I_ASSOCIATIONS
	LIST_ENTRY(pnode_base) pb_alinks;		/* names to same ino */
#endif
#ifdef PB_DEBUG
	TAILQ_ENTRY(pnode_base) pb_links;		/* leaf pbnodes links */
#endif
};

/*
 *Init a path-base record.
 */
#define PB_INIT(_pb, _name, _parent) \
	do { \
		mutex_init(&(_pb)->pb_mutex, MUTEX_RECURSIVE); \
		(_pb)->pb_lckcnt = 0; \
		(_pb)->pb_ncache = NULL; \
		(_pb)->pb_tentry.tn_key = &(_pb)->pb_key; \
		(_pb)->pb_tentry.tn_left = (_pb)->pb_tentry.tn_right = NULL; \
		(_pb)->pb_key.pbk_name = *(_name); \
		(_pb)->pb_key.pbk_parent = (_parent); \
		(_pb)->pb_ino = NULL; \
		(_pb)->pb_children = NULL; \
		(_pb)->pb_sib.tn_key = (_pb); \
		(_pb)->pb_sib.tn_left = (_pb)->pb_sib.tn_right = NULL; \
		LIST_INIT(&(_pb)->pb_aliases); \
	} while (0)

#define PB_ISLOCKED(_pb) \
	((_pb)->pb_lckcnt)

#ifdef LOCK_DEBUG
#define _PB_CHECK_LOCK(_pb, _test) \
	(assert(((_test) && PB_ISLOCKED(_pb)) || \
		!((_test) || PB_ISLOCKED(_pb))))
#else
#define _PB_CHECK_LOCK(_pb, _test)
#endif

#define PB_LOCK(_pb) \
	do { \
		mutex_lock(&(_pb)->pb_mutex); \
		(_pb)->pb_lckcnt++; \
		_PB_CHECK_LOCK((_pb), 1); \
	} while (0)

#define PB_UNLOCK(_pb) \
	do { \
		_PB_CHECK_LOCK((_pb), 1); \
		(_pb)->pb_lckcnt--; \
		mutex_unlock(&(_pb)->pb_mutex); \
	} while (0)

/*
 *Lock path-base node and get associated i-node if present.
 */
#define PB_GET(_pb) \
	do { \
		PB_LOCK(_pb); \
		if ((_pb)->pb_ino) \
		I_GET((_pb)->pb_ino); \
	} while (0)

/*
 *Unlock path-base node and put associated i-node if present.
 */
#define PB_PUT(_pb) \
	do { \
		_PB_CHECK_LOCK((_pb), 1); \
		if ((_pb)->pb_ino) \
		I_PUT((_pb)->pb_ino); \
		PB_UNLOCK(_pb); \
	} while (0)

#ifdef I_ASSOCIATIONS
#define _PB_ADD_ASSOC(_pb) \
	do { \
		LIST_INSERT_HEAD(&(_pb)->pb_ino->i_assoc, \
				 (_pb), \
				 pb_alinks); \
	} while (0)
#define _PB_REMOVE_ASSOC(_pb) \
	do { \
		LIST_REMOVE((_pb), pb_alinks); \
	} while (0)
#else
#define _PB_ADD_ASSOC(_pb)
#define _PB_REMOVE_ASSOC(_pb)
#endif

/*
 * Set path-base node association.
 *
 * NB: All of the path-base node, any i-node it currently points at, and
 * the new i-node must be locked. If the path-base node is associated,
 * the association will be removed before making the new but it will not be
 * released, unlocked, or dereferenced.
 *
 * NB(2): We no longer support re-association. Now, an association can be
 * set, anew, or cleared (only when destroying the path-base node.
 */
#define PB_SET_ASSOC(_pb, _ino) \
	do { \
		_PB_CHECK_LOCK((_pb), 1); \
		if ((_pb)->pb_ino) { \
			assert(!(_ino)); \
			_I_CHECK_LOCK((_pb)->pb_ino, 1); \
			_PB_REMOVE_ASSOC(_pb); \
			_I_RELE_NO_LOCK((_pb)->pb_ino); \
		} \
		(_pb)->pb_ino = (_ino); \
		if ((_pb)->pb_ino) { \
			_I_CHECK_LOCK((_pb)->pb_ino, 1); \
			_PB_ADD_ASSOC(_pb); \
			_I_REF_NO_LOCK((_pb)->pb_ino); \
		} \
	} while (0)

/*
 * Since a file system may be multiply mounted, in different parts of the local
 * tree, a file system object may appear in different places. We handle that
 * with aliases. There is one pnode for every alias the system is tracking.
 *
 * Name space traversal depends heavily on the interpretation of many
 * of the fields in this structure. For that reason a detailed discussion
 * of the various fields is given.
 *
 * The reference field records soft references to the record. For instance,
 * it tracks file and directory opens. It does not track sibling references,
 * though, as those are hard references and can be found by examining the
 * aliases list in the base part of the node.
 *
 * The parent value points to the parent directory for this entry, in the
 * *system* name space -- Not the mounted volumes. If you want to examine
 * the moutned volume name space, use the base record.
 *
 * The base value points to the base path node information. It is info common
 * to all of the aliases.
 *
 * The mount value points to the mount record for the rooted name space in
 * which the alias is found. Notably, if a node is the root of a sub-tree then
 * the mount record, among other things, indicates another node
 * (in another sub-tree) that is covered by this one.
 *
 * Another sub-tree, mounted on this node, is indicated by a non-null cover.
 * The pnode pointed to, then, is the root of the mounted sub-tree.
 *
 * The links list entry holds pointers to other aliases for the base path
 * node entry.
 *
 * The nodes link is bookkeeping.
 */
struct pnode {
	mutex_t	p_mutex;				/* record mutex */
	unsigned p_lckcnt;				/* # recursive locks */
	unsigned p_ref;					/* soft ref count */
	struct pnode *p_parent;				/* parent */
	struct pnode_base *p_base;			/* base part */
	struct mount *p_mount;				/* mount info */
	struct pnode *p_cover;				/* covering pnode */
	LIST_ENTRY(pnode) p_links;			/* other aliases */
	TAILQ_ENTRY(pnode) p_idle;			/* idle list links */
};

/*
 * Init path node record.
 */
#define P_INIT(_pno, _parent, _pb, _mnt, _cover) \
	do { \
		mutex_init(&(_pno)->p_mutex, MUTEX_RECURSIVE); \
		(_pno)->p_lckcnt = 0; \
		(_pno)->p_ref = 0; \
		(_pno)->p_parent = (_parent); \
		(_pno)->p_base = (_pb); \
		(_pno)->p_mount = (_mnt); \
		(_pno)->p_cover = (_cover); \
	} while (0)

#define P_ISLOCKED(_pno) \
	((_pno)->p_lckcnt)

#ifdef LOCK_DEBUG
#define _P_CHECK_LOCK(_pno, _test) \
	(assert(((_test) && P_ISLOCKED(_pno)) || \
		!((_test) || P_ISLOCKED(_pno))))
#else
#define _P_CHECK_LOCK(_pno, _test)
#endif

#define P_LOCK(_pno) \
	do { \
		mutex_lock(&(_pno)->p_mutex); \
		(_pno)->p_lckcnt++; \
		_P_CHECK_LOCK((_pno), 1); \
	} while (0)

#define P_UNLOCK(_pno) \
	do { \
		_P_CHECK_LOCK((_pno), 1); \
		(_pno)->p_lckcnt--; \
		mutex_unlock(&(_pno)->p_mutex); \
	} while (0)

/*
 * Reference path-tree node.
 */
#define _P_REF_NO_LOCK(_pno) \
	do { \
		_P_CHECK_LOCK((_pno), 1); \
		if (!(_pno)->p_ref++) \
		TAILQ_REMOVE(&_sysio_idle_pnodes, (_pno), p_idle); \
		assert((_pno)->p_ref); \
	} while (0)

#define P_REF(_pno) \
	do { \
		P_LOCK(_pno); \
		_P_REF_NO_LOCK(_pno); \
		P_UNLOCK(_pno); \
	} while (0)

/*
 * Release reference to path-tree node.
 */
#define _P_RELE_NO_LOCK(_pno) \
	do { \
		_P_CHECK_LOCK((_pno), 1); \
		assert((_pno)->p_ref); \
		if (!--(_pno)->p_ref) \
		TAILQ_INSERT_TAIL(&_sysio_idle_pnodes, (_pno), p_idle); \
	} while (0)

#define P_RELE(_pno) \
	do { \
		P_LOCK(_pno); \
		_P_RELE_NO_LOCK(_pno); \
		P_UNLOCK(_pno); \
	} while (0)

/*
 * Lock and reference pnode and get associated path-base node.
 */
#define P_GET(_pno) \
	do { \
		P_LOCK(_pno); \
		_P_REF_NO_LOCK(_pno); \
		PB_GET((_pno)->p_base); \
	} while (0)

/*
 * Unlock and drop reference to pnode and put associated path-base node.
 * i-node.
 */
#define P_PUT(_pno) \
	do { \
		_P_CHECK_LOCK((_pno), 1); \
		PB_PUT((_pno)->p_base); \
		_P_RELE_NO_LOCK(_pno); \
		P_UNLOCK(_pno); \
	} while (0)

/*
 * Path node meta-call.
 */
#define INOP_CALL(_f, ...) \
	((*_f)(__VA_ARGS__))
#define _INOP_FUNC(_ino, _fbase) \
	((_ino)->i_ops.inop_##_fbase)
#define PNOP_FUNC(_pno, _fbase) \
	_INOP_FUNC((_pno)->p_base->pb_ino, _fbase)
#define _PNOP_CALL(_pno, _fbase, ...) \
	INOP_CALL(PNOP_FUNC((_pno), _fbase), __VA_ARGS__)
#define _PNOP_MKCALL(_pno, _fbase, ...) \
	((_pno)->p_base->pb_ino \
	   ? _PNOP_CALL((_pno), _fbase, __VA_ARGS__) \
	   : _PNOP_CALL((_pno)->p_mount->mnt_root, _fbase, __VA_ARGS__))

/*
 * Operations
 */
#define PNOP_LOOKUP(_pno, _inop, _intnt, _path) \
	_PNOP_MKCALL((_pno), lookup, (_pno), (_inop), (_intnt), (_path))
#define PNOP_GETATTR(_pno, _stbuf) \
	_PNOP_CALL((_pno), getattr, (_pno), (_stbuf))
#define PNOP_SETATTR(_pno, _mask, _stbuf) \
	_PNOP_CALL((_pno), setattr, \
		   (_pno), (_mask), (_stbuf))
#define PNOP_FILLDIRENTRIES(_pno, _posp, _buf, _nbytes) \
	_PNOP_CALL((_pno), filldirentries, (_pno), (_posp), (_buf), (_nbytes))
#define PNOP_FILLDIRENTRIES2(_pno, _posp, _fill, _data) \
	_PNOP_CALL((_pno), filldirentries2, (_pno), (_posp), (_fill), (_data))
#define PNOP_MKDIR(_pno, _mode) \
	_PNOP_MKCALL((_pno), mkdir, (_pno), (_mode))
#define PNOP_RMDIR(_pno) \
	_PNOP_MKCALL((_pno), rmdir, (_pno))
#define PNOP_SYMLINK(_pno, _data) \
	_PNOP_MKCALL((_pno), symlink, (_pno), (_data))
#define PNOP_READLINK(_pno, _buf, _bufsiz) \
	_PNOP_CALL((_pno), readlink, (_pno), (_buf), (_bufsiz))
#define PNOP_OPEN(_pno, _flags, _mode) \
	_PNOP_MKCALL((_pno), open, (_pno), (_flags), (_mode))
#define PNOP_CLOSE(_pno) \
	_PNOP_CALL((_pno), close, (_pno))
#define PNOP_LINK(_old, _new) \
	_PNOP_CALL((_old), link, (_old), (_new))
#define PNOP_UNLINK(_pno) \
	_PNOP_CALL((_pno), unlink, (_pno))
#define PNOP_RENAME(_old, _new) \
	_PNOP_CALL((_old), rename, (_old), (_new))
#define PNOP_READ(_ioctx) \
	_PNOP_CALL((_ioctx)->ioctx_pno, read, (_ioctx))
#define PNOP_WRITE(_ioctx) \
	_PNOP_CALL((_ioctx)->ioctx_pno, write, (_ioctx))
#define PNOP_POS(_pno, _off) \
	_PNOP_CALL((_pno), pos, (_pno), (_off))
#define PNOP_IODONE(_ioctx) \
	_PNOP_CALL((_ioctx)->ioctx_pno, iodone, (_ioctx))
#define PNOP_FCNTL(_pno, _cmd, _ap, _rtn) \
	_PNOP_CALL((_pno), fcntl, (_pno), (_cmd), (_ap), (_rtn))
#define PNOP_SYNC(_pno) \
	_PNOP_CALL((_pno), sync, (_pno))
#define PNOP_DATASYNC(_pno) \
	_PNOP_CALL((_pno), datasync, (_pno))
#define PNOP_IOCTL(_pno, _request, _ap) \
	_PNOP_CALL((_pno), ioctl, (_pno), (_request), (_ap))
#define PNOP_MKNOD(_pno, _mode, _dev) \
	_PNOP_MKCALL((_pno), mknod, (_pno), (_mode), (_dev))
#ifdef _HAVE_STATVFS
#define PNOP_STATVFS(_pno, _buf) \
	_PNOP_CALL((_pno), statvfs, (_pno), (_buf))
#endif
#define PNOP_PERMS_CHECK(_pno, _amode, _eff) \
	_PNOP_MKCALL((_pno), perms_check, (_pno), (_amode), (_eff))

/*
 * An intent record allows callers of namei and lookup to pass some information
 * about what they want to accomplish in the end.
 */
struct intent {
	unsigned int_opmask;
	void	*int_arg1;
	void	*int_arg2;
};

/*
 * Intent operations.
 */
#define INT_GETATTR		0x01			/* get attrs */
#define INT_SETATTR		0x02			/* set attrs */
#define INT_UPDPARENT		0x04			/* insert/delete */
#define INT_OPEN		0x08			/* open */
#define INT_CREAT		(0x10|INT_UPDPARENT)	/* creat obj */
#define INT_READLINK		0x20			/* readlink */

#define INTENT_INIT(intnt, mask, arg1, arg2) \
	do { \
		(intnt)->int_opmask = (mask); \
		(intnt)->int_arg1 = (arg1); \
		(intnt)->int_arg2 = (arg2); \
	} while (0)

/*
 * Bundled up arguments to _sysio_path_walk.
 */
struct nameidata {
	unsigned nd_flags;				/* flags (see below) */
	const char *nd_path;				/* path arg */
	struct pnode *nd_pno;				/* returned pnode */
	struct pnode *nd_root;				/* system/user root */
	struct intent *nd_intent;			/* intent (NULL ok) */
	unsigned nd_slicnt;				/* symlink indirects */
#ifdef AUTOMOUNT_FILE_NAME
	unsigned nd_amcnt;				/* automounts */
#endif
};

/*
 * Values for nameidata flags field.
 */
#define ND_NOFOLLOW		0x01			/* no follow symlinks */
#define ND_NEGOK		0x02			/* last missing is ok */
#define ND_NOPERMCHECK		0x04			/* don't check perms */
#define ND_NXMNT		0x08			/* don't cross mounts */
#define ND_WANTPARENT		0x10			/* want parent too */

#ifdef AUTOMOUNT_FILE_NAME
#define _ND_INIT_AUTOMOUNT(nd)	((nd)->nd_amcnt = 0)
#else
#define _ND_INIT_AUTOMOUNT(nd)
#endif

#define _ND_INIT_OTHERS(nd) \
	_ND_INIT_AUTOMOUNT(nd)

/*
 * Init nameidata record.
 */
#define ND_INIT(nd, flags, path, root, intnt) \
	do { \
		(nd)->nd_flags = (flags); \
		(nd)->nd_path = (path); \
		(nd)->nd_pno = NULL; \
		(nd)->nd_root = (root); \
		(nd)->nd_intent = (intnt); \
		(nd)->nd_slicnt = 0; \
		_ND_INIT_OTHERS(nd); \
	} while (0)

/*
 * IO completion callback record.
 */
struct ioctx_callback {
	TAILQ_ENTRY(ioctx_callback) iocb_next;		/* list link */
	void	(*iocb_f)(struct ioctx *, void *);	/* cb func */
	void	*iocb_data;				/* cb data */
};

/*
 * All IO internally is done with an asynchronous mechanism. This record
 * holds the completion information. It's too big :-(
 */
struct ioctx {
	LIST_ENTRY(ioctx) ioctx_link;			/* AIO list link */
	unsigned
		ioctx_fast:1,				/* from stack space */
		ioctx_done:1;				/* transfer complete */
	struct pnode *ioctx_pno;			/* p-node */
	const struct iovec *ioctx_iov;			/* scatter/gather vec */
	size_t	ioctx_iovlen;				/* iovec length */
	const struct intnl_xtvec *ioctx_xtv;		/* extents */
	size_t	ioctx_xtvlen;				/* xtv length */
	ssize_t	ioctx_cc;				/* rtn char count */
	int	ioctx_errno;				/* error number */
	void	*ioctx_args;				/* op args */
	TAILQ_HEAD(, ioctx_callback) ioctx_cbq;		/* callback queue */
	void	*ioctx_private;				/* driver data */
};

/*
 * Init IO context record.
 */
#define IOCTX_INIT(ioctx, fast, pno, iov, iovlen, xtv, xtvlen, args) \
	do { \
		(ioctx)->ioctx_fast = (fast); \
		(ioctx)->ioctx_done = 0; \
		(ioctx)->ioctx_pno = (pno); \
		(ioctx)->ioctx_iov = (iov); \
		(ioctx)->ioctx_iovlen = (iovlen); \
		(ioctx)->ioctx_xtv = (xtv); \
		(ioctx)->ioctx_xtvlen = (xtvlen); \
		(ioctx)->ioctx_cc = 0; \
		(ioctx)->ioctx_errno = 0; \
		(ioctx)->ioctx_args = (args); \
		TAILQ_INIT(&(ioctx)->ioctx_cbq); \
		(ioctx)->ioctx_private = NULL; \
	} while (0)

/*
 * Return whether access to a pnode is read-only.
 */
#define IS_RDONLY(pno) \
	((pno)->p_mount->mnt_flags & MOUNT_F_RO)

extern struct pnode *_sysio_root;

extern TAILQ_HEAD(inodes_head, inode) _sysio_inodes;
extern TAILQ_HEAD(pnodes_head, pnode) _sysio_idle_pnodes;

extern int _sysio_i_init(void);

#ifdef ZERO_SUM_MEMORY
extern void _sysio_i_shutdown(void);
#endif
extern struct inode *_sysio_i_new(struct filesys *fs,
				  struct file_identifier *fid,
				  struct intnl_stat *stat,
				  unsigned immunity,
				  struct inode_ops *ops,
				  void* _private);
extern struct inode *_sysio_i_find(struct filesys *fs,
				   struct file_identifier *fid);
extern void _sysio_i_gone(struct inode *ino);
extern void _sysio_i_undead(struct inode *ino);
extern struct pnode_base *_sysio_pb_new(struct qstr *name,
					struct pnode_base *parent,
					struct inode *ino);
extern void _sysio_pb_gone(struct pnode_base *pb);
extern void _sysio_pb_disconnect(struct pnode_base *pb);
extern int _sysio_p_path(struct pnode *pno, char **bufp, size_t size);
#ifdef P_DEBUG
extern void _sysio_p_show(const char *pre, struct pnode *pno);
#endif
extern int _sysio_p_find_alias(struct pnode *parent,
			       struct qstr *name,
			       struct pnode **pnop);
extern int _sysio_p_validate(struct pnode *pno,
			     struct intent *intnt,
			     const char *path);
extern struct pnode *_sysio_p_new_alias(struct pnode *parent,
					struct pnode_base *pb,
					struct mount *mnt);
extern void _sysio_p_gone(struct pnode *pno);
extern size_t _sysio_p_prune(struct pnode *root);
extern void _sysio_p_get2(struct pnode *pno1, struct pnode *pno2);
extern int _sysio_pb_pathof(struct pnode_base *pb,
			    char separator, char **pathp);
extern char *_sysio_pb_path(struct pnode_base *pb, char separator);
extern ssize_t _sysio_p_filldirentries(struct pnode *pno,
				       char *buf,
				       size_t nbytes,
				       _SYSIO_OFF_T * __restrict basep);
extern int _sysio_p_setattr(struct pnode *pno,
			    unsigned mask,
			    struct intnl_stat *stbuf);
extern int _sysio_p_link(struct pnode *old, struct pnode* _new);
extern int _sysio_p_unlink(struct pnode *pno);
extern int _sysio_p_symlink(const char *oldpath, struct pnode* _new);
extern int _sysio_p_rmdir(struct pnode *pno);
extern ssize_t _sysio_p_readlink(struct pnode *pno, char *buf, size_t bufsiz);
extern int _sysio_p_rename(struct pnode *old, struct pnode* _new);
extern int _sysio_p_iiox(int (*f)(struct ioctx *),
			 struct pnode *pno,
			 _SYSIO_OFF_T limit,
			 const struct iovec *iov,
			 size_t iov_count,
			 void (*release_iov)(struct ioctx *, void *),
			 const struct intnl_xtvec *xtv,
			 size_t xtv_count,
			 void (*release_xtv)(struct ioctx *, void *),
			 void (*completio)(struct ioctx *, void *),
			 void *data,
			 void *args,
			 struct ioctx **ioctxp);
extern void _sysio_do_noop(void);
extern void _sysio_do_illop(void);
extern int _sysio_do_ebadf(void);
extern int _sysio_do_einval(void);
extern int _sysio_do_enoent(void);
extern int _sysio_do_enodev(void);
extern int _sysio_do_espipe(void);
extern int _sysio_do_eisdir(void);
extern int _sysio_do_enosys(void);
extern int _sysio_path_walk(struct pnode *parent, struct nameidata *nd);
#ifdef AUTOMOUNT_FILE_NAME
extern void _sysio_next_component(const char *path, struct qstr *name);
#endif
extern int _sysio_epermitted(struct pnode *pno, int amode, int effective);
extern int _sysio_permitted(struct pnode *pno, int amode);
extern int _sysio_namei(struct pnode *pno,
			const char *path,
			unsigned flags,
			struct intent *intnt,
			struct pnode **pnop);
extern int _sysio_p_chdir(struct pnode *pno);
extern int _sysio_ioctx_init(void);
extern void _sysio_ioctx_enter(struct ioctx *ioctx);
extern struct ioctx *_sysio_ioctx_new(struct pnode *pno,
				      const struct iovec *iov,
				      size_t iovlen,
				      const struct intnl_xtvec *xtv,
				      size_t xtvlen,
				      void *args);
extern int _sysio_ioctx_cb(struct ioctx *ioctx,
			   void (*f)(struct ioctx *, void *),
			   void *data);
extern void _sysio_ioctx_cb_free(struct ioctx_callback *cb);
extern struct ioctx *_sysio_ioctx_find(void *id);
extern int _sysio_ioctx_done(struct ioctx *ioctx);
extern ssize_t _sysio_ioctx_wait(struct ioctx *ioctx);
extern void _sysio_ioctx_complete(struct ioctx *ioctx);
extern int _sysio_open(struct pnode *pno, int flags, mode_t mode);
extern int _sysio_p_aread(struct pnode *pno,
			  _SYSIO_OFF_T off,
			  void *buf,
			  size_t count,
			  struct ioctx **ioctxp);
extern int _sysio_p_awrite(struct pnode *pno,
			   _SYSIO_OFF_T off,
			   void *buf,
			   size_t count,
			   struct ioctx **ioctxp);
extern int _sysio_mkdir(struct pnode *where, mode_t mode);
extern int _sysio_mknod(struct pnode *where, mode_t mode, dev_t dev);
extern int _sysio_p_generic_perms_check(struct pnode *pno,
					int amode,
					int effective);
