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
#include "mount.h"
#include "inode.h"
#include "dev.h"

/*
 * Support for path and index nodes.
 */

/*
 * Size of all names bucket-hash table.
 */
#ifndef NAMES_TABLE_LEN
#define NAMES_TABLE_LEN		251
#endif

/*
 * Desired high-water mark.
 */
#define P_RECLAIM_MIN		511
#if P_RECLAIM_MIN > (NAMES_TABLE_LEN / 4)
#undef P_RECLAIM_MIN
#define P_RECLAIM_MIN		(NAMES_TABLE_LEN / 4)
#endif

/*
 * Active i-nodes in the system and the number of same.
 */
struct inodes_head _sysio_inodes;
static size_t n_inodes = 0;

/*
 * Number of names tracked by the system.
 */
static size_t n_names = 0;

/*
 * List of all path-nodes (aliases) that are not currently referenced.
 */
struct pnodes_head _sysio_idle_pnodes;

#ifdef PB_DEBUG
static TAILQ_HEAD(, pnode_base) pb_leaf_nodes;
#endif

/*
 * The system root -- Aka `/'.
 */
struct pnode *_sysio_root = NULL;

#ifdef INO_CACHE_STATS
static struct {
	unsigned long long cst_iprobes;
	unsigned long long cst_ihits;
	unsigned long long cst_pbprobes;
	unsigned long long cst_pbhits;
	unsigned long long cst_preclaims;
	unsigned long long cst_pexamined;
	unsigned long long cst_pdismissed;
} cstats;

static void
ino_cstats_init()
{

	cstats.cst_iprobes = cstats.cst_ihits = 0;
	cstats.cst_pbprobes = cstats.cst_pbhits = 0;
	cstats.cst_preclaims =
	    cstats.cst_pexamined =
	    cstats.cst_pdismissed = 0;
}

#define INO_CST_UPDCNT(__which) \
	do { \
		(cstats.cst_##__which)++; \
	} while (0)
#else
#define INO_CST_UPDCNT(__which) \
	while (0)
#endif

/*
 * Initialize path and i-node support. Must be called before any other
 * routine in this module.
 */
int
_sysio_i_init()
{

	TAILQ_INIT(&_sysio_inodes);
#ifdef PB_DEBUG
	TAILQ_INIT(&pb_leaf_nodes);
#endif
	TAILQ_INIT(&_sysio_idle_pnodes);

#ifdef INO_CACHE_STATS
	ino_cstats_init();
#endif

	return 0;
}

/*
 * Compare two file identifiers, returning -1 if length of first is
 * less than the second, or 1 if length of first greater than then the
 * second, or the result of a memory compare if lengths are the same.
 */
static int
compar_fid(const struct file_identifier *fid1,
	   const struct file_identifier *fid2)
{

	if (fid1->fid_len < fid2->fid_len)
		return -1;
	if (fid1->fid_len > fid2->fid_len)
		return 1;
	return memcmp(fid1->fid_data, fid2->fid_data, fid1->fid_len);
}

/*
 * Insert inode into per-fs inode cache.
 *
 * NB: The given inode must not already be in the cache.
 */
static void
icache_insert(struct inode *ino)
{

	if (_sysio_tree_search(&ino->i_tnode,
			       &ino->i_fs->fs_icache,
			       (int (*)(const void *,
			       		const void *))compar_fid) !=
	    &ino->i_tnode)
		abort();
}

/*
 * Remove inode from inode cache.
 *
 * NB: The inode must be present in the cache.
 */
static void
icache_delete(struct inode *ino)
{

	if (_sysio_tree_delete(ino->i_fid,
			       &ino->i_fs->fs_icache,
			       (int (*)(const void *,
			       		const void *))compar_fid) !=
	    &ino->i_tnode)
		abort();
}

/*
 * Find, and return, inode with given file identifier. If not found, NULL is
 * returned instead.
 */
static struct inode *
icache_lookup(struct filesys *fs, struct file_identifier *fid)
{
	struct tree_node *tn;

	tn =
	    _sysio_tree_find(fid,
			     &fs->fs_icache,
			     (int (*)(const void *,
			     	      const void *))compar_fid);
	return tn ? TREE_ENTRY(tn, inode, i_tnode) : NULL;
}

/*
 * Allocate and initialize a new i-node. Returned i-node is referenced.
 *
 * NB: The passed file identifier is not copied. It is, therefor, up to the
 * caller to assure that the value is static until the inode is destroyed.
 */
struct inode *
_sysio_i_new(struct filesys *fs,
	     struct file_identifier *fid,
	     struct intnl_stat *stat,
	     unsigned immunity,
	     struct inode_ops *ops,
	     void *private)
{
	struct inode *ino;
	struct inode_ops operations;

	ino = malloc(sizeof(struct inode));
	if (!ino)
		return NULL;
	operations = *ops;
	if (S_ISBLK(stat->st_mode) ||
	    S_ISCHR(stat->st_mode) ||
	    S_ISFIFO(stat->st_mode)) {
		struct inode_ops *o;

		/*
		 * Replace some operations sent with
		 * those from the device table.
		 */
		o = _sysio_dev_lookup(stat->st_mode, stat->st_rdev);
		operations.inop_open = o->inop_open;
		operations.inop_close = o->inop_close;
		operations.inop_read = o->inop_read;
		operations.inop_write = o->inop_write;
		operations.inop_pos = o->inop_pos;
		operations.inop_iodone = o->inop_iodone;
		operations.inop_fcntl = o->inop_fcntl;
		operations.inop_old_datasync = o->inop_old_datasync;
		operations.inop_idatasync = o->inop_idatasync;
		operations.inop_ioctl = o->inop_ioctl;
	}
	I_INIT(ino, fs, stat, &operations, fid, immunity, private);
	ino->i_ref = 1;
	TAILQ_INSERT_TAIL(&_sysio_inodes, ino, i_nodes);

	I_GET(ino);
	icache_insert(ino);
	I_RELE(ino);

	n_inodes++;
	assert(n_inodes);

	return ino;
}

/*
 * Find existing i-node given i-number and pointers to FS record
 * and identifier.
 */
struct inode *
_sysio_i_find(struct filesys *fs, struct file_identifier *fid)
{
	struct inode *ino;

	INO_CST_UPDCNT(iprobes);
	ino = icache_lookup(fs, fid);
	if (!ino)
		return NULL;
	I_GET(ino);
	INO_CST_UPDCNT(ihits);
	return ino;
}

/*
 * Force reclaim of idle i-node.
 */
void
_sysio_i_gone(struct inode *ino)
{

	if (ino->i_ref)
		abort();
#ifdef I_ASSOCIATIONS
	assert(!I_ISASSOC(ino));
#endif
	if (!ino->i_zombie) 
		icache_delete(ino);
	TAILQ_REMOVE(&_sysio_inodes, ino, i_nodes);
	(*ino->i_ops.inop_gone)(ino);
	I_UNLOCK(ino);
	mutex_destroy(&ino->i_mutex);
	free(ino);

	assert(n_inodes);
	n_inodes--;
}

/*
 * Stale inode, zombie it and move it out of the way 
 */
void
_sysio_i_undead(struct inode *ino)
{
	
	if (ino->i_zombie)
		return;
	ino->i_zombie = 1;
	icache_delete(ino);
}

#ifdef PB_DEBUG
static void
p_reclaim_debug()
{
	struct pnode_base *nxt, *pb;
	unsigned long npb, npbleaves, npborphans;

	npb = 0;
	npbleaves = 0;
	npborphans = 0;

	nxt = pb_leaf_nodes.tqh_first;
	while ((pb = nxt)) {
		nxt = pb->pb_links.tqe_next;
		npb++;
		if (!pb->pb_children)
			npbleaves++;
		else if (!pb->pb_aliases.lh_first)
			npborphans++;
	}
	_sysio_cprintf("PBSTATS: n %lu, leaves %lu, orphans %lu\n",
		       npb,
		       npbleaves,
		       npborphans);
}
#endif

/*
 * Garbage collect idle path (and base path) nodes tracked by the system.
 */
static void
p_reclaim(unsigned limit)
{
	struct pnode *next, *pno;
	struct pnode_base *pb;

	INO_CST_UPDCNT(preclaims);
	next = _sysio_idle_pnodes.tqh_first;
	if (!next)
		return;
	do {
		INO_CST_UPDCNT(pexamined);
		pno = next;
		P_LOCK(pno);
		next = pno->p_idle.tqe_next;
		if (pno->p_ref)
			abort();
		if (pno->p_mount->mnt_root == pno) {
			/*
			 * We never reclaim mount points, here.
			 */
			P_UNLOCK(pno);
			continue;
		}
		if (pno->p_base->pb_children) {
			/*
			 * Must not dismiss from interior nodes. There
			 * might be aliases on the child pointing
			 * at this one.
			 */
			P_UNLOCK(pno);
			continue;
		}
		INO_CST_UPDCNT(pdismissed);
		PB_LOCK(pno->p_base);
		pb = pno->p_base;
		_sysio_p_gone(pno);
		if (!(pb->pb_children || pb->pb_aliases.lh_first))
			_sysio_pb_gone(pb);
		else
			PB_UNLOCK(pb);
	} while ((!limit || limit-- > 1) && next);

#ifdef PB_DEBUG
	p_reclaim_debug();
#endif
}

static int
compar_pb_key(const struct pnode_base_key *pbk1,
	      const struct pnode_base_key *pbk2)
{

#ifdef notdef
	if (pbk1->pbk_parent < pbk2->pbk_parent)
		return -1;
	if (pbk1->pbk_parent > pbk2->pbk_parent)
		return 1;
#endif

	if (pbk1->pbk_name.hashval < pbk2->pbk_name.hashval)
		return -1;
	if (pbk1->pbk_name.hashval > pbk2->pbk_name.hashval)
		return 1;

	if (pbk1->pbk_name.len < pbk2->pbk_name.len)
		return -1;
	if (pbk1->pbk_name.len > pbk2->pbk_name.len)
		return 1;

	return strncmp(pbk1->pbk_name.name,
		       pbk2->pbk_name.name,
		       pbk1->pbk_name.len);
}

/*
 * Insert a path base node into the system name cache.
 */
static void
ncache_insert(struct pnode_base *pb)
{

	assert(pb->pb_key.pbk_parent);
	if (_sysio_tree_search(&pb->pb_tentry,
			       &pb->pb_key.pbk_parent->pb_ncache,
			       (int (*)(const void *,
			       		const void *))compar_pb_key) !=
	    &pb->pb_tentry)
		abort();
}

/*
 * Delete path base node from the system name cache.
 */
static void
ncache_delete(struct pnode_base *pb)
{

	assert(pb->pb_key.pbk_parent);
	if (_sysio_tree_delete(&pb->pb_key,
			       &pb->pb_key.pbk_parent->pb_ncache,
			       (int (*)(const void *,
			       		const void *))compar_pb_key) !=
	    &pb->pb_tentry)
		abort();
}

/*
 * Lookup path base node in the system name cache given key info.
 */
static struct pnode_base *
ncache_lookup(struct pnode_base *pb, struct pnode_base_key *pbk)
{
	struct tree_node *tn;

	tn =
	    _sysio_tree_find(pbk,
	   		     &pb->pb_ncache,
			     (int (*)(const void *,
			     	      const void *))compar_pb_key);
	return tn ? TREE_ENTRY(tn, pnode_base, pb_tentry) : NULL;
}

/*
 * Compare two addresses.
 */
static int
compar_addr(const void *a, const void *b)
{
	ptrdiff_t diff;

	diff = (char *)a - (char *)b;
	if (diff < 0)
		return -1;
	if (diff > 0)
		return 1;
	return 0;
}

/*
 * Allocate and initialize a new base path node.
 */
struct pnode_base *
_sysio_pb_new(struct qstr *name, struct pnode_base *parent, struct inode *ino)
{
	struct pnode_base *pb;
	static struct qstr noname = { NULL, 0, 0 };

	if (n_names > NAMES_TABLE_LEN) {
		unsigned n;
		/*
		 * Limit growth.
		 */
		n = n_names - NAMES_TABLE_LEN;
		if (n < P_RECLAIM_MIN)
			n = P_RECLAIM_MIN;
		p_reclaim(n);
	}

	pb = malloc(sizeof(struct pnode_base) + name->len + 1);
	if (!pb)
		return NULL;

	if (!name)
		name = &noname;
	PB_INIT(pb, name, parent);
	PB_SET_ASSOC(pb, ino);
	PB_LOCK(pb);

	if (pb->pb_key.pbk_parent) {
#if defined(PB_DEBUG)
		if (!parent->pb_children) {
			/*
			 * The parent is no longer a leaf. We
			 * need to remove it from that list.
			 */
			TAILQ_REMOVE(&pb_leaf_nodes, parent, pb_links);
		}
#endif
		if (_sysio_tree_search(&pb->pb_sib,
				       &pb->pb_key.pbk_parent->pb_children,
				       (int (*)(const void *,
				       		const void *))compar_addr) !=
		    &pb->pb_sib)
			abort();			/* can't happen */
	}
	if (pb->pb_key.pbk_name.len) {
		char	*cp;

		/*
		 * Copy the passed name.
		 *
		 * We have put the space for the name immediately behind
		 * the record in order to maximize spatial locality.
		 */
		cp = (char *)pb + sizeof(struct pnode_base);
		(void )strncpy(cp, name->name, name->len);
		pb->pb_key.pbk_name.name = cp;
		*(cp + name->len) = 0; /* just set the string end to zero */
		ncache_insert(pb);
	}
#ifdef PB_DEBUG
	TAILQ_INSERT_TAIL(&pb_leaf_nodes, pb, pb_links);
#endif
	PB_GET(pb);
	PB_UNLOCK(pb);

	n_names++;
	assert(n_names);

	return pb;
}

/*
 * Force reclaim of idle base path node.
 */
void
_sysio_pb_gone(struct pnode_base *pb)
{
	struct inode *ino;

	assert(n_names);
	n_names--;

	assert(!pb->pb_aliases.lh_first);
	assert(!pb->pb_children);

	if (pb->pb_key.pbk_name.len)
		ncache_delete(pb);
	if (pb->pb_key.pbk_parent) {
		if (_sysio_tree_delete(pb,
				       &pb->pb_key.pbk_parent->pb_children,
				       (int (*)(const void *,
				       		const void *))compar_addr) !=
		    &pb->pb_sib)
			abort();			/* can't happen */
#if defined(PB_DEBUG)
		/*
		 * If we just removed the last child, put the parent on
		 * the list of leaves.
		 */
		if (!pb->pb_key.pbk_parent->pb_children) {
			TAILQ_INSERT_TAIL(&pb_leaf_nodes,
					  pb->pb_key.pbk_parent,
					  pb_links);
		}
#endif
	}
#ifdef PB_DEBUG
	TAILQ_REMOVE(&pb_leaf_nodes, pb, pb_links);
#endif
	ino = pb->pb_ino;
	if (ino) {
		I_LOCK(ino);
		if (ino->i_ref == 1 && ino->i_zombie) {
			/*
			 * The PB_UNLOCK, below, will kill it before
			 * we have a chance to force the issue. Avoid
			 * the double gone by resetting the pointer
			 * to NULL.
			 */
			ino = NULL;
		}
	}
	PB_SET_ASSOC(pb, NULL);
	PB_UNLOCK(pb);
	mutex_destroy(&pb->pb_mutex);
	free(pb);

	if (ino) {
		if (!(ino->i_ref || ino->i_immune || I_ISASSOC(ino)))
			I_GONE(ino);
		else
			I_UNLOCK(ino);
	}
}

/*
 * Clean up the namespace graph after an unlink.
 */
void
_sysio_pb_disconnect(struct pnode_base *pb)
{

	/*
	 * Remove name from the names cache so that it can't
	 * be found anymore.
	 */
	if (!pb->pb_key.pbk_name.len)
		abort();
	ncache_delete(pb);
	pb->pb_key.pbk_name.len = 0;
}

#ifdef ZERO_SUM_MEMORY
/*
 * Shutdown
 */
void
_sysio_i_shutdown()
{

#ifdef INO_CACHE_STATS
	_sysio_cprintf("inode: probe %llu, hit %llu\n",
		       cstats.cst_iprobes,
		       cstats.cst_ihits);
	_sysio_cprintf("pbnode: probe %llu, hit %llu\n",
		       cstats.cst_pbprobes,
		       cstats.cst_pbhits);
	_sysio_cprintf("pnode: reclaims %llu, examined %llu dismissed %llu\n",
		       cstats.cst_preclaims,
		       cstats.cst_pexamined,
		       cstats.cst_pdismissed);
#endif

	return;
}
#endif

/*
 * Return path tracked by the path ancestor chain.
 *
 * If the buf pointer is NULL, a buffer large enough to hold the path
 * is allocated from the heap.
 */

int
_sysio_p_path(struct pnode *pno, char **bufp, size_t size)
{
	struct pnode *cur;
	size_t	len;
	size_t	n;
	char	*cp;

	cur = pno;

	if (!size && bufp && *bufp)
		return -EINVAL;

	/*
	 * Walk up the tree to the root, summing the component name
	 * lengths and counting the vertices.
	 */
	len = 0;
	n = 0;
	do {
		/*
		 * If this is a covering path-node then the name should be
		 * the *covered* nodes name, not this one unless we are at
		 * the root of the name-space.
		 */
		while (pno == pno->p_mount->mnt_root &&
		       pno != pno->p_mount->mnt_covers) {
			pno = pno->p_mount->mnt_covers;
			assert(pno);
		}

		/*
		 * Add length of this component to running sum and
		 * account for this vertex.
		 */
		assert((len >= pno->p_base->pb_key.pbk_name.len &&
			(size_t )~0 - pno->p_base->pb_key.pbk_name.len > len) ||
		       (size_t )~0 - len > pno->p_base->pb_key.pbk_name.len);
		len += pno->p_base->pb_key.pbk_name.len;
		n++;
		assert(n);
		pno = pno->p_parent;
		assert(pno);
	} while (pno != pno->p_parent ||
		 (pno != pno->p_mount->mnt_covers &&
		  pno->p_mount->mnt_covers));

	if (!*bufp)
		size = len + n + 1;
	if (len >= size || n >= size - len)
		return -ERANGE;
	if (!*bufp) {
		/*
		 * Allocate path buffer from the heap.
		 */
		*bufp = malloc(size * sizeof(char));
		if (!*bufp)
			return -ENOMEM;
	}

	/*
	 * Fill in the path buffer.
	 */
	pno = cur;
	cp = *bufp + len + n;
	*cp = '\0';					/* NUL terminate */
	do {
		/*
		 * If this is a covering path-node then the name should be
		 * the *covered* nodes name, not this one unless we are at
		 * the root of the name-space.
		 */
		while (pno == pno->p_mount->mnt_root &&
		       pno != pno->p_mount->mnt_covers) {
			pno = pno->p_mount->mnt_covers;
		}

		/*
		 * Add component and separator.
		 */
		cp -= pno->p_base->pb_key.pbk_name.len;
		(void )memcpy(cp, pno->p_base->pb_key.pbk_name.name,
			      pno->p_base->pb_key.pbk_name.len);

		*--cp = PATH_SEPARATOR;
		pno = pno->p_parent;
	} while (pno != pno->p_parent ||
		 (pno != pno->p_mount->mnt_covers &&
		  pno->p_mount->mnt_covers));

	return 0;
}

/*
 * Allocate, initialize and establish appropriate links for new path (alias)
 * node.
 */
struct pnode *
_sysio_p_new_alias(struct pnode *parent,
		   struct pnode_base *pb,
		   struct mount *mnt)
{
	struct pnode *pno;

	pno = malloc(sizeof(struct pnode));
	if (!pno)
		return NULL;

	if (!parent)
		parent = pno;
	P_INIT(pno, parent, pb, mnt, NULL);
	pno->p_ref = 1;
	P_GET(pno);
	P_RELE(pno);
	LIST_INSERT_HEAD(&pb->pb_aliases, pno, p_links);
#ifdef P_DEBUG
	_sysio_p_show("P_CREATE_ALIAS", pno);
#endif

	return pno;
}

#ifdef P_DEBUG
/*
 * Thread-safe strerror.
 *
 * Returns string describing the passed error number or NULL on failure.
 */
static char *
x_strerror(int err)
{
	char	*s;
	size_t	buflen;
	void	*p;
	int	myerr;

	s = NULL;
	buflen = 16;
	for (;;) {
		buflen <<= 1;
		p = realloc(s, buflen);
		if (!p)
			free(s);
		s = p;
		if (!s)
			break;
		myerr = 0;
#if defined(_XOPEN_SOURCE) && _XOPEN_SOURCE >= 600
		if (strerror_r(err, s, buflen) != 0)
			myerr = errno;
#elif defined(__GNU__)
		 {
			p = strerror_r(err, s, buflen);
			if (!p)
				myerr = EINVAL;
			else if (p != s) {
				(void )strncpy(s, (char *)p, buflen - 1);
				s[buflen = 1] = '\0';
			}
			if (!myerr && strlen(s) == buflen - 1)
				myerr = ERANGE;
		}
#else
#error I do not know whtat strerror_r returns
#endif
		if (!myerr)
			break;
		if (myerr == ERANGE)
			continue;
		free(s);
		s = NULL;
		errno = myerr;
		break;
	}
	return s;
}

void
_sysio_p_show(const char *pre, struct pnode *pno)
{
	int	err;
	char	*path;
	static char error_message[] = "<ERROR>";

	path = NULL;
	if ((err = _sysio_p_path(pno, &path, 0)) != 0) {
		char	*s;

		s = x_strerror(-err);
		if (!s ||(path = malloc(strlen(s) + 2 + 1)) == NULL) {
			if (s)
				free(s);
			path = error_message;
		} else {
			(void )sprintf(path, "<%s>", s);
			free(s);
		}
	}
	_sysio_diag_printf(pre, "%x:%x %s\n", pno->p_mount, pno, path);
	if (path != error_message)
		free(path);
}
#endif

/*
 * For reclamation of idle path (alias) node.
 */
void
_sysio_p_gone(struct pnode *pno)
{
	assert(!pno->p_ref);
	assert(!pno->p_cover);

	TAILQ_REMOVE(&_sysio_idle_pnodes, pno, p_idle);
	LIST_REMOVE(pno, p_links);

#ifdef P_DEBUG
	_sysio_p_show("P_GONE", pno);
#endif
	P_UNLOCK(pno);
	mutex_destroy(&pno->p_mutex);
	free(pno);
}

/*
 * (Re)Validate passed path node.
 */
int
_sysio_p_validate(struct pnode *pno, struct intent *intnt, const char *path)
{
	struct inode *ino;
	int	err;

	if (!(pno && pno->p_parent))
		return -ENOENT;

	ino = pno->p_base->pb_ino;
	err = PNOP_LOOKUP(pno, &ino, intnt, path);
	if (!err) {
		if (pno->p_base->pb_ino == NULL) {
			/*
			 * Make valid.
			 */
			PB_SET_ASSOC(pno->p_base, ino);
		} else if (pno->p_base->pb_ino != ino) {
			CURVEFS_DPRINTF("_sysio_p_validate in another branch pb_ino=%p ino=%p\n", pno->p_base->pb_ino, ino);
			/*
			 * Path resolves to a different inode, now. The
			 * currently attached inode, then, is stale.
			 *
			 * Don't want to disconnect it. If caller
			 * is handles based, we want to keep returning
			 * stale. Other users should know better than to
			 * keep retrying; It'll never become valid.
			 */
			err = -ESTALE;
			I_PUT(ino);
		}
	} else if (pno->p_base->pb_ino)
		_sysio_pb_disconnect(pno->p_base);
	return err;
}

/*
 * Find (or create!) an alias for the given parent and name. A misnomer,
 * really -- This is a "lookup". Returned path node is referenced.
 */
int
_sysio_p_find_alias(struct pnode *parent,
		    struct qstr *name,
		    struct pnode **pnop)
{
	struct pnode_base *pb;
	struct pnode_base_key key;
	int	err;
	struct pnode *pno;
#ifdef P_DEBUG
	int	isnew;
#endif

	if (!parent)
		return -ENOENT;

	/*
	 * Find the named child.
	 */
	pb = NULL;
	if (name->len) {
		/*
		 * Try the names table.
		 */
		INO_CST_UPDCNT(pbprobes);
		key.pbk_name = *name;
		key.pbk_parent = parent->p_base;
		pb = ncache_lookup(key.pbk_parent, &key);
		if (pb) {
			PB_GET(pb);
			INO_CST_UPDCNT(pbhits);
		}
	}
	if (!pb) {
		/*
		 * None found, create new child.
		 */
		pb = _sysio_pb_new(name, parent->p_base, NULL);
		CURVEFS_DPRINTF("_sysio_p_find_alias pb=%p inode=%p\n", pb, pb->pb_ino);
		if (!pb)
			return -ENOMEM;
	}
	/*
	 * Now find the proper alias. It's the one with the passed
	 * parent.
	 */
	err = 0;
	pno = pb->pb_aliases.lh_first;
	while (pno) {
		if (pno->p_parent == parent) {
			P_GET(pno);
			break;
		}
		pno = pno->p_links.le_next;
	}
#ifdef P_DEBUG
	isnew = 0;
#endif
	if (!pno) {
#ifdef P_DEBUG
		isnew = 1;
#endif
		/*
		 * Hmm. No alias. Create one.
		 */
		pno = _sysio_p_new_alias(parent, pb, parent->p_mount);
		CURVEFS_DPRINTF("_sysio_p_find_alias pno=%p pb=%p inode=%p\n", pno, pno->p_base, pno->p_base->pb_ino);
		if (!pno)
			err = -ENOMEM;
	}
	PB_PUT(pb);
	if (!err) {
#ifdef P_DEBUG
		if (!isnew)
			_sysio_p_show("P_FIND_ALIAS", pno);
#endif
		*pnop = pno;
	}
	return err;
}

static size_t
p_remove_aliases(struct mount *mnt, struct pnode_base *pb)
{
	size_t	count;
	struct pnode *nxtpno, *pno;

	count = 0;
	pno = NULL;
	nxtpno = pb->pb_aliases.lh_first;
	for (;;) {
		if (pno)
			P_UNLOCK(pno);
		pno = nxtpno;
		if (!pno)
			break;
		P_LOCK(pno);
		nxtpno = pno->p_links.le_next;
		if (pno->p_mount != mnt) {
			/*
			 * Not the alias we were looking for.
			 */
			continue;
		}
		if (pno->p_ref) {
			/*
			 * Can't prune; It's active.
			 */
			count++;
			continue;
		}
		if (pno->p_cover) {
			/*
			 * Mounted on.
			 */
			count++;
			continue;
		}
#ifdef AUTOMOUNT_FILE_NAME
		if (pno->p_mount->mnt_root == pno) {
			count++;
			continue;
		}
#endif
		_sysio_p_gone(pno);
		pno = NULL;
	}

	return count;
}

static size_t
pb_prune(struct mount *mnt, struct pnode_base *pb)
{
	size_t	count;
	struct pnode_base *stop, *parent;
	struct tree_node *tn_next;

	count = 0;
	stop = parent = pb->pb_key.pbk_parent;
	do {
		/*
		 * Just descended into this node.
		 */
		while (pb->pb_children) {
			_sysio_splay(NULL,
				     &pb->pb_children,
				     (int  (*)(const void *,
					       const void *))compar_addr);
			parent = pb;
			pb =
			    TREE_ENTRY(parent->pb_children,
				       pnode_base,
				       pb_sib);
		}
		/*
		 * No children.
		 */
		for (;;) {
			count += p_remove_aliases(mnt, pb);
			tn_next = pb->pb_sib.tn_right;
			if (tn_next) {
				_sysio_splay(NULL,
					     &pb->pb_sib.tn_right,
					     (int  (*)(const void *,
						       const void *))compar_addr);
				tn_next = pb->pb_sib.tn_right;
			}
			if (!(pb->pb_children || pb->pb_aliases.lh_first)) {
				PB_LOCK(pb);
				_sysio_pb_gone(pb);
			}
			if (!tn_next) {
				/*
				 * Ascend.
				 */
				pb = parent;
				if (pb == stop)
					break;
				parent = pb->pb_key.pbk_parent;
				continue;
			}
			pb = TREE_ENTRY(tn_next, pnode_base, pb_sib);
			_sysio_splay(pb,
				     &parent->pb_children,
				     (int  (*)(const void *,
					       const void *))compar_addr);
			break;
		}
	} while (pb != stop);

	return count;
}

/*
 * Prune idle nodes from the passed sub-tree, including the root.
 *
 * Returns the number of aliases on the same mount that could not be pruned.
 * i.e. a zero return means the entire sub-tree is gone.
 */
size_t
_sysio_p_prune(struct pnode *pno)
{
	return pb_prune(pno->p_mount, pno->p_base);
}

/*
 * Simultaneous get of two path nodes.
 */
void
_sysio_p_get2(struct pnode *pno1, struct pnode *pno2)
{
	struct inode *ino1, *ino2;
	char	*cp1, *cp2;
	size_t	count1, count2;
	int	order;

	if (pno1 < pno2) {
		P_LOCK(pno1);
		P_LOCK(pno2);
	} else {
		P_LOCK(pno2);
		P_LOCK(pno1);
	}
	if (pno1->p_base < pno2->p_base) {
		PB_LOCK(pno1->p_base);
		PB_LOCK(pno2->p_base);
	} else {
		PB_LOCK(pno2->p_base);
		PB_LOCK(pno1->p_base);
	}
	/*
	 * File identifiers are never allowed to change and we've prevented
	 * the inode pointer from changing in the path-base records. So...
	 * It's safe to examine the file identifiers without a lock now.
	 */
	ino1 = pno1->p_base->pb_ino;
	ino2 = pno2->p_base->pb_ino;
	cp1 = NULL;
	count1 = 0;
	if (ino1) {
		cp1 = ino1->i_fid->fid_data;
		count1 = ino1->i_fid->fid_len;
	}
	cp2 = NULL;
	count2 = 0;
	if (ino2) {
		cp2 = ino2->i_fid->fid_data;
		count2 = ino2->i_fid->fid_len;
	}
	order = 0;
	while (count1 && count2 && *cp1 == *cp2) {
		cp1++; cp2++;
		count1--; count2--;
	}
	if (count1 && count2)
		order = *cp2 - *cp1;
	else if (count1 > count2)
		order = 1;
	else if (count1 > count2)
		order = -1;
	if (order <= 0) {
		P_GET(pno1);
		P_GET(pno2);
	} else {
		P_GET(pno2);
		P_GET(pno1);
	}
	/*
	 * We did get operations, finally, above. Have to release our
	 * redundant locks. The order in which we unlock is unimportant.
	 */
	PB_UNLOCK(pno1->p_base);
	PB_UNLOCK(pno2->p_base);
	P_UNLOCK(pno1);
	P_UNLOCK(pno2);
}

/*
 * Return path tracked by the base path node ancestor chain.
 *
 * Remember, base path nodes track the path relative to the file system and
 * path (alias) nodes track path relative to our name space -- They cross
 * mount points.
 */
int
_sysio_pb_pathof(struct pnode_base *pb, const char separator, char **pathp)
{
	size_t	len, n;
	struct pnode_base *tmp;
	char	*buf;
	char	*cp;

	/*
	 * First pass: Traverse to the root of the sub-tree, remembering
	 * lengths.
	 */
	len = 0;
	tmp = pb;
	do {
		n = tmp->pb_key.pbk_name.len;
		len += tmp->pb_key.pbk_name.len;
		if (n)
			len++;
		tmp = tmp->pb_key.pbk_parent;
	} while (tmp);
	if (tmp && !tmp->pb_key.pbk_name.len)
		return -ENOENT;
	if (!len)
		len++;
	/*
	 * Alloc space.
	 */
	buf = malloc(len + 1);
	if (!buf)
		return -ENOMEM;
	/*
	 * Fill in the path buffer -- Backwards, since we're starting
	 * from the end.
	 */
	cp = buf;
	*cp = separator;
	cp += len;
	*cp = '\0';					/* NUL term */
	tmp = pb;
	do {
		cp -= tmp->pb_key.pbk_name.len;
		n = tmp->pb_key.pbk_name.len;
		if (n) {
			(void )strncpy(cp, tmp->pb_key.pbk_name.name, n);
			*--cp = separator;
		}
		tmp = tmp->pb_key.pbk_parent;
	} while (tmp);

	*pathp = buf;
	return 0;
}

/*
 * Return path tracked by the base path node ancestor chain.
 *
 * NB: Deprecated.
 */
char *
_sysio_pb_path(struct pnode_base *pb, const char separator)
{
	int	err;
	char	*path;

	err = _sysio_pb_pathof(pb, separator, &path);
	if (err) {
		if (err == -ENOMEM)
			return NULL;
		path = malloc(1);
		if (path == NULL)
			return NULL;
		*path = '\0';
	}
	return path;
}

/*
 * Common set attributes routine.
 */
int
_sysio_p_setattr(struct pnode *pno,
		 unsigned mask,
		 struct intnl_stat *stbuf)
{

	CURVEFS_DPRINTF("comeing into the _sysio_p_setattr readonly=%d\n", IS_RDONLY(pno));

	if (IS_RDONLY(pno))
		return -EROFS;

	/*
	 * Determining permission to change the attributes is
	 * difficult, at best. Just try it.
	 */
	return PNOP_SETATTR(pno, mask, stbuf);
}

/*
 * Do nothing.
 */
void
_sysio_do_noop()
{

	return;
}

/*
 * Abort.
 */
void
_sysio_do_illop()
{

	abort();
}

/*
 * Return -EBADF
 */
int
_sysio_do_ebadf()
{

	return -EBADF;
}

/*
 * Return -EINVAL
 */
int
_sysio_do_einval()
{

	return -EINVAL;
}

/*
 * Return -ENOENT
 */
int
_sysio_do_enoent()
{

	return -ENOENT;
}

/*
 * Return -ESPIPE
 */
int
_sysio_do_espipe()
{

	return -ESPIPE;
}

/*
 * Return -EISDIR
 */
int
_sysio_do_eisdir()
{

	return -EISDIR;
}

/*
 * Return -ENOSYS
 */
int
_sysio_do_enosys()
{

	return -ENOSYS;
}


/*
 * Return -ENODEV
 */
int
_sysio_do_enodev()
{

	return -ENODEV;
}
