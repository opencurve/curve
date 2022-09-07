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

#include <stdlib.h>
#include <errno.h>

#include "tree.h"

/*
 * Splay to node with given key.
 *
 * Returns the value from the last compare; The one that brought, or left,
 * the root node at return.
 */
int
_sysio_splay(const void *key,
	     struct tree_node **rootp,
	     int (*compar)(const void *, const void *))
{
	struct tree_node spare, *l, *r, *tmp, *root;
	int	i;

	spare.tn_left = spare.tn_right = NULL;
	l = r = &spare;
	root = *rootp;
	for (;;) {
		i = (*compar)(key, root->tn_key);
		if (i < 0) {
			if (root->tn_left &&
			    (*compar)(key, root->tn_left->tn_key) < 0) {
				tmp = root->tn_left;
				root->tn_left = tmp->tn_right;
				tmp->tn_right = root;
				root = tmp;
			}
			if (!root->tn_left)
				break;
			r->tn_left = root;
			r = root;
			root = root->tn_left;
		} else if (i > 0) {
			if (root->tn_right &&
			    (*compar)(key, root->tn_right->tn_key) > 0) {
				tmp = root->tn_right;
				root->tn_right = tmp->tn_left;
				tmp->tn_left = root;
				root = tmp;
			}
			if (!root->tn_right)
				break;
			l->tn_right = root;
			l = root;
			root = root->tn_right;
		} else
			break;
	}

	/*
	 * Re-link.
	 */
	l->tn_right = root->tn_left;
	r->tn_left = root->tn_right;
	root->tn_left = spare.tn_right;
	root->tn_right = spare.tn_left;
	*rootp = root;

	return i;
}

/*
 * Return tree node with matching key or replace with passed node.
 */
struct tree_node *
_sysio_tree_search(struct tree_node *tn,
		   struct tree_node **rootp,
		   int (*compar)(const void *, const void *))
{
	int	i;

	if (!*rootp) {
		tn->tn_left = tn->tn_right = NULL;
	} else {
		i = _sysio_splay(tn->tn_key, rootp, compar);
		if (i < 0) {
			tn->tn_left = (*rootp)->tn_left;
			tn->tn_right = *rootp;
			(*rootp)->tn_left = NULL;
		} else if (i > 0) {
			tn->tn_right = (*rootp)->tn_right;
			tn->tn_left = *rootp;
			(*rootp)->tn_right = NULL;
		} else
			return *rootp;
	}
	*rootp = tn;
	return *rootp;
}

/*
 * Find node with given key. Return NULL if not found.
 */
struct tree_node *
_sysio_tree_find(const void *key,
		 struct tree_node **rootp,
		 int (*compar)(const void *, const void *))
{

	if (!*rootp)
		return NULL;
	return _sysio_splay(key, rootp, compar) == 0 ? *rootp : NULL;
}

/*
 * Remove node with given key. Returns the node removed or NULL if not found.
 */
struct tree_node *
_sysio_tree_delete(const void *key,
		   struct tree_node **rootp,
		   int (*compar)(const void *, const void *))
{
	struct tree_node *tn;

	tn = _sysio_tree_find(key, rootp, compar);
	if (!tn)
		return NULL;
	/*
	 * Invariant; The desired node is at the root.
	 */
	if (!(*rootp)->tn_left)
		*rootp = (*rootp)->tn_right;
	else {
		_sysio_splay((*rootp)->tn_key, &(*rootp)->tn_left, compar);
		(*rootp)->tn_left->tn_right = (*rootp)->tn_right;
		*rootp = (*rootp)->tn_left;
	}
	return tn;
}

/*
 * Enumerate the passed tree calling the pre, in, and post routines at each
 * node if they aren't NULL.
 */
int
_sysio_tree_enumerate(const struct tree_node *tn,
		      int (*pre)(const struct tree_node *, void *),
		      int (*in)(const struct tree_node *, void *),
		      int (*post)(const struct tree_node *, void *),
		      void *data)
{
	struct stk_entry {
		enum { PRE, IN, POST } state;
		union {
			const struct tree_node *tn;
			struct stk_entry *next;
		} u;
	} *_free, *_list, *_tmp, *_seg_end, *top;
	int	result;

#define _NPERSEG	20

#define _GET_FREE \
	(_free \
	   ? ((_tmp = _free), \
	      (_free = _tmp->u.next), \
	      (_tmp->u.next = _list), \
	      (_list = _tmp)) \
	   : ((_tmp = malloc(_NPERSEG * sizeof(struct stk_entry))) \
		? ((_tmp->u.next = _list), \
		   (_list = _tmp)) \
		: NULL))

#define _PUSH \
	((++top < _seg_end) \
	   ? 0 \
	   : (_GET_FREE \
		? ((_seg_end = _list + _NPERSEG), \
		   (top = _list + 1), \
		   0) \
		: -ENOMEM))
#define _POP \
	((--top > _list) \
	   ? 0 \
	   : ((_tmp = _list) \
	        ? ((_list = _list->u.next), \
		   (top = (_seg_end = _list + _NPERSEG) - 1), \
		   (_tmp->u.next = _free), \
		   (_free = _tmp->u.next), \
		   0) \
		: 1))

	top = _free = _list = _seg_end = NULL;
	result = ((++top < _seg_end)
	   ? 0
	   : (_GET_FREE
		? ((_seg_end = _list + _NPERSEG),
		   (top = _list + 1),
		   0)
		: -ENOMEM));
	if (!result) {
		top->state = PRE;
		top->u.tn = tn;
	}
	while (!result && _POP) {
		tn = top->u.tn;
		switch (top->state) {
		case PRE:
			if (pre && (result = (*pre)(tn, data)))
				break;
			top->state = IN;
			if (tn->tn_left) {
				if ((result = _PUSH))
					break;
				top->state = PRE;
				top->u.tn = tn->tn_left;
				break;
			}
		case IN:
			if (in && (result = (*in)(tn, data)))
				break;
			top->state = POST;
			if (tn->tn_right) {
				if ((result = _PUSH))
					break;
				top->state = PRE;
				top->u.tn = tn->tn_right;
				break;
			}
		case POST:
			if (post && (result = (*post)(tn, data)))
				break;
			break;
		default:
			result = -EINVAL;
		}
	}

#undef _POP
#undef _PUSH
#undef _GET_FREE
#undef _NPERSEG

	while (_list) {
		_tmp = _list;
		_list = _list->u.next;
		free(_tmp);
	}
	while (_free) {
		_tmp = _free;
		_free = _free->u.next;
		free(_tmp);
	}
	return result;
}
