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

#ifndef _TREE_H
#define _TREE_H

struct tree_node {
	void	*tn_key;
	struct tree_node *tn_left;
	struct tree_node *tn_right;
};

/*
 * Return containing record given ptr to tree node.
 */
#define TREE_ENTRY(tn, type, mbr) \
	CONTAINER(type, mbr, tn)

extern struct tree_node *_sysio_tree_search(struct tree_node *tn,
					    struct tree_node **rootp,
					    int (*compar)(const void *,
							  const void *));
extern struct tree_node *_sysio_tree_find(const void *key,
					  struct tree_node **rootp,
					  int (*compar)(const void *,
					  		const void *));
extern struct tree_node *_sysio_tree_delete(const void *key,
					    struct tree_node **rootp,
					    int (*compar)(const void *,
					    const void *));
extern int _sysio_tree_enumerate(const struct tree_node *tn,
				 int (*pre)(const struct tree_node *, void *),
				 int (*in)(const struct tree_node *, void *),
				 int (*post)(const struct tree_node *, void *),
				 void *);
extern int _sysio_splay(const void *key,
			struct tree_node **rootp,
			int (*compar)(const void *, const void *));
#endif /* !defined(_TREE_H) */
