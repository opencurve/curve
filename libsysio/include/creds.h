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

#include <unistd.h>

#ifndef _CREDS_H_
#define _CREDS_H_

/*
 * Superuser's UID.
 */
#define _SYSIO_ROOT_UID	0

/*
 * Data structure for user credentials
 */

struct creds {
	uid_t creds_uid; 
	gid_t *creds_gids; 
	int creds_ngids;
};


/*
 * Is caller the superuser?
 */
#ifdef _SYSIO_ROOT_UID
#define _sysio_is_root(_crp) \
	((_crp)->creds_uid == _SYSIO_ROOT_UID)
#else
	(0)
#endif

struct pnode;

extern int (*_sysio_get_credentials)(struct creds **, int);
extern void (*_sysio_put_credentials)(struct creds *);
extern int (*_sysio_check_permission)(struct pnode *, struct creds *, int);
#endif
