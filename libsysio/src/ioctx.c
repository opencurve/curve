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
#include <sched.h>
#include <assert.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"
#include "inode.h"

#if defined(REDSTORM)
#include <catamount/do_iostats.h>
#endif


/*
 * Asynchronous IO context support.
 */

/*
 * List of all outstanding (in-flight) asynch IO requests tracked
 * by the system.
 */
static LIST_HEAD( ,ioctx) aioq;

/*
 * Free callback entry.
 */
#define cb_free(cb)		free(cb)

/*
 * Initialization. Must be called before using any other routine in this
 * module.
 */
int
_sysio_ioctx_init()
{

	LIST_INIT(&aioq);
	return 0;
}

/*
 * Enter an IO context onto the async IO events queue.
 */
void
_sysio_ioctx_enter(struct ioctx *ioctx)
{

	LIST_INSERT_HEAD(&aioq, ioctx, ioctx_link);
}

/*
 * Allocate and initialize a new IO context.
 */
struct ioctx *
_sysio_ioctx_new(struct pnode *pno,
		 const struct iovec *iov,
		 size_t iovlen,
		 const struct intnl_xtvec *xtv,
		 size_t xtvlen,
		 void *args)
{
	struct ioctx *ioctx;

	assert(pno->p_base->pb_ino);

	ioctx = malloc(sizeof(struct ioctx));
	if (!ioctx)
		return NULL;

	P_REF(pno);

	IOCTX_INIT(ioctx,
		   0,
		   pno,
		   iov, iovlen,
		   xtv, xtvlen,
		   args);

	/*
	 * Link request onto the outstanding requests queue.
	 */
	_sysio_ioctx_enter(ioctx);

	return ioctx;
}

/*
 * Add an IO completion call-back to the end of the context call-back queue.
 * These are called in iowait() as the last thing, right before the context
 * is destroyed.
 *
 * They are called in order. Beware.
 */
int
_sysio_ioctx_cb(struct ioctx *ioctx,
		void (*f)(struct ioctx *, void *),
		void *data)
{
	struct ioctx_callback *entry;

	entry = malloc(sizeof(struct ioctx_callback));
	if (!entry)
		return -ENOMEM;

	entry->iocb_f = f;
	entry->iocb_data = data;

	TAILQ_INSERT_TAIL(&ioctx->ioctx_cbq, entry, iocb_next);

	return 0;
}

/*
 * Find an IO context given it's identifier.
 *
 * NB: This is dog-slow. If there are alot of these, we will need to change
 * this implementation.
 */
struct ioctx *
_sysio_ioctx_find(void *id)
{
	struct ioctx *ioctx;

	for (ioctx = aioq.lh_first; ioctx; ioctx = ioctx->ioctx_link.le_next)
		if (ioctx == id)
			return ioctx;

	return NULL;
}

/*
 * Check if asynchronous IO operation is complete.
 */
int
_sysio_ioctx_done(struct ioctx *ioctx)
{

	if (ioctx->ioctx_done)
		return 1;
	P_GET(ioctx->ioctx_pno);
	if (PNOP_IODONE(ioctx))
		ioctx->ioctx_done = 1;
	P_PUT(ioctx->ioctx_pno);
	return (int )ioctx->ioctx_done;
}

/*
 * Wait for asynchronous IO operation to complete, return status
 * and dispose of the context.
 *
 * Note:
 * The context is no longer valid after return.
 */
ssize_t
_sysio_ioctx_wait(struct ioctx *ioctx)
{
	ssize_t	cc;

	/*
	 * Wait for async operation to complete.
	 */
	while (!_sysio_ioctx_done(ioctx)) {
#ifdef POSIX_PRIORITY_SCHEDULING
		(void )sched_yield();
#endif
	}

	/*
	 * Get status.
	 */
	cc = ioctx->ioctx_cc;
	if (cc < 0)
		cc = -ioctx->ioctx_errno;

	/*
	 * Dispose.
	 */
	_sysio_ioctx_complete(ioctx);

	return cc;
}

/*
 * Free callback entry.
 */
void
_sysio_ioctx_cb_free(struct ioctx_callback *cb)
{

	cb_free(cb);
}

/*
 * Complete an asynchronous IO request.
 */
void
_sysio_ioctx_complete(struct ioctx *ioctx)
{
	struct ioctx_callback *entry;


	/* update IO stats */
	_SYSIO_UPDACCT(ioctx->ioctx_write, ioctx->ioctx_cc);

	/*
	 * Run the call-back queue.
	 */
	while ((entry = ioctx->ioctx_cbq.tqh_first)) {
		TAILQ_REMOVE(&ioctx->ioctx_cbq, entry, iocb_next);
		(*entry->iocb_f)(ioctx, entry->iocb_data);
		cb_free(entry);
	}

	/*
	 * Unlink from the file record's outstanding request queue.
	 */
	LIST_REMOVE(ioctx, ioctx_link);

	if (ioctx->ioctx_fast)
		return;
	P_RELE(ioctx->ioctx_pno);
	free(ioctx);
}
