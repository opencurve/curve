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

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"

/*
 * Asynch IO support for the API.
 */

/*
 * Poll status of asynch IO request.
 */
int
SYSIO_INTERFACE_NAME(iodone)(void *ioid)
{
	struct ioctx *ioctx;
	int rc;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(iodone, "%p", ioid);
	ioctx = _sysio_ioctx_find(ioid);
	if (!ioctx)
		SYSIO_INTERFACE_RETURN(-1, -EINVAL, iodone, "%d", 0);

	rc = _sysio_ioctx_done(ioctx);
	SYSIO_INTERFACE_RETURN(rc < 0 ? -1 : rc, rc < 0 ? rc : 0,
			       iodone, "%d", 0);
}

/*
 * Wait for completion of and return results from identified asynch IO
 * request.
 *
 * The identifier is no longer valid after return.
 */
ssize_t
SYSIO_INTERFACE_NAME(iowait)(void *ioid)
{
	struct ioctx *ioctx;
	ssize_t	cc;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(iowait, "%p", ioid);
	ioctx = _sysio_ioctx_find(ioid);
	if (!ioctx)
		SYSIO_INTERFACE_RETURN(-1, -EINVAL, iowait, "%zd", 0);

	cc = _sysio_ioctx_wait(ioctx);
	SYSIO_INTERFACE_RETURN(cc < 0 ? -1 : cc, cc < 0 ? (int )cc : 0,
			       iowait, "%zd", 0);
}
