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
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"

/*
 * Extent-vector IO support.
 */

/*
 * Arguments to IO vector enumerator callback when used by _sysio_doio().
 */
struct doio_helper_args {
	ssize_t (*f)(void *, size_t, _SYSIO_OFF_T, void *);	/* base func */
	void	*arg;						/* caller arg */
};

/*
 * General help validating strided-IO vectors.
 *
 * A driver may call this to make sure underflow/overflow of an off_t can't
 * occur and overflow of a ssize_t can't occur when writing. The sum
 * of the reconciled transfer length is returned or some appropriate
 * error depending on underflow/overflow.
 *
 * The following algorithm assumes:
 *
 * a) sizeof(size_t) >= sizeof(ssize_t)
 * b) 2's complement arithmetic
 * c) The compiler won't optimize away code because it's developers
 *	believed that something with an undefined result in `C' can't happen.
 */
ssize_t
_sysio_validx(const struct intnl_xtvec *xtv, size_t xtvlen,
	      const struct iovec *iov, size_t iovlen,
	      _SYSIO_OFF_T limit)
{
	ssize_t	acc, cc;
	struct iovec iovec = { NULL, 0 };
	struct intnl_xtvec xtvec = { 0, 0 };
	_SYSIO_OFF_T off;

	if (!(xtvlen && iovlen))
		return -EINVAL;

	acc = 0;
	do {
		while (!xtvec.xtv_len) {
			if (!xtvlen--)
				break;
			if (!xtv->xtv_len) {
				xtv++;
				continue;
			}
			xtvec = *xtv++;
			if (xtvec.xtv_off < 0)
				return -EINVAL;
		}
		if (!xtvec.xtv_len)
			break;
		do {
			while (!iovec.iov_len) {
				if (!iovlen--)
					break;
				if (!iov->iov_len) {
					iov++;
					continue;
				}
				iovec = *iov++;
			}
			if (!iovec.iov_len)
				break;
			cc = iovec.iov_len;
			if (cc < 0)
				return -EINVAL;
			if ((size_t )cc > xtvec.xtv_len)
				cc = xtvec.xtv_len;
			xtvec.xtv_len -= cc;
			iovec.iov_len -= cc;
			off = xtvec.xtv_off + cc;
			if (xtvec.xtv_off && off <= xtvec.xtv_off)
				return off < 0 ? -EINVAL : -EOVERFLOW;
			if (off > limit)
				return -EFBIG;
			xtvec.xtv_off = off;
			cc += acc;
			if (acc && (cc <= acc))
				return -EINVAL;
			acc = cc;
		} while (xtvec.xtv_len && iovlen);
	} while ((xtvlen || xtvec.xtv_len) && iovlen);
	return acc;
}

/*
 */
ssize_t
_sysio_enumerate_extents(const struct intnl_xtvec *xtv, size_t xtvlen,
			 const struct iovec *iov, size_t iovlen,
			 ssize_t (*f)(const struct iovec *, int,
				      _SYSIO_OFF_T,
				      ssize_t,
				      void *),
			 void *arg)
{
	ssize_t	acc, tmp, cc;
	struct iovec iovec;
	struct intnl_xtvec xtvec;
	const struct iovec *start;
	_SYSIO_OFF_T off;
	size_t	n;
	size_t	remain;
	
	acc = 0;
	iovec.iov_len = 0;
	while (xtvlen) {
		/*
		 * Coalesce contiguous extent vector entries.
		 */
		off = xtvec.xtv_off = xtv->xtv_off;
		off += xtvec.xtv_len = xtv->xtv_len;
		while (++xtv, --xtvlen) {
			if (off != xtv->xtv_off) {
				/*
				 * Not contiguous.
				 */
				break;
			}
			if (!xtv->xtv_len) {
				/*
				 * Zero length.
				 */
				continue;
			}
			off += xtv->xtv_len;
			xtvec.xtv_len += xtv->xtv_len;
		}
		while (xtvec.xtv_len) {
			if (iovec.iov_len) {
				tmp = iovec.iov_len; 
				if (iovec.iov_len > xtvec.xtv_len)
					iovec.iov_len = xtvec.xtv_len;
				cc =
				    (*f)(&iovec, 1,
					 xtvec.xtv_off,
					 xtvec.xtv_len,
					 arg);
				if (cc <= 0) {
					if (acc)
						return acc;
					return cc;
				}
				iovec.iov_base = (char *)iovec.iov_base + cc;
				iovec.iov_len = tmp - cc; 
				tmp = cc + acc;
				if (acc && tmp <= acc)
					abort();		/* paranoia */
				acc = tmp;
			} else if (iovlen) {
				start = iov;
				n = xtvec.xtv_len;
				do {
					if (iov->iov_len > n) {
						/*
						 * That'll do.
						 */
						break;
					}
					n -= iov->iov_len;
					iov++;
				} while (--iovlen);
				if (iov == start) {
					iovec = *iov++;
					iovlen--;
					continue;
				}
				remain = xtvec.xtv_len - n;
				cc =
				    (*f)(start, iov - start,
					 xtvec.xtv_off,
					 remain,
					 arg);
				if (cc <= 0) {
					if (acc)
						return acc;
					return cc;
				}
								
				tmp = cc + acc;
				if (acc && tmp <= acc)
					abort();		/* paranoia */
				acc = tmp;

				remain -= cc;
				if (remain)
					return acc;		/* short */
			} else
				return acc;			/* short out */
			xtvec.xtv_off += cc;
			xtvec.xtv_len -= cc;
		}
	}
	return acc;
}

ssize_t
_sysio_enumerate_iovec(const struct iovec *iov, size_t count,
		       _SYSIO_OFF_T off,
		       ssize_t limit,
		       ssize_t (*f)(void *, size_t, _SYSIO_OFF_T, void *),
		       void *arg)
{
	ssize_t	acc, cc;
	size_t	n;
	unsigned indx;
	size_t	remain;

	if (!count)
		return -EINVAL;
	assert(limit >= 0);
	acc = 0;
	n = limit;
	for (indx = 0; n && indx < count; indx++) {
		if (iov[indx].iov_len < n) {
			cc = (ssize_t )iov[indx].iov_len;
			if (cc < 0)
				return -EINVAL;
		} else
			cc = (ssize_t )n;
		if (!cc)
			continue;
		n -= cc;
		cc += acc;
		if (acc && cc <= acc)
			return -EINVAL;
		acc = cc;
	}
	if (!acc)
		return 0;
	acc = 0;
	do {
		if (!iov->iov_len) {
			iov++;
			continue;
		}
		n =
		    iov->iov_len < (size_t )limit
		      ? iov->iov_len
		      : (size_t )limit;
		cc = (*f)(iov->iov_base, n, off, arg);
		if (cc <= 0) {
			if (acc)
				return acc;
			return cc;
		}
		off += cc;
		limit -= cc;
		remain = iov->iov_len - cc;
		cc += acc;
		if (acc && cc <= acc)
			abort();			/* bad driver! */
		acc = cc;
		if (remain || !limit)
			break;				/* short/limited read */
		iov++;
	} while (--count);
	return acc;
}

static ssize_t
_sysio_doio_helper(const struct iovec *iov, int count,
		   _SYSIO_OFF_T off,
		   ssize_t limit,
		   struct doio_helper_args *args)
{

	return _sysio_enumerate_iovec(iov, count,
				      off, limit,
				      args->f,
				      args->arg);
}

/*
 * A meta-driver for the whole strided-io process. Appropriate when
 * the driver can't handle anything but simple p{read,write}-like
 * interface.
 */
ssize_t
_sysio_doio(const struct intnl_xtvec *xtv, size_t xtvlen,
	    const struct iovec *iov, size_t iovlen,
	    ssize_t (*f)(void *, size_t, _SYSIO_OFF_T, void *),
	    void *arg)
{
	struct doio_helper_args arguments;

	arguments.f = f;
	arguments.arg = arg;
	return _sysio_enumerate_extents(xtv, xtvlen,
					iov, iovlen,
					(ssize_t (*)(const struct iovec *, int,
						     _SYSIO_OFF_T,
						     ssize_t,
						     void *))_sysio_doio_helper,
					&arguments);
}
