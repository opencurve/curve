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

/*
 * Extended application programmers interface for IO as found on Cray RedStorm
 * and the other current SUNMos/Puma/Cougar/Catamount systems.
 */

#ifndef _XTIO_H_
#define _XTIO_H_

/*
 * When compiled for use with libsysio, this allows one to move all the
 * externals to a distinct namespace. When not, we want it to do nothing.
 *
 * NB: The choice of macro name here is dangerous. It's in the global
 * namespace! We should fix that one of these days.
 */
#if !defined(SYSIO_INTERFACE_NAME)
#define SYSIO_INTERFACE_NAME(_n)	_n
#endif

#ifndef _IOID_T_DEFINED
#define _IOID_T_DEFINED
typedef void *ioid_t;

#define IOID_FAIL			0
#endif

/*
 * Structure for strided I/O.
 */
struct xtvec {
#ifndef __USE_FILE_OFFSET64
	__off_t	xtv_off;			/* Stride/Extent offset. */
#else
	__off64_t xtv_off;			/* Stride/Extent offset. */
#endif
	size_t	xtv_len;			/* Stride/Extent length. */
};

#ifdef __USE_LARGEFILE64
struct xtvec64 {
	__off64_t xtv_off;			/* Stride/Extent offset. */
	size_t	xtv_len;			/* Stride/Extent length. */
};
#endif

struct iovec;

/*
 * Get status of previously posted async file IO operation.
 */
extern int SYSIO_INTERFACE_NAME(iodone)(ioid_t ioid);

/*
 * Wait for completion of a previously posted asynch file IO request.
 */
extern ssize_t SYSIO_INTERFACE_NAME(iowait)(ioid_t ioid);

/*
 * Post asynch read into buffers mapped by an iovec from file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipreadv)(int fd,
					    const struct iovec *iov,
					    size_t count,
					    off_t offset);

#ifdef _LARGEFILE64_SOURCE
/*
 * Post asynch read into buffers mapped by an iovec from file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipread64v)(int fd,
					      const struct iovec *iov, 
					      size_t count,
					      off64_t offset);
#endif

/*
 * Post asynch read into buffer from file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipread)(int fd,
					   void *buf,
					   size_t count, 
					   off_t offset);

#ifdef _LARGEFILE64_SOURCE
/*
 * Post asynch read into buffer from file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipread64)(int fd,
					     void *buf,
					     size_t count, 
					     off64_t offset);
#endif

/*
 * Read into buffers mapped by an iovec from file at given offset.
 */
extern ssize_t SYSIO_INTERFACE_NAME(preadv)(int fd,
					    const struct iovec *iov,
					    int count,
					    off_t offset);

#ifdef _LARGEFILE64_SOURCE
/*
 * Read into buffers mapped by an iovec from file at given offset.
 */
extern ssize_t SYSIO_INTERFACE_NAME(pread64v)(int fd,
					      const struct iovec *iov, 
					      size_t count,
					      off64_t offset);
#endif

/*
 * Post asynch read into buffers mapped by an iovec.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ireadv)(int fd,
					   const struct iovec *iov, 
					   int count);

/*
 * Read into buffer.
 */
extern ioid_t SYSIO_INTERFACE_NAME(iread)(int fd,
					  void *buf,
					  size_t count);

/*
 * Post async read into buffers mapped by iovec from regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ireadx)(int fd,
					   const struct iovec *iov, 
					   size_t iov_count,
					   const struct xtvec *xtv,
					   size_t xtv_count);

#ifdef __USE_LARGEFILE64
/*
 * Post async read into buffers mapped by iovec from regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ioid_t SYSIO_INTERFACE_NAME(iread64x)(int fd,
					     const struct iovec *iov, 
					     size_t iov_count,
					     const struct xtvec64 *xtv,
					     size_t xtv_count);
#endif

/*
 * Read into buffers mapped by iovec from regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ssize_t SYSIO_INTERFACE_NAME(readx)(int fd,
					   const struct iovec *iov,
					   size_t iov_count,
					   const struct xtvec *xtv,
					   size_t xtv_count);

#ifdef __USE_LARGEFILE64
/*
 * Read into buffers mapped by iovec from regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ssize_t SYSIO_INTERFACE_NAME(read64x)(int fd,
					     const struct iovec *iov,
					     size_t iov_count,
					     const struct xtvec64 *xtv,
					     size_t xtv_count);
#endif

/*
 * Post asynch write from buffers mapped by an iovec to file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipwritev)(int fd,
					     const struct iovec *iov,
					     size_t count,
					     off_t offset);
#ifdef _LARGEFILE64_SOURCE
/*
 * Post asynch write from buffers mapped by an iovec to file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipwrite64v)(int fd,
					       const struct iovec *iov,
					       size_t count,
					       off64_t offset);
#endif

/*
 * Post asynch write from buffer to file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipwrite)(int fd,
					    const void *buf,
					    size_t count,
					    off_t offset);

#ifdef _LARGEFILE64_SOURCE
/*
 * Post asynch write from buffer to file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(ipwrite64)(int fd,
					      const void *buf,
					      size_t count,
					      off64_t offset);
#endif

/*
 * Write from buffers mapped by an iovec to file at given offset.
 */
extern ssize_t SYSIO_INTERFACE_NAME(pwritev)(int fd,
					     const struct iovec *iov,
					     int count,
					     off_t offset);

#ifdef _LARGEFILE64_SOURCE
/*
 * Write from buffers mapped by an iovec to file at given offset.
 */
extern ssize_t SYSIO_INTERFACE_NAME(pwrite64v)(int fd,
					       const struct iovec *iov,
					       size_t count,
					       off64_t offset);
#endif

/*
 * Post asynch write from buffer to file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(iwritev)(int fd,
					    const struct iovec *iov,
					    int count);

/*
 * Write from buffer to file at given offset.
 */
extern ioid_t SYSIO_INTERFACE_NAME(iwrite)(int fd,
					   const void *buf,
					   size_t count);

/*
 * Post async write from buffers mapped by iovec to regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ioid_t SYSIO_INTERFACE_NAME(iwritex)(int fd,
					    const struct iovec *iov,
					    size_t iov_count,
					    const struct xtvec *xtv,
					    size_t xtv_count);

#ifdef __USE_LARGEFILE64
/*
 * Post async write from buffers mapped by iovec to regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ioid_t SYSIO_INTERFACE_NAME(iwrite64x)(int fd,
					      const struct iovec *iov,
					      size_t iov_count,
					      const struct xtvec64 *xtv,
					      size_t xtv_count);
#endif

/*
 * Write from buffers mapped by iovec to regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ssize_t SYSIO_INTERFACE_NAME(writex)(int fd,
					    const struct iovec *iov,
					    size_t iov_count,
					    const struct xtvec *xtv,
					    size_t xtv_count);

#ifdef __USE_LARGEFILE64
/*
 * Write from buffers mapped by iovec to regions mapped
 * by xtvec.
 *
 * NB: An adaptation of "listio" from Argonne's PVFS.
 */
extern ssize_t SYSIO_INTERFACE_NAME(write64x)(int fd,
					      const struct iovec *iov, 
					      size_t iov_count,
					      const struct xtvec64 *xtv,
					      size_t xtv_count);
#endif

/*
 * Sync file
 */
extern ioid_t SYSIO_INTERFACE_NAME(ifsync)(int fd);

/*
 * Sync file data
 */
extern ioid_t SYSIO_INTERFACE_NAME(ifdatasync)(int fd);
#endif /* ! _XTIO_H_ */
