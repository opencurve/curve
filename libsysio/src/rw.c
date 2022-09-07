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
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/queue.h>

#include "sysio.h"
#include "xtio.h"
#include "file.h"
#include "inode.h"

#include "sysio-symbols.h"

typedef enum { READ, WRITE } direction;

/*
 * Post some IO operation.
 */
static int
_sysio_post_io(int (*f)(struct ioctx *ioctx),
	       struct pnode *pno,
	       const struct iovec *iov, size_t iov_count,
	       void (*release_iov)(struct ioctx *, void *),
	       const struct intnl_xtvec *xtv, size_t xtv_count,
	       void (*release_xtv)(struct ioctx *, void *),
	       void (*completio)(struct ioctx *, void *),
	       struct file *fil,
	       void *args,
	       struct ioctx **ioctxp)
{
	struct ioctx *ioctx;
	int	err;
	struct ioctx_callback *cb;

	assert(f != NULL && pno->p_base->pb_ino != NULL);

	/*
	 * Get new IO context.
	 */
	ioctx =
	    _sysio_ioctx_new(pno,
			     iov, iov_count,
			     xtv, xtv_count,
			     args);
	if (ioctx == NULL)
		return -ENOMEM;
	do {
		/*
		 * Set callbacks, if given.
		 */
		if ((release_iov &&
		     (err =
			  _sysio_ioctx_cb(ioctx, release_iov, (void *)iov))) ||
		    (release_xtv &&
		     (err =
			  _sysio_ioctx_cb(ioctx, release_xtv, (void *)xtv))) ||
		    (completio &&
		     (err = _sysio_ioctx_cb(ioctx, completio, fil))))
		     	break;
		/*
		 * Post the op.
		 */
		err = INOP_CALL(f, ioctx);
	} while (0);
	if (!err) {
		*ioctxp = ioctx;
		return 0;
	}
	/*
	 * We won't be needing the completion callbacks after all.
	 */
	while ((cb = ioctx->ioctx_cbq.tqh_first)) {
		TAILQ_REMOVE(&ioctx->ioctx_cbq, cb, iocb_next);
		_sysio_ioctx_cb_free(cb);
	}
	_sysio_ioctx_complete(ioctx);
	return err;
}

/*
 * Release resource at completion.
 */
static void
free_arg(struct ioctx *ioctx __IS_UNUSED, void *arg)
{

	free(arg);
}

/*
 * Post simple asynch, internal, IO operation.
 */
static int
_do_p_aio(int (*f)(struct ioctx *ioctx),
	  struct pnode *pno,
	  _SYSIO_OFF_T off,
	  void *buf,
	  size_t count,
	  struct ioctx **ioctxp)
{
	struct iovec *iov;
	struct intnl_xtvec *xtv;
	int	err;

	iov = malloc(sizeof(struct iovec));
	xtv = malloc(sizeof(struct intnl_xtvec));
	err = -ENOMEM;
	if (iov && xtv) {
		iov->iov_base = buf;
		xtv->xtv_off = off;
		iov->iov_len = xtv->xtv_len = count;
		err =
		    _sysio_post_io(f,
				   pno,
				   iov, 1, free_arg,
				   xtv, 1, free_arg,
				   NULL, NULL,
				   NULL,
				   ioctxp);
	}
	if (err) {
		if (iov)
			free(iov);
		if (xtv)
			free(xtv);
	}
	return err;
}

/*
 * Post simple asynch read from pnode.
 */
int
_sysio_p_aread(struct pnode *pno,
	       _SYSIO_OFF_T off,
	       void *buf,
	       size_t count,
	       struct ioctx **ioctxp)
{

	return _do_p_aio(PNOP_FUNC(pno, read), pno, off, buf, count, ioctxp);
}

/*
 * Post simple asynch write to pnode.
 */
int
_sysio_p_awrite(struct pnode *pno,
		_SYSIO_OFF_T off,
		void *buf,
		size_t count,
		struct ioctx **ioctxp)
{

	return _do_p_aio(PNOP_FUNC(pno, write), pno, off, buf, count, ioctxp);
}

/*
 * Post asynch IO operation using path-node.
 */
int
_sysio_p_iiox(int (*f)(struct ioctx *),
	      struct pnode *pno,
	      _SYSIO_OFF_T limit,
	      const struct iovec *iov, size_t iov_count,
	      void (*release_iov)(struct ioctx *, void *),
	      const struct intnl_xtvec *xtv, size_t xtv_count,
	      void (*release_xtv)(struct ioctx *, void *),
	      void (*completio)(struct ioctx *, void *),
	      void *data,
	      void *args,
	      struct ioctx **ioctxp)
{
	int	err;

	err = 0;
	do {
		if (f == NULL) {
			err = -ENOTSUP;
			break;
		}

		/*
		 * Is it live?
		 */
		if (!pno->p_base->pb_ino) {
			err = -EBADF;
			break;
		}

		if (limit && xtv && iov) {
			ssize_t	cc;

			/*
			 * Valid maps?
		 	*/
			cc =
			    _sysio_validx(xtv, xtv_count,
					  iov, iov_count,
					  limit);
			if (cc < 0) {
				err = cc;
				break;
			}
		}
		/*
		 * Post the operation.
		 */
		err =
		    _sysio_post_io(f,
				   pno,
				   iov, iov_count, release_iov,
				   xtv, xtv_count, release_xtv,
				   completio,
				   data,
				   args,
				   ioctxp);
	} while (0);
	return err;
}

/*
 * Post asynch IO operation using file handle.
 */
static int
_sysio_iiox(direction writing,
	    struct file *fil,
	    const struct iovec *iov, size_t iov_count,
	    void (*release_iov)(struct ioctx *, void *),
	    const struct intnl_xtvec *xtv, size_t xtv_count,
	    void (*release_xtv)(struct ioctx *, void *),
	    void (*completio)(struct ioctx *, void *),
	    struct ioctx **ioctxp)
{
	int	err;

	/*
	 * Opened for proper access?
	 */
	if (!FIL_CHKRW(fil, writing ? 'w' : 'r'))
		return -EBADF;
	err =
	    _sysio_p_iiox((writing
			     ? PNOP_FUNC(fil->f_pno, write)
			     : PNOP_FUNC(fil->f_pno, read)),
			  fil->f_pno,
			  (
#if defined(_LARGEFILE64_SOURCE) && defined(O_LARGEFILE)
			   (fil->f_flags & O_LARGEFILE)
			     ? LONG_MAX
			     :
#endif
			       _SYSIO_OFF_T_MAX),
			  iov, iov_count, release_iov,
			  xtv, xtv_count, release_xtv,
			  completio, fil,
			  NULL,
			  ioctxp);
	return err;
}

/*
 * Decoding the interface routine names:
 *
 * Much of this carries legacy from the POSIX world and the Intel ASCI
 * Red programming environment. Routine names are composed of prefix,
 * basic POSIX names, and postfix. The basic POSIX names are read and write.
 * Prefixes, left-to-right:
 *
 *	- 'i' -- asynchronous operation (from ASCI Red)
 *	- 'p' -- positional (POSIX)
 * Posfixes, only one:
 *	- 'v' -- vectored (POSIX)
 *	- 'x' -- extent-based (new for Red Storm)
 *
 * All valid combinations are available and symmetric.
 */

/*
 * Post ireadx using fildes.
 */
static ioid_t
_do_ireadx(int fd,
	   const struct iovec *iov, size_t iov_count,
	   void (*release_iov)(struct ioctx *, void *),
	   const struct intnl_xtvec *xtv, size_t xtv_count,
	   void (*release_xtvec)(struct ioctx *, void *),
	   void (*completio)(struct ioctx *, void *))
{
	struct file *fil;
	int	err;
	struct ioctx *ioctx;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(ireadx,
			      "%d%*iZ%*xY",
			      fd,
			      iov_count, iov,
			      xtv_count, xtv);
	do {
		fil = _sysio_fd_find(fd);
		if (fil == NULL) {
			err = -EBADF;
			break;
		}
		err =
		    _sysio_iiox(READ,
				fil,
				iov, iov_count, release_iov,
				xtv, xtv_count, release_xtvec,
				completio,
				&ioctx);
	} while (0);
	if (fil)
		FIL_PUT(fil);
	SYSIO_INTERFACE_RETURN(err ? IOID_FAIL : ioctx, err, ireadx, "%p", 0);
}

#ifdef _LARGEFILE64_SOURCE
ioid_t
SYSIO_INTERFACE_NAME(iread64x)(int fd,
			       const struct iovec *iov, size_t iov_count,
			       const struct xtvec64 *xtv, size_t xtv_count)
{

	return _do_ireadx(fd,
			  iov, iov_count, NULL,
			  xtv, xtv_count, NULL,
			  NULL);
}
#endif

/*
 * Post iwritex using fildes.
 */
static ioid_t
_do_iwritex(int fd,
	    const struct iovec *iov, size_t iov_count,
	    void (*release_iov)(struct ioctx *, void *),
	    const struct intnl_xtvec *xtv, size_t xtv_count,
	    void (*release_xtvec)(struct ioctx *, void *),
	    void (*completio)(struct ioctx *, void *))
{
	struct file *fil;
	int	err;
	struct ioctx *ioctx;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(iwritex,
			      "%d%*iZ%*xY",
			      fd,
			      iov_count, iov,
			      xtv_count, xtv);
	do {
		fil = _sysio_fd_find(fd);
		if (fil == NULL) {
			err = -EBADF;
			break;
		}
		err =
		    _sysio_iiox(WRITE,
				fil,
				iov, iov_count, release_iov,
				xtv, xtv_count, release_xtvec,
				completio,
				&ioctx);
	} while (0);
	if (fil)
		FIL_PUT(fil);
	SYSIO_INTERFACE_RETURN(err ? IOID_FAIL : ioctx, err, iwritex, "%p", 0);
}

#ifdef _LARGEFILE64_SOURCE
ioid_t
SYSIO_INTERFACE_NAME(iwrite64x)(int fd,
				const struct iovec *iov, size_t iov_count,
				const struct xtvec64 *xtv, size_t xtv_count)
{

	return _do_iwritex(fd,
			   iov, iov_count, NULL,
			   xtv, xtv_count, NULL,
			   NULL);
}
#endif

/*
 * Convert to internal xtvec form and post extended asynch IO using fildes.
 */
static ioid_t
_do_iiox(int fd,
	 ioid_t (*f)(int,
		     const struct iovec *, size_t,
		     void (*)(struct ioctx *, void *),
		     const struct intnl_xtvec *, size_t,
		     void (*)(struct ioctx *, void *),
		     void (*)(struct ioctx *, void *)),
	 const struct iovec *iov, size_t iov_count,
	 const struct xtvec *xtv, size_t xtv_count)
{
#ifdef _LARGEFILE64_SOURCE
	struct intnl_xtvec *myixtv, *ixtv;
	size_t	n;
#endif
	ioid_t	ioid;

#ifdef _LARGEFILE64_SOURCE
	myixtv = malloc(xtv_count * sizeof(struct intnl_xtvec));
	if (myixtv == NULL)
		return IOID_FAIL;
	for (n = xtv_count, ixtv = myixtv; n; n--, ixtv++, xtv++) {
		ixtv->xtv_off = xtv->xtv_off;
		ixtv->xtv_len = xtv->xtv_len;
	}
#endif
	ioid =
	    (*f)(fd,
		 iov, iov_count, NULL,
		 myixtv, xtv_count, free_arg,
		 NULL);
#ifdef _LARGEFILE64_SOURCE
	if (ioid == IOID_FAIL)
		free(myixtv);
#endif
	return ioid;
}

ioid_t
SYSIO_INTERFACE_NAME(ireadx)(int fd,
			     const struct iovec *iov, size_t iov_count,
			     const struct xtvec *xtv, size_t xtv_count)
{

	return _do_iiox(fd, _do_ireadx, iov, iov_count, xtv, xtv_count);
}

ioid_t
SYSIO_INTERFACE_NAME(iwritex)(int fd,
			      const struct iovec *iov, size_t iov_count,
			      const struct xtvec *xtv, size_t xtv_count)
{

	return _do_iiox(fd, _do_iwritex, iov, iov_count, xtv, xtv_count);
}

#ifdef _LARGEFILE64_SOURCE
/*
 * Perform 64-bit extended IO.
 */
static ssize_t
_do_io64x(int fd,
	  ioid_t (*f)(int fd,
		      const struct iovec *iov, size_t iov_count,
		      const struct xtvec64 *xtv, size_t xtv_count),
	  const struct iovec *iov, size_t iov_count,
	  const struct xtvec64 *xtv, size_t xtv_count)
{
	ioid_t 	ioid;

	ioid = (*f)(fd, iov, iov_count, xtv, xtv_count);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait)(ioid);
}

ssize_t
SYSIO_INTERFACE_NAME(read64x)(int fd,
			      const struct iovec *iov, size_t iov_count,
			      const struct xtvec64 *xtv, size_t xtv_count)
{

	return _do_io64x(fd,
			 SYSIO_INTERFACE_NAME(iread64x),
			 iov, iov_count,
			 xtv, xtv_count);
}

ssize_t
SYSIO_INTERFACE_NAME(write64x)(int fd,
			       const struct iovec *iov, size_t iov_count,
			       const struct xtvec64 *xtv, size_t xtv_count)
{

	return _do_io64x(fd,
			 SYSIO_INTERFACE_NAME(iwrite64x),
			 iov, iov_count,
			 xtv, xtv_count);
}
#endif

/*
 * Perform native extended IO.
 */
static ssize_t
_do_iox(int fd,
	ioid_t (*f)(int fd,
		    const struct iovec *iov, size_t iov_count,
		    const struct xtvec *xtv, size_t xtv_count),
	const struct iovec *iov, size_t iov_count,
	const struct xtvec *xtv, size_t xtv_count)
{
	ioid_t 	ioid;

	ioid = (*f)(fd, iov, iov_count, xtv, xtv_count);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait)(ioid);
}

ssize_t
SYSIO_INTERFACE_NAME(readx)(int fd,
			    const struct iovec *iov, size_t iov_count,
			    const struct xtvec *xtv, size_t xtv_count)
{

	return _do_iox(fd,
		       SYSIO_INTERFACE_NAME(ireadx),
		       iov, iov_count,
		       xtv, xtv_count);
}

ssize_t
SYSIO_INTERFACE_NAME(writex)(int fd,
			     const struct iovec *iov, size_t iov_count,
			     const struct xtvec *xtv, size_t xtv_count)
{

	return _do_iox(fd,
		       SYSIO_INTERFACE_NAME(iwritex),
		       iov, iov_count,
		       xtv, xtv_count);
}

/*
 * Return count of all bytes mapped by iovec vector.
 */
static ssize_t
_sysio_sum_iovec(const struct iovec *iov, int count)
{
	ssize_t	tmp, cc;

	if (count <= 0)
		return -EINVAL;

	cc = 0;
	while (count--) {
		tmp = cc;
		cc += iov->iov_len;
		if (tmp && iov->iov_len && cc <= tmp)
			return -EINVAL;
		iov++;
	}
	return cc;
}

/*
 * Post async IO using fildes at given file offset by iovec.
 */
static ioid_t
_do_ipiov(int fd,
	  ioid_t (*f)(int,
	  	      const struct iovec *, size_t,
		      void (*)(struct ioctx *, void *),
	  	      const struct intnl_xtvec *, size_t,
		      void (*)(struct ioctx *, void *),
		      void (*)(struct ioctx *, void *)),
	  const struct iovec *iov,
	  size_t count,
	  void (*release_iov)(struct ioctx *, void *),
	  _SYSIO_OFF_T offset,
	  void (*completio)(struct ioctx *, void *))
{
	ssize_t	cc;
	struct intnl_xtvec *xtv;
	ioid_t	ioid;

	cc = _sysio_sum_iovec(iov, count);
	if (cc < 0) {
		errno = (int )-cc;
		return IOID_FAIL;
	}
	xtv = malloc(sizeof(struct intnl_xtvec));
	if (xtv == NULL)
		return IOID_FAIL;
	xtv->xtv_off = offset;
	xtv->xtv_len = cc;
	ioid =
	    (*f)(fd,
		 iov, count, release_iov,
		 xtv, 1, free_arg,
		 completio);
	if (ioid == IOID_FAIL)
		free(xtv);
	return ioid;
}

#ifdef _LARGEFILE64_SOURCE
ioid_t
SYSIO_INTERFACE_NAME(ipread64v)(int fd,
				const struct iovec *iov, size_t count,
				off64_t offset)
{

	return _do_ipiov(fd, _do_ireadx, iov, count, NULL, offset, NULL);
}

ioid_t
SYSIO_INTERFACE_NAME(ipwrite64v)(int fd,
				 const struct iovec *iov, size_t count,
				 off64_t offset)
{

	return _do_ipiov(fd, _do_iwritex, iov, count, NULL, offset, NULL);
}
#endif

ioid_t
SYSIO_INTERFACE_NAME(ipreadv)(int fd,
			      const struct iovec *iov, size_t count,
			      off_t offset)
{

	return _do_ipiov(fd, _do_ireadx, iov, count, NULL, offset, NULL);
}

ioid_t
SYSIO_INTERFACE_NAME(ipwritev)(int fd,
			       const struct iovec *iov, size_t count,
			       off_t offset)
{

	return _do_ipiov(fd, _do_iwritex, iov, count, NULL, offset, NULL);
}

/*
 * Perform IO at given offset using iovec.
 */
static ssize_t
_do_piov(int fd,
	 ioid_t (*f)(int,
	  	     const struct iovec *, size_t,
		     void (*)(struct ioctx *, void *),
	  	     const struct intnl_xtvec *, size_t,
		     void (*)(struct ioctx *, void *),
		     void (*)(struct ioctx *, void *)),
	 const struct iovec *iov, size_t count,
	 _SYSIO_OFF_T offset)
{
	ioid_t	ioid;

	ioid = _do_ipiov(fd, f,  iov, count, NULL, offset, NULL);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait)(ioid);
}

#ifdef _LARGEFILE64_SOURCE
ssize_t
SYSIO_INTERFACE_NAME(pread64v)(int fd,
			       const struct iovec *iov, size_t count,
			       off64_t offset)
{

	return _do_piov(fd, _do_ireadx, iov, count, offset);
}

ssize_t
SYSIO_INTERFACE_NAME(pwrite64v)(int fd,
			       const struct iovec *iov, size_t count,
			       off64_t offset)
{

	return _do_piov(fd, _do_iwritex, iov, count, offset);
}
#endif

ssize_t
SYSIO_INTERFACE_NAME(preadv)(int fd,
			     const struct iovec *iov, int count,
			     off_t offset)
{

	return _do_piov(fd, _do_ireadx, iov, count, offset);
}

ssize_t
SYSIO_INTERFACE_NAME(pwritev)(int fd,
			      const struct iovec *iov, int count,
			      off_t offset)
{

	return _do_piov(fd, _do_iwritex, iov, count, offset);
}

/*
 * Post asynch IO using fildes at given offset.
 */
static ioid_t
_do_ipio(int fd,
	 ioid_t (*f)(int,
	  	     const struct iovec *, size_t,
		     void (*)(struct ioctx *, void *),
	  	     const struct intnl_xtvec *, size_t,
		     void (*)(struct ioctx *, void *),
		     void (*)(struct ioctx *, void *)),
	 void *buf, size_t count, 
	 _SYSIO_OFF_T offset)
{
	struct iovec *iov;
	ioid_t	ioid;

	iov = malloc(sizeof(struct iovec));
	if (iov == NULL)
		return IOID_FAIL;
	iov->iov_base = buf;
	iov->iov_len = count;
	ioid = _do_ipiov(fd, f, iov, 1, free_arg, offset, NULL);
	if (ioid == IOID_FAIL)
		free(iov);
	return ioid;
}

#ifdef _LARGEFILE64_SOURCE
ioid_t
SYSIO_INTERFACE_NAME(ipread64)(int fd,
			       void *buf, size_t count,
			       off64_t offset)
{

	return _do_ipio(fd, _do_ireadx, buf, count, offset);
}

ioid_t
SYSIO_INTERFACE_NAME(ipwrite64)(int fd,
				const void *buf, size_t count,
				off64_t offset)
{

	return _do_ipio(fd, _do_iwritex, (void *)buf, count, offset);
}
#endif

ioid_t
SYSIO_INTERFACE_NAME(ipread)(int fd,
			     void *buf, size_t count,
			     off_t offset)
{

	return _do_ipio(fd, _do_ireadx, buf, count, offset);
}

ioid_t
SYSIO_INTERFACE_NAME(ipwrite)(int fd,
			      const void *buf, size_t count,
			      off_t offset)
{

	return _do_ipio(fd, _do_iwritex, (void *)buf, count, offset);
}

/*
 * Perform IO using fildes at given offset.
 */
static ssize_t
_do_pio(int fd,
	ioid_t (*f)(int,
	  	    const struct iovec *, size_t,
		    void (*)(struct ioctx *, void *),
	  	    const struct intnl_xtvec *, size_t,
		    void (*)(struct ioctx *, void *),
		    void (*)(struct ioctx *, void *)),
	void *buf, size_t count,
	_SYSIO_OFF_T offset)
{
	ioid_t	ioid;

	ioid = _do_ipio(fd, f, buf, count, offset);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait)(ioid);
}

#ifdef _LARGEFILE64_SOURCE
ssize_t
SYSIO_INTERFACE_NAME(pread64)(int fd, void *buf, size_t count, off64_t offset)
{

	return _do_pio(fd, _do_ireadx, buf, count, offset);
}

#if defined(__GLIBC__)
#undef __pread64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pread64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(pread64)))
#undef __libc_pread64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pread64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_pread64)))
#endif

ssize_t
SYSIO_INTERFACE_NAME(pwrite64)(int fd, 
			       const void *buf, size_t count, 
			       off64_t offset)
{

	return _do_pio(fd, _do_iwritex, (void *)buf, count, offset);
}

#if defined(__GLIBC__)
#undef __pwrite64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pwrite64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(pwrite64)))
#undef __libc_pwrite64
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pwrite64),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_pwrite64)))
#endif /* defined(__GLIBC__) */
#endif

ssize_t
SYSIO_INTERFACE_NAME(pread)(int fd,
			    void *buf,
			    size_t count,
			    off_t offset)
{

	return _do_pio(fd, _do_ireadx, buf, count, offset);
}

#if defined(__GLIBC__)
#undef __pread
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pread),
		     PREPEND(__, SYSIO_INTERFACE_NAME(pread)))
#undef __libc_pread
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pread),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_pread)))
#endif /* defined(__GLIBC__) */

ssize_t
SYSIO_INTERFACE_NAME(pwrite)(int fd,
			     const void *buf,
			     size_t count,
			     off_t offset)
{

	CURVEFS_DPRINTF("coming into the prwite\n");
	return _do_pio(fd, _do_iwritex, (void *)buf, count, offset);
}

#if defined(__GLIBC__)
#undef __pwrite
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pwrite),
		     PREPEND(__, SYSIO_INTERFACE_NAME(pwrite)))
#undef __libc_pwrite
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(pwrite),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_pwrite)))
#endif /* defined(__GLIBC__) */

/*
 * Post asynch IO using fildes and iovec.
 */
static ioid_t
_do_iiov(direction dir,
	 int fd,
	 const struct iovec *iov, int count,
	 void (*release_iov)(struct ioctx *, void *))
{
	struct intnl_xtvec *xtv;
	int	err;
	struct file *fil;
	ssize_t	cc;
	struct ioctx *ioctx;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	if (dir == READ)
		SYSIO_INTERFACE_ENTER(ireadv, "%d%*iZ", fd, count, iov);
	else
		SYSIO_INTERFACE_ENTER(iwritev, "%d%*iZ", fd, count, iov);
	xtv = NULL;
	err = 0;
	do {
		fil = _sysio_fd_find(fd);
		if (fil == NULL) {
			err = -EBADF;
			break;
		}
		cc = _sysio_sum_iovec(iov, count);
		if (cc < 0) {
			err = -EINVAL;
			break;
		}
		xtv = malloc(sizeof(struct intnl_xtvec));
		if (xtv == NULL) {
			err = -ENOMEM;
			break;
		}
		xtv->xtv_off = fil->f_pos;
		xtv->xtv_len = cc;
		err =
		    _sysio_iiox(dir,
				fil,
				iov, count, release_iov,
				xtv, 1, free_arg,
				(void (*)(struct ioctx *,
					  void *))_sysio_fcompletio,
					  &ioctx);
	} while (0);
	if (fil)
		FIL_PUT(fil);
	if (err && xtv)
		free(xtv);
	if (dir == READ)
		SYSIO_INTERFACE_RETURN(err ? IOID_FAIL : ioctx, err,
				       ireadv, "%p", 0);
	else
		SYSIO_INTERFACE_RETURN(err ? IOID_FAIL : ioctx, err,
				       iwritev, "%p", 0);
	/* Not reached. */
}

ioid_t
SYSIO_INTERFACE_NAME(ireadv)(int fd, const struct iovec *iov, int count)
{

	return _do_iiov(READ, fd, iov, count, NULL);
}

ioid_t
SYSIO_INTERFACE_NAME(iwritev)(int fd, const struct iovec *iov, int count)
{

	return _do_iiov(WRITE, fd, iov, count, NULL);
}

/*
 * Perform IO using fildes and iovec.
 */
static ssize_t
_do_iov(int fd,
	ioid_t (*f)(int,
		    const struct iovec *, int),
	const struct iovec *iov, int count)
{
	ioid_t	ioid;

	ioid = (*f)(fd, iov, count);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait)(ioid);
}

ssize_t
SYSIO_INTERFACE_NAME(readv)(int fd, const struct iovec *iov, int count)
{

	return _do_iov(fd, SYSIO_INTERFACE_NAME(ireadv), iov, count);
}

#if defined(__GLIBC__)
#undef __readv
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(readv),
		     PREPEND(__, SYSIO_INTERFACE_NAME(readv)))
#undef __libc_readv
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(readv),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_readv)))
#endif

ssize_t
SYSIO_INTERFACE_NAME(writev)(int fd, const struct iovec *iov, int count)
{

	return _do_iov(fd, SYSIO_INTERFACE_NAME(iwritev), iov, count);
}

#if defined(__GLIBC__)
#undef __writev
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(writev),
		     PREPEND(__, SYSIO_INTERFACE_NAME(writev)))
#undef __libc_writev
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(writev),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_writev)))
#endif

/*
 * Post asynch IO using fildes.
 */
static ioid_t
_do_iio(direction dir,
	int fd,
	void *buf,
	size_t count)
{
	struct iovec *iov;
	ioid_t	ioid;

	iov = malloc(sizeof(struct iovec));
	if (iov == NULL)
		return IOID_FAIL;
	iov->iov_base = buf;
	iov->iov_len = count;
	ioid = _do_iiov(dir, fd, iov, 1, free_arg);
	if (ioid == IOID_FAIL)
		free(iov);
	return ioid;
}

ioid_t
SYSIO_INTERFACE_NAME(iread)(int fd, void *buf, size_t count)
{

	return _do_iio(READ, fd, buf, count);
}

ioid_t
SYSIO_INTERFACE_NAME(iwrite)(int fd, const void *buf, size_t count)
{

	return _do_iio(WRITE, fd, (void *)buf, count);
}

/*
 * Perform IO using fildes.
 */
static ssize_t
_do_io(int fd, ioid_t (*f)(int, void *, size_t), void *buf, size_t count)
{
	ioid_t	ioid;

	ioid = (*f)(fd, buf, count);
	if (ioid == IOID_FAIL)
		return -1;
	return SYSIO_INTERFACE_NAME(iowait)(ioid);
}

ssize_t
SYSIO_INTERFACE_NAME(read)(int fd, void *buf, size_t count)
{

	return _do_io(fd, SYSIO_INTERFACE_NAME(iread), buf, count);
}

#if defined(__GLIBC__)
#undef __read
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(read),
		     PREPEND(__, SYSIO_INTERFACE_NAME(read)))
#undef __libc_read
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(read),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_read)))
#endif

ssize_t
SYSIO_INTERFACE_NAME(write)(int fd, const void *buf, size_t count)
{

	return _do_io(fd,
		      (ioid_t (*)(int,
				  void *,
				  size_t))SYSIO_INTERFACE_NAME(iwrite),
		      (void *)buf, count);
}

#if defined(__GLIBC__)
#undef __write
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(write),
		     PREPEND(__, SYSIO_INTERFACE_NAME(write)))
#undef __libc_write
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(write),
		     PREPEND(__, SYSIO_INTERFACE_NAME(libc_write)))
#endif
