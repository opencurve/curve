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
 * Albuquerque, NM 87185-1110
 *
 * lee@sandia.gov
 */

#include "smp.h"

/*
 * Open file support.
 */

/*
 * Test whether large file support on this file.
 */
#ifdef O_LARGEFILE
#define _FIL_LARGEFILE(fil) \
	((fil)->f_flags & O_LARGEFILE)
#else
#define _FIL_LARGEFILE(fil) \
	(1)
#endif
/*
 * Return max seek value for this file.
 */
#define _SEEK_MAX(fil) \
	(_FIL_LARGEFILE(fil) ? _SYSIO_OFF_T_MAX : LONG_MAX)

#ifdef _LARGEFILE64_SOURCE
#define	_SYSIO_FLOCK	flock64
#else
#define	_SYSIO_FLOCK	flock
#endif

/*
 * A file record is maintained for each open file in the system. It holds
 * all the info necessary to track the context and parameters for the
 * operations that may be performed.
 */
struct file {
	mutex_t	f_mutex;				/* record mutex */
	unsigned f_lckcnt;				/* # recursive locks */
	struct pnode *f_pno;				/* path node */
	_SYSIO_OFF_T f_pos;				/* current stream pos */
	unsigned f_ref;					/* ref count */
	int	f_flags;				/* open/fcntl flags */
};

#ifdef LOCK_DEBUG
#define _FIL_CHECK_LOCK(_fil, _test) \
	(assert(((_test) && (_fil)->f_lckcnt) || \
		!((_test) || (_fil)->f_lckcnt)))
#else
#define _FIL_CHECK_LOCK(_fil, _test)
#endif

#define FIL_LOCK(_fil) \
	do { \
		mutex_lock(&(_fil)->f_mutex); \
		(_fil)->f_lckcnt++; \
		_FIL_CHECK_LOCK((_fil), 1); \
	} while (0)

#define FIL_UNLOCK(_fil) \
	do { \
		_FIL_CHECK_LOCK((_fil), 1); \
		(_fil)->f_lckcnt--; \
		if ((_fil)->f_ref) \
			mutex_unlock(&(_fil)->f_mutex); \
		else \
			_sysio_fgone(_fil); \
	} while (0)

/*
 * Reference a file record.
 */
#define _FIL_REF_NO_LOCK(_fil) \
	do { \
		_FIL_CHECK_LOCK((_fil), 1); \
		(_fil)->f_ref++; \
		assert((_fil)->f_ref); \
	} while (0)

/*
 * Reference a file record.
 */
#define FIL_REF(_fil) \
	do { \
		FIL_LOCK(_fil); \
		_FIL_REF_NO_LOCK(_fil); \
		FIL_UNLOCK(_fil); \
	} while (0)

/*
 * Release reference to a file record.
 */
#define _FIL_RELE_NO_LOCK(_fil) \
	do { \
		_FIL_CHECK_LOCK((_fil), 1); \
		(_fil)->f_ref--; \
	} while (0)

/*
 * Release reference to a file record.
 */
#define FIL_RELE(_fil) \
	do { \
		FIL_LOCK(_fil); \
		_FIL_RELE_NO_LOCK(_fil); \
		FIL_UNLOCK(_fil); \
	} while (0)

/*
 * Lock and reference file node; Lock associated path node.
 */
#define FIL_GET(_fil) \
	do { \
		FIL_LOCK(_fil); \
		_FIL_REF_NO_LOCK(_fil); \
		P_GET((_fil)->f_pno); \
	} while (0)

/*
 * Unlock and drop reference to file node; Unlock associated path node.
 */
#define FIL_PUT(_fil) \
	do { \
		_FIL_CHECK_LOCK((_fil), 1); \
		P_PUT((_fil)->f_pno); \
		_FIL_RELE_NO_LOCK(_fil); \
		FIL_UNLOCK(_fil); \
	} while (0)

/*
 * Init file record.
 *
 * NB: Don't forget to take a reference to the pnode too!
 */
#define _SYSIO_FINIT(fil, pno, flags) \
	do { \
		mutex_init(&(fil)->f_mutex, MUTEX_RECURSIVE); \
		(fil)->f_lckcnt = 0; \
		(fil)->f_pno = (pno); \
		(fil)->f_pos = 0; \
		(fil)->f_ref = 0; \
		(fil)->f_flags = (flags); \
	} while (0)

/*
 * Valid file object?
 */
#define FIL_FILEOK(_fil) \
	((_fil) && (_fil)->f_pno)

/*
 * Determine if a file may be read/written.
 *
 * Given a ptr to an open file table entry and a flag indicating desired
 * access return non-zero if the file record indicates that the access is
 * permitted or zero, if not.
 *
 * 'r'	for read access check
 * 'w'	for write access check
 */

#define FIL_CHKRW(_fil, _c) \
	(((_c) == 'r' && !((_fil)->f_flags & O_WRONLY)) || \
	 ((_c) == 'w' && ((_fil)->f_flags & (O_WRONLY | O_RDWR))))

/*
 * Is file object writable? Return 0, if so. Otherwise, the appropriate
 * (negated) error number.
 */
#define FIL_WRITEOK(_fil) \
	(!(FIL_FILEOK(_fil) && FIL_CHKRW((_fil), 'w')) \
	   ? -EBADF \
	   : (IS_RDONLY((_fil)->f_pno) ? -EROFS : 0))

struct ioctx;

extern struct file *_sysio_fnew(struct pnode *pno, int flags);
extern void _sysio_fgone(struct file *fil);
extern void _sysio_fcompletio(struct ioctx *ioctx, struct file *fil);
extern int _sysio_fd_close(int fd);
extern struct file *_sysio_fd_find(int fd);
extern int _sysio_fd_set(struct file *fil, int fd, int force);
extern int _sysio_fd_dup(int oldfd, int newfd, int force);
extern int _sysio_fd_close_all(void);
#ifdef ZERO_SUM_MEMORY
extern void _sysio_fd_shutdown(void);
#endif
extern _SYSIO_OFF_T _sysio_lseek_prepare(struct file *fil,
					 _SYSIO_OFF_T offset,
					 int whence,
					 _SYSIO_OFF_T max);
