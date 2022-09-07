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

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"

#include "sysio-symbols.h"

static int
_sysio_fcntl_raw_call(struct pnode *pno, int *r, int cmd, ...)
{
	va_list	ap;
	int	err;

	va_start(ap, cmd);
	err = PNOP_FCNTL(pno, cmd, ap, r);
	va_end(ap);
	return err;
}

/*
 * Convert offsets to absolute, when appropriate, and call appropriate driver
 * to complete the fcntl lock function. If successful, convert
 * returned values back to appropriate form.
 */
static int
_sysio_fcntl_lock(struct file *fil, int cmd, struct _SYSIO_FLOCK *fl)
{
	struct _SYSIO_FLOCK flock;
	_SYSIO_OFF_T pos;
	int	err;
	int	rtn;

	/*
	 * The drivers will not have a clue as to the
	 * current position of the file pointer. We need to
	 * convert relative whence values to absolute
	 * file adresses for them, then.
	 */
	flock = *fl;
	switch (flock.l_whence) {
	case SEEK_SET:
		/*
		 * At least parameter check this one, too.
		 */
	case SEEK_CUR:
	case SEEK_END:
		pos =
		    _sysio_lseek_prepare(fil,
					 flock.l_start,
					 flock.l_whence,
					 _SEEK_MAX(fil));
		if (pos < 0)
			return (int )pos;
		flock.l_start = pos;
		flock.l_whence = SEEK_SET;
		break;
	default:
		return -EINVAL;
	}
	err =
	    _sysio_fcntl_raw_call(fil->f_pno, &rtn, cmd, &flock);
	if (err)
		return err;
	/*
	 * Ugh, convert back to relative form.
	 */
	switch (fl->l_whence) {
	case SEEK_SET:
		break;
	case SEEK_CUR:
		fl->l_start = flock.l_start;
		fl->l_start -= fil->f_pos;
		break;
	case SEEK_END:
		fl->l_start = flock.l_start;
		fl->l_start -=
		    fil->f_pno->p_base->pb_ino->i_stbuf.st_size;
		break;
	default:
		abort();
	}
	/*
	 * Return success.
	 */
	return 0;
}

static int
_sysio_vfcntl(int fd, int cmd, va_list ap)
{
	int	err;
	int	rtn;
	struct file *fil;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(vfcntl, "%d%d", fd, cmd);
	err = 0;
	fil = _sysio_fd_find(fd);
	if (!fil) {
		rtn = -1;
		err = -EBADF;
		goto out;
	}

	switch (cmd) {

	    case F_DUPFD:
		{
			long	newfd;

			newfd = va_arg(ap, long);
			if (newfd != (int )newfd || newfd < 0) {
				rtn = -1;
				err = -EBADF;
				goto out;
			}
			rtn = _sysio_fd_dup(fd, (int )newfd, 0);
			if (rtn < 0) {
				err = rtn;
				rtn = -1;
			}
		}
		break;
#if !(defined(_LARGEFILE64_SOURCE) && defined(F_GETLK64) && \
      F_GETLK64 == F_GETLK)
	    case F_GETLK:
	    case F_SETLK:
	    case F_SETLKW:
		{
			struct intnl_stat buf;
			struct flock *fl;
#ifdef _LARGEFILE64_SOURCE
			struct _SYSIO_FLOCK flock64;
#endif

			/*
			 * Refresh the cached attributes.
			 */
			err =
			    PNOP_GETATTR(fil->f_pno, &buf);
			if (err) {
				rtn = -1;
				break;
			}
			/*
			 * Copy args to a temp and normalize.
			 */
			fl = va_arg(ap, struct flock *);
#ifdef _LARGEFILE64_SOURCE
			flock64.l_type = fl->l_type;
			flock64.l_whence = fl->l_whence;
			flock64.l_start = fl->l_start;
			flock64.l_len = fl->l_len;
			flock64.l_pid = fl->l_pid;
			err = _sysio_fcntl_lock(fil, cmd, &flock64);
#else
			err = _sysio_fcntl_lock(fil, cmd, fl);
#endif
			if (err < 0) {
				rtn = -1;
				break;
			}
#ifdef _LARGEFILE64_SOURCE
			/*
			 * Copy back. Note that the fcntl_lock call
			 * should have ensured that no overflow was possible.
			 */
			fl->l_type = flock64.l_type;
			fl->l_whence = flock64.l_whence;
			fl->l_start = flock64.l_start;
			assert(fl->l_start == flock64.l_start);
			fl->l_len = flock64.l_len;
			assert(fl->l_len == flock64.l_len);
			fl->l_pid = flock64.l_pid;
#endif
			rtn = 0;
		}
		break;
#endif /* !(_LARGEFILE64_SOURCE && F_GETLK64 == F_GETLK) */
#ifdef _LARGEFILE64_SOURCE
	    case F_GETLK64:
	    case F_SETLK64:
	    case F_SETLKW64:
			{
				struct flock64 *fl64;

				fl64 = va_arg(ap, struct flock64 *);
				err = _sysio_fcntl_lock(fil, cmd, fl64);
				rtn = err ? -1 : 0;
			}
		break;
#endif
	    default:
		err = PNOP_FCNTL(fil->f_pno, cmd, ap, &rtn);
		break;
	}

out:
	FIL_PUT(fil);
	SYSIO_INTERFACE_RETURN(rtn, err, vfcntl, "%d", 0);
}

int
SYSIO_INTERFACE_NAME(fcntl)(int fd, int cmd, ...)
{
	va_list	ap;
	int	err;

	va_start(ap, cmd);
	err = _sysio_vfcntl(fd, cmd, ap);
	va_end(ap);
	return err;
}

sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(fcntl),
		     SYSIO_INTERFACE_NAME(fcntl64))

#ifdef __GLIBC__
#undef __fcntl
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(fcntl), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(fcntl)))
#endif

#ifdef BSD
#undef _fcntl
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(fcntl), 
		     PREPEND(_, SYSIO_INTERFACE_NAME(fcntl)))
#endif
