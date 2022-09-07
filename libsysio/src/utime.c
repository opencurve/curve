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

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <assert.h>
#include <sys/types.h>
#include <utime.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/queue.h>
#include <sys/time.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"

time_t
_sysio_local_time()
{
	struct timeval tv;

	if (gettimeofday(&tv, NULL) != 0)
		abort();
	return tv.tv_sec;
}

int
SYSIO_INTERFACE_NAME(utime)(const char *path, const struct utimbuf *buf)
{
	int	err;
	struct pnode *pno;
	struct utimbuf _utbuffer;
	struct intnl_stat stbuf;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(utime, "%s", path);
	err = _sysio_namei(_sysio_cwd, path, 0, NULL, &pno);
	if (err)
		goto out;
	if (!buf) {
		_utbuffer.actime = _utbuffer.modtime = _SYSIO_LOCAL_TIME();
		buf = &_utbuffer;
	}
	(void )memset(&stbuf, 0, sizeof(struct intnl_stat));
	stbuf.st_atime = buf->actime;
	stbuf.st_mtime = buf->modtime;
	err =
	    _sysio_p_setattr(pno, SETATTR_ATIME | SETATTR_MTIME, &stbuf);
	P_PUT(pno);
out:
	/*
	 * Note: Pass the utimbuf buffer to the tracing routines some day.
	 */
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, utime, "%d", 0);
}
