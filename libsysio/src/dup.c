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

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "file.h"
#include "sysio-symbols.h"

int
SYSIO_INTERFACE_NAME(dup2)(int oldfd, int newfd)
{
	int	fd;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(dup2, "%d%d", oldfd, newfd);
	if (newfd < 0)
		SYSIO_INTERFACE_RETURN(-1, -EBADF, dup2, "%d", 0);
	fd = _sysio_fd_dup(oldfd, newfd, 1);
	SYSIO_INTERFACE_RETURN(fd < 0 ? -1 : fd,
			       fd < 0 ? fd : 0,
			       dup2, "%d", 0);
}

#ifdef REDSTORM
#undef __dup2 
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(dup2), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(dup2)))
#endif

int
SYSIO_INTERFACE_NAME(dup)(int oldfd)
{
	int	fd;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(dup, "%d", oldfd);
	fd = _sysio_fd_dup(oldfd, -1, 0);
	SYSIO_INTERFACE_RETURN(fd < 0 ? -1 : fd,
			       fd < 0 ? fd : 0,
			       dup, "%d", 0);
}

#ifdef __GLIBC__
#undef __dup
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(dup), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(dup)))
#endif
