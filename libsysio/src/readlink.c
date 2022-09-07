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

#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "sysio-symbols.h"

/*
 * Read symbolic link content from passed path-node.
 */
ssize_t
_sysio_p_readlink(struct pnode *pno, char *buf, size_t bufsiz)
{

	if (!pno->p_base->pb_ino)
		return -ESTALE;
	if (!S_ISLNK(pno->p_base->pb_ino->i_stbuf.st_mode))
		return -EINVAL;
	return (ssize_t )PNOP_READLINK(pno, buf, bufsiz);
}

#ifdef HAVE_POSIX_1003_READLINK
ssize_t
#else
int
#endif
SYSIO_INTERFACE_NAME(readlink)(const char *path, char *buf, size_t bufsiz)
{
	struct intent intent;
	int	err;
	ssize_t	cc;
	struct pnode *pno;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(readlink, "%s%zd", path, bufsiz);
	INTENT_INIT(&intent, 0, NULL, NULL);
	err = _sysio_namei(_sysio_cwd, path, ND_NOFOLLOW, &intent, &pno);
	if (err) {
		cc = err;
		goto out;
	}
	cc = _sysio_p_readlink(pno, buf, bufsiz);
	P_PUT(pno);
out:
#ifdef HAVE_POSIX_1003_READLINK
#define _ty	ssize_t
#define _rtnfmt	"%zd"
#else
#define _ty	int
#define _rtnfmt	"%d"
#endif
	SYSIO_INTERFACE_RETURN((_ty )(cc < 0 ? -1 : cc),
			       cc >= 0 ? 0 : (int )cc,
			       readlink, _rtnfmt "%*s", cc, buf);
#undef _rtnfmt
#undef _ty
}

#ifdef REDSTORM
#undef __readlink
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(readlink),
		     PREPEND(__, SYSIO_INTERFACE_NAME(readlink)))
#endif
