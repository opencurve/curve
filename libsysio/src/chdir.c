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
 *    Cplant(TM) Copyright 1998-2006 Sandia Corporation. 
 *    Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive
 *    license for use of this work by or on behalf of the US Government.
 *    Export of this program may require a license from the United States
 *    Government.
 */

/*
 * #############################################################################
 * #
 * #     This Cplant(TM) source code is the property of Sandia National
 * #     Laboratories.
 * #
 * #     This Cplant(TM) source code is copyrighted by Sandia National
 * #     Laboratories.
 * #
 * #     The redistribution of this Cplant(TM) source code is subject to the
 * #     terms of the GNU Lesser General Public License
 * #     (see cit/LGPL or http://www.gnu.org/licenses/lgpl.html)
 * #
 * #     Cplant(TM) Copyright 1998-2003 Sandia Corporation. 
 * #     Under the terms of Contract DE-AC04-94AL85000, there is a non-exclusive
 * #     license for use of this work by or on behalf of the US Government.
 * #     Export of this program may require a license from the United States
 * #     Government.
 * #
 * #############################################################################
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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "mount.h"
#include "file.h"
#include "sysio-symbols.h"

#ifdef DEFER_INIT_CWD
const char *_sysio_init_cwd = NULL;
#endif

struct pnode *_sysio_cwd = NULL;

/*
 * Change to directory specified by the given pnode.
 */
int
_sysio_p_chdir(struct pnode *pno)
{
	int	err;

	/*
	 * Revalidate the pnode, and ensure it's an accessable directory
	 */
	err = _sysio_p_validate(pno, NULL, NULL);
	if (err)
		return err;
	if (!(pno->p_base->pb_ino &&
	      S_ISDIR(pno->p_base->pb_ino->i_stbuf.st_mode)))
		return -ENOTDIR;
	if ((err = _sysio_permitted(pno, X_OK)) != 0)
		return err;

	/*
	 * Release old if set.
	 */
        if (_sysio_cwd)
                P_RELE(_sysio_cwd);

	/*
	 * Finally, change to the new.
	 */
	P_REF(pno);
        _sysio_cwd = pno;

        return 0;
}

int
SYSIO_INTERFACE_NAME(chdir)(const char *path)
{
        int     err;
        struct pnode *pno;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(chdir, "%s", path);
        err = _sysio_namei(_sysio_cwd, path, 0, NULL, &pno);
        if (err)
		SYSIO_INTERFACE_RETURN(-1, err, chdir, "%d", 0);

	err = _sysio_p_chdir(pno);
	P_PUT(pno);
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, chdir, "%d", 0);
}

#ifdef REDSTORM
#undef __chdir
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(chdir),
		     PREPEND(__, SYSIO_INTERFACE_NAME(chdir)))
#endif

char *
SYSIO_INTERFACE_NAME(getcwd)(char *buf, size_t size)
{
	int	err;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(getcwd, "%zu", size);
#ifdef DEFER_INIT_CWD
	if (!_sysio_cwd) {
		struct pnode *pno;

		/*
		 * Can no longer defer initialization of the current working
		 * directory. Force namei to make it happen now.
		 */
	        if (_sysio_namei(NULL, ".", 0, NULL, &pno) != 0)
			abort();
	} else
#endif
		P_GET(_sysio_cwd);
	err = _sysio_p_path(_sysio_cwd, &buf, buf ? size : 0);
	P_PUT(_sysio_cwd);
	SYSIO_INTERFACE_RETURN(err ? NULL : buf, err, getcwd, "%s", 0);
}

#ifdef __GLIBC__
#undef __getcwd
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(getcwd), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(getcwd)))
#endif

#if defined(PATH_MAX) && !(defined(REDSTORM))
char    *
SYSIO_INTERFACE_NAME(getwd)(char *buf)
{

	if (!buf) {
		errno = EFAULT;
		return NULL;
	}

	return SYSIO_INTERFACE_NAME(getcwd)(buf, PATH_MAX);
}
#endif
