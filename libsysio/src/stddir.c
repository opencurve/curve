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
 */

#ifdef __linux__
#include <features.h>
#if defined(__GLIBC__) && !defined(REDSTORM) 

/*
 * stddir.c
 *
 * As of glibc 2.3, the new capability to define functions with a 'hidden'
 * attribute means that any time glibc decides to use that capability
 * we will no longer be able to successfully intercept low level calls
 * in a link against default system glibc. Thus the following imported 
 * functions.
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>

#include <sysio.h>

#include "sysio-symbols.h"
#include "stddir.h"

/***********************************************************
 * dir series functions                                    *
 ***********************************************************/

DIR* 
SYSIO_INTERFACE_NAME(opendir)(const char *name)
{
	DIR *dir;

	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(opendir, "%s", name);

	dir = (DIR * )calloc(1, sizeof(DIR));
	if (!dir)
		SYSIO_INTERFACE_RETURN(NULL, -ENOMEM, opendir, "%p", 0);

	dir->fd = SYSIO_INTERFACE_NAME(open)(name, O_RDONLY);
	if (dir->fd < 0) {
		free(dir);
		SYSIO_INTERFACE_RETURN(NULL, -errno, opendir, "%p", 0);
	}
	dir->base = 0;
	SYSIO_INTERFACE_RETURN(dir, 0, opendir, "%p", 0);
}

sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(opendir),
		     PREPEND(__, SYSIO_INTERFACE_NAME(opendir)))

int
SYSIO_INTERFACE_NAME(closedir)(DIR *dir)
{
	int rc;
	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(closedir, "%p", dir);

	rc = SYSIO_INTERFACE_NAME(close)(dir->fd);
	free(dir);

	SYSIO_INTERFACE_RETURN(rc, 0, closedir, "%d", 0);
}

sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(closedir), 
		     PREPEND(__, SYSIO_INTERFACE_NAME(closedir)))

int 
SYSIO_INTERFACE_NAME(dirfd)(DIR *dir)
{
	return(dir->fd);
}

long int
SYSIO_INTERFACE_NAME(telldir)(DIR *dir)
{
	return(dir->filepos);
}

void 
SYSIO_INTERFACE_NAME(seekdir)(DIR *dir, long int offset)
{
	dir->filepos = offset;
	dir->base = offset;
	dir->effective = 0;
	dir->cur = 0;
}

void 
SYSIO_INTERFACE_NAME(rewinddir)(DIR *dir)
{
	dir->base = 0;
	dir->filepos = 0;
	dir->cur = 0;
	dir->effective = 0;
}

#endif
#endif
