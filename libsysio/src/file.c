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
#include <assert.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "fs.h"
#include "mount.h"
#include "inode.h"
#include "file.h"

/*
 * Support for file IO.
 */

/*
 * The open files table and it's size.
 */
static struct file **_sysio_oftab = NULL;
static size_t _sysio_oftab_size = 0;

/*
 * Create and initialize open file record.
 */
struct file *
_sysio_fnew(struct pnode *pno, int flags)
{
	struct file *fil;

	fil = malloc(sizeof(struct file));
	if (!fil)
		return NULL;

	P_REF(pno);
	_SYSIO_FINIT(fil, pno, flags);
	FIL_LOCK(fil);
	FIL_REF(fil);

	return fil;
}

/*
 * Destroy open file record.
 */
void
_sysio_fgone(struct file *fil)
{
	int	oerr, err;

	assert(!fil->f_ref);
	assert(fil->f_pno);
	oerr = 0;
	P_GET(fil->f_pno);
	while ((err = PNOP_CLOSE(fil->f_pno))) {
		if (err == EBADF) {
			if (!oerr)
				oerr = err;
			break;
		} else if (oerr) {
			char	*path;
			static char *error_path = "<noname>";

			/*
			 * If this happens we are bleeding file descriptors
			 * from somewhere.
			 */
			path = NULL;
			if (_sysio_pb_pathof(fil->f_pno->p_base,
					     PATH_SEPARATOR,
					     &path) != 0)
				path = error_path;
			_sysio_cprintf("[%lu]\"%s\" pnode won't close (%d)\n",
				       fil->f_pno->p_mount->mnt_fs->fs_id,
				       path,
				       err);
			if (path != error_path)
				free(path);
			break;
		}
		oerr = err;
	}
	P_PUT(fil->f_pno);
	P_RELE(fil->f_pno);
	mutex_unlock(&fil->f_mutex);
	mutex_destroy(&fil->f_mutex);
	free(fil);
}

/*
 * IO operation completion handler.
 */
void
_sysio_fcompletio(struct ioctx *ioctx, struct file *fil)
{
	_SYSIO_OFF_T off;

	if (ioctx->ioctx_cc <= 0)
		return;

	assert(ioctx->ioctx_pno == fil->f_pno);
	off = fil->f_pos + ioctx->ioctx_cc;
	if (fil->f_pos && off <= fil->f_pos)
		abort();
	fil->f_pos = off;
}

/*
 * Grow (or truncate) the file descriptor table.
 */
static int
fd_grow(size_t n)
{
	size_t	count;
	struct file **noftab, **filp;

	/*
	 * Sanity check the new size.
	 */
	if ((int )n < 0)
		return -EMFILE;

	/*
	 * We never shrink the table.
	 */
	if (n <= _sysio_oftab_size)
		return 0;

	noftab = realloc(_sysio_oftab, n * sizeof(struct file *));
	if (!noftab)
		return -ENOMEM;
	_sysio_oftab = noftab;
	count = _sysio_oftab_size;
	_sysio_oftab_size = n;
	filp = _sysio_oftab + count;
	n -= count;
	while (n--)
		*filp++ = NULL;
	return 0;
}

#ifdef ZERO_SUM_MEMORY
void
_sysio_fd_shutdown()
{

	free(_sysio_oftab);
	_sysio_oftab_size = 0;
}
#endif

/*
 * Find a free slot in the open files table greater than or equal to the
 * argument.
 */
static int
find_free_fildes(int low)
{
	int	n;
	int	err;
	struct file **filp;

	for (n = low, filp = _sysio_oftab + low;
	     n >= 0 && (unsigned )n < _sysio_oftab_size && *filp;
	     n++, filp++)
		;
	if (n < 0)
		return -ENFILE;
	if ((unsigned )n >= _sysio_oftab_size) {
		err = fd_grow((unsigned )n + 1);
		if (err)
			return err;
		filp = &_sysio_oftab[n];
		assert(!*filp);
	}

	return n;
}

/*
 * Find open file record from file descriptor.
 */
struct file *
_sysio_fd_find(int fd)
{
	struct file *fil;

	if (fd < 0 || (unsigned )fd >= _sysio_oftab_size)
		return NULL;

	fil = _sysio_oftab[fd];
	if (fil)
		FIL_GET(fil);
	return fil;
}

/*
 * Close an open descriptor.
 */
int
_sysio_fd_close(int fd)
{
	struct file *fil;

	fil = _sysio_fd_find(fd);
	if (!fil)
		return -EBADF;

	_sysio_oftab[fd] = NULL;
	FIL_RELE(fil);
	FIL_PUT(fil);

	return 0;
}

/*
 * Associate open file record with given file descriptor (if forced), or any
 * available file descriptor if less than zero, or any available descriptor
 * greater than or equal to the given one if not forced.
 */
int
_sysio_fd_set(struct file *fil, int fd, int force)
{
	int	err;
	struct file *ofil;

	/*
	 * Search for a free descriptor if needed.
	 */
	if (fd < 0 || !force) {
		if (fd < 0)
			fd = 0;
		fd = find_free_fildes(fd);
		if (fd < 0)
			return fd;
	}

	if ((unsigned )fd >= _sysio_oftab_size) {
		err = fd_grow((unsigned )fd + 1);
		if (err)
			return err;
	}

	/*
	 * Remember old.
	 */
	ofil = _sysio_fd_find(fd);
	/*
	 * Take the entry.
	 */
	_sysio_oftab[fd] = fil;
	FIL_REF(fil);
	if (ofil) {
		FIL_RELE(ofil);
		FIL_PUT(ofil);
	}

	return fd;
}

/*
 * Duplicate old file descriptor.
 *
 * If the new file descriptor is less than zero, the new file descriptor
 * is chosen freely. Otherwise, choose an available descriptor greater
 * than or equal to the new, if not forced. Otherwise, if forced, (re)use
 * the new.
 */
int
_sysio_fd_dup(int oldfd, int newfd, int force)
{
	struct file *fil;
	int	fd;

	if (oldfd == newfd && oldfd >= 0)
		return newfd;

	fil = _sysio_fd_find(oldfd);
	if (!fil)
		return -EBADF;

	fd = _sysio_fd_set(fil, newfd, force);
	FIL_PUT(fil);
	return fd;
}

int
_sysio_fd_close_all()
{
	int	fd;
	struct file **filp;

	/*
	 * Close all open descriptors.
	 */
	for (fd = 0, filp = _sysio_oftab;
	     (size_t )fd < _sysio_oftab_size;
	     fd++, filp++) {
		if (!*filp)
			continue;
		(void )_sysio_fd_close(fd);
	}

	/*
	 * Release current working directory.
	 */
	if (_sysio_cwd) {
		P_RELE(_sysio_cwd);
		_sysio_cwd = NULL;
	}

	return 0;
}
