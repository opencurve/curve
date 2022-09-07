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

#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "dev.h"

const struct inode_ops _sysio_nodev_ops = {
	_sysio_nodev_inop_lookup,
	_sysio_nodev_inop_getattr,
	_sysio_nodev_inop_setattr,
	_sysio_nodev_inop_filldirentries,
	_sysio_nodev_inop_filldirentries2,
	_sysio_nodev_inop_mkdir,
	_sysio_nodev_inop_rmdir,
	_sysio_nodev_inop_symlink,
	_sysio_nodev_inop_readlink,
	_sysio_nodev_inop_open,
	_sysio_nodev_inop_close,
	_sysio_nodev_inop_link,
	_sysio_nodev_inop_unlink,
	_sysio_nodev_inop_rename,
	_sysio_nodev_inop_read,
	_sysio_nodev_inop_write,
	_sysio_nodev_inop_pos,
	_sysio_nodev_inop_iodone,
	_sysio_nodev_inop_fcntl,
	NULL,
	_sysio_nodev_inop_sync,
	NULL,
	_sysio_nodev_inop_datasync,
	_sysio_nodev_inop_ioctl,
	_sysio_nodev_inop_mknod,
#ifdef _HAVE_STATVFS
	_sysio_nodev_inop_statvfs,
#endif
	_sysio_nodev_inop_perms_check,
	_sysio_nodev_inop_gone
};

/*
 * Support for pseudo-devices.
 */

struct device {
	const char *dev_name;
	struct inode_ops dev_ops;
};

static struct device cdev[128];

int
_sysio_dev_init()
{
	unsigned major;

	major = 0;
	do {
		cdev[major].dev_name = NULL;
		cdev[major].dev_ops = _sysio_nodev_ops;
	} while (++major < sizeof(cdev) / sizeof(struct device));

	return 0;
}

/*
 * Allocate major dev number in the dynamic range [128-255].
 */
dev_t
_sysio_dev_alloc()
{
	unsigned short major;
	static unsigned char c_major = 128;

	assert(c_major);
	major = c_major++;
	return SYSIO_MKDEV(major, 0);
}

static int
dev_register(struct device devtbl[],
	     int major,
	     const char *name,
	     struct inode_ops *ops)
{

	assert(major < 128);

	if (major < 0) {
		major = sizeof(cdev) / sizeof(struct device);
		while (major--) {
			if (!devtbl[major].dev_name)
				break;
		}
	}
	if (major < 0)
		return -ENXIO;				/* I dunno, what? */
	if (devtbl[major].dev_name)
		return -EEXIST;
	devtbl[major].dev_name = name;
	devtbl[major].dev_ops = *ops;

	return major;
}

int
_sysio_char_dev_register(int major, const char *name, struct inode_ops *ops)
{

	return dev_register(cdev, major, name, ops);
}

struct inode_ops *
_sysio_dev_lookup(mode_t mode, dev_t dev)
{
	struct device *devtbl;
	dev_t	major;

	if (S_ISCHR(mode) || S_ISFIFO(mode))
		devtbl = cdev;
	else
		return (struct inode_ops *)&_sysio_nodev_ops;

	major = SYSIO_MAJOR_DEV(dev);
	if (!(major < 128) || !devtbl[major].dev_name)
		return (struct inode_ops *)&_sysio_nodev_ops;

	return &devtbl[major].dev_ops;
}
