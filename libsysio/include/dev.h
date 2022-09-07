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
 * Albuquerque, NM 87185-1110
 *
 * lee@sandia.gov
 */

/*
 * Device support.
 */

/*
 * Make a device number, composed of major and minor parts. We *assume* that
 * the system version of a dev_t is 16 bits or more.
 */
#define SYSIO_MKDEV(major, minor) \
	((((major) & 0xff) << 8) | ((minor) & 0xff))

/*
 * Return major unit given dev number.
 */
#define SYSIO_MAJOR_DEV(dev) \
	(((dev) >> 8) & 0xff)

/*
 * Return minor unit given dev number.
 */
#define SYSIO_MINOR_DEV(dev) \
	((dev) & 0xff)

extern const struct inode_ops _sysio_nodev_ops;

#define _sysio_nodev_inop_lookup \
	(int (*)(struct pnode *, \
		 struct inode **, \
		 struct intent *, \
		 const char *))_sysio_do_illop
#define _sysio_nodev_inop_getattr \
	(int (*)(struct pnode *, \
		 struct intnl_stat *))_sysio_do_ebadf
#define _sysio_nodev_inop_setattr \
	(int (*)(struct pnode *, \
		 unsigned , \
		 struct intnl_stat *))_sysio_do_ebadf
#define _sysio_nodev_inop_filldirentries \
	(ssize_t (*)(struct pnode *, \
		     _SYSIO_OFF_T *, \
		     char *, \
		     size_t))_sysio_do_illop
#define _sysio_nodev_inop_filldirentries2 \
	(int (*)(struct pnode *, \
		 _SYSIO_OFF_T *, \
		 filldir_t, \
		 void *))_sysio_do_illop
#define _sysio_nodev_inop_mkdir \
	(int (*)(struct pnode *, \
		 mode_t))_sysio_do_illop
#define _sysio_nodev_inop_rmdir \
	(int (*)(struct pnode *))_sysio_do_illop
#define _sysio_nodev_inop_symlink \
	(int (*)(struct pnode *, \
		 const char *))_sysio_do_illop
#define _sysio_nodev_inop_readlink \
	(int (*)(struct pnode *, \
		 char *, \
		 size_t))_sysio_do_illop
#define _sysio_nodev_inop_open \
	(int (*)(struct pnode *, \
		 int, \
		 mode_t))_sysio_do_enodev
#define _sysio_nodev_inop_close \
	(int (*)(struct pnode *))_sysio_do_ebadf
#define _sysio_nodev_inop_link \
	(int (*)(struct pnode *, struct pnode *))_sysio_do_illop
#define _sysio_nodev_inop_unlink \
	(int (*)(struct pnode *))_sysio_do_illop
#define _sysio_nodev_inop_rename \
	(int (*)(struct pnode *, struct pnode *))_sysio_do_illop
#define _sysio_nodev_inop_read \
	(int (*)(struct ioctx *))_sysio_do_ebadf
#define _sysio_nodev_inop_write \
	(int (*)(struct ioctx *))_sysio_do_ebadf
#define _sysio_nodev_inop_pos \
	(_SYSIO_OFF_T (*)(struct pnode *, _SYSIO_OFF_T))_sysio_do_ebadf
#define _sysio_nodev_inop_iodone \
	(int (*)(struct ioctx *))_sysio_do_einval
#define _sysio_nodev_inop_fcntl \
	(int (*)(struct pnode *, \
		 int, \
		 va_list, \
		 int *))_sysio_do_ebadf
#define _sysio_nodev_inop_sync \
	(int (*)(struct pnode *))_sysio_do_ebadf
#define _sysio_nodev_inop_datasync \
	(int (*)(struct pnode *))_sysio_do_ebadf
#define _sysio_nodev_inop_ioctl \
	(int (*)(struct pnode *, \
		 unsigned long int, \
		 va_list))_sysio_do_ebadf
#define _sysio_nodev_inop_mknod \
	(int (*)(struct pnode *, \
		 mode_t, \
		 dev_t))_sysio_do_illop
#ifdef _HAVE_STATVFS
#define _sysio_nodev_inop_statvfs \
	(int (*)(struct pnode *, \
		 struct intnl_statvfs *))_sysio_do_illop
#endif
#define _sysio_nodev_inop_perms_check \
	(int (*)(struct pnode *, int amode, int effective))_sysio_do_ebadf
#define _sysio_nodev_inop_gone \
	(void (*)(struct inode *))_sysio_do_noop

extern int _sysio_dev_init(void);
extern dev_t _sysio_dev_alloc(void);
extern struct inode_ops *_sysio_dev_lookup(mode_t mode, dev_t dev);
extern int _sysio_char_dev_register(int major,
				    const char *name,
				    struct inode_ops *ops);
