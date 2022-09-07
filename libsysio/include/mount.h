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

/*
 * Mount support.
 */

struct filesys;
struct pnode;

/*
 * Each file system may be mounted multiple times and in various places
 * in the name space. The mount record maintains the binding information
 * between the system name space and the file system's.
 */
struct mount {
	struct filesys *mnt_fs;				/* file system */
	unsigned mnt_flags;				/* flags (see below) */
	struct pnode *mnt_root;				/* fs sub-tree root */
	struct pnode *mnt_covers;			/* covered pnode */
	LIST_ENTRY(mount) mnt_link;			/* link to next */
};

/*
 * Mount flags definitions.
 */
#define MOUNT_F_RO		0x01			/* read-only */
#ifdef AUTOMOUNT_FILE_NAME
#define MOUNT_F_AUTO		0x02			/* automount enabled */
#endif

/*
 * Mount status (internal state) flags.
 */
#define MOUNT_ST_IFST		0xf000			/* st flags bitmask */
#ifdef AUTOMOUNT_FILE_NAME
#define MOUNT_ST_IFAUTO		0x1000			/* automount */
#endif

/*
 * Test macros for mount status.
 */
#define _MOUNT_ST_ISST(st, mask) \
	(((st) & MOUNT_ST_IFST) == (mask))

#ifdef MOUNT_ST_IFAUTO
#define MOUNT_ST_ISAUTO(st)	_MOUNT_ST_ISST((st), MOUNT_ST_IFAUTO)
#endif

/*
 * Lock mount record.
 */
#define MNT_GET(_mnt)

/*
 * Unlock mount record.
 */
#define MNT_PUT(_mnt)

#ifdef AUTOMOUNT_FILE_NAME
/*
 * Relative name of file containing automount description.
 */
extern struct qstr _sysio_mount_file_name;
#endif

struct pnode_base;
struct inode;

extern int _sysio_mount_init(void);
extern int _sysio_do_mount(struct filesys *fs,
			   struct pnode_base *rootpb,
			   unsigned flags,
			   struct pnode *tocover,
			   struct mount **mntp);
extern int _sysio_mounti(struct filesys *fs,
			 struct inode *rootino,
			 unsigned flags,
			 struct pnode *tocover,
			 struct mount **mntp);
extern int _sysio_do_unmount(struct mount *fs);
extern int _sysio_mount_root(const char *source,
			     const char *type,
			     unsigned flags,
			     const void *data);
extern int _sysio_mount(struct pnode *cwd,
			const char *source,
			const char *target,
			const char *filesystemtype,
			unsigned long mountflags,
			const void *data);
extern int _sysio_unmount_all(void);
#ifdef AUTOMOUNT_FILE_NAME
extern int _sysio_automount(struct pnode *mntpno);
#endif
