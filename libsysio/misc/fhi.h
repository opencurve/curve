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

struct file_handle_info_export;

/*
 * File handle information contains everything necessary to uniquely
 * identify a file system object.
 */
struct file_handle_info {
	struct file_handle_info_export *fhi_export;	/* associated export */
	void	*fhi_handle;				/* opaque handle data */
	size_t	fhi_handle_len;				/* opaque handle len */
};

/*
 * Set attributes record, used to alter an existing attribute. One sets
 * the flag, corresponding to the field to be changed, and the field itself.
 */
struct file_handle_info_sattr {
	unsigned
		fhisattr_mode_set		: 1,	/* set mode */
		fhisattr_uid_set		: 1,	/* set uid */
		fhisattr_gid_set		: 1,	/* set gid */
		fhisattr_size_set		: 1,	/* set file length */
		fhisattr_atime_set		: 1,	/* set last access */
		fhisattr_mtime_set		: 1;	/* set last modify */
	mode_t	fhisattr_mode;				/* desired mode */
	uid_t	fhisattr_uid;				/* desired uid */
	gid_t	fhisattr_gid;				/* desired gid */
	off64_t fhisattr_size;				/* desired length */
	time_t	fhisattr_atime;				/* desired access */
	time_t	fhisattr_mtime;				/* desired modify */
};

/*
 * Set an attribute value in the passed buffer.
 */
#define _FHISATTR_SET(_fhisattr, _n, _f, _v) \
	do { \
		(_fhisattr)->fhisattr_##_n##_set = (_f); \
		(_fhisattr)->fhisattr_##_n = (_v); \
	} while (0)

/*
 * Helper macros to set both the mask and value in the file handle info set
 * attribute record.
 */
#define FHISATTR_SET_MODE(_fhisattr, _v) \
	_FHISATTR_SET((_fhisattr), mode, 1, (_v) & 07777)
#define FHISATTR_SET_UID(_fhisattr, _v) \
	_FHISATTR_SET((_fhisattr), uid, 1, (_v))
#define FHISATTR_SET_GID(_fhisattr, _v) \
	_FHISATTR_SET((_fhisattr), gid, 1, (_v))
#define FHISATTR_SET_SIZE(_fhisattr, _v) \
	_FHISATTR_SET((_fhisattr), size, 1, (_v))
#define FHISATTR_SET_ATIME(_fhisattr, _v) \
	_FHISATTR_SET((_fhisattr), atime, 1, (_v))
#define FHISATTR_SET_MTIME(_fhisattr, _v) \
	_FHISATTR_SET((_fhisattr), mtime, 1, (_v))

/*
 * Clear all attribute values in passed file handle info set attribute record
 * buffer.
 */
#define FHISATTR_CLEAR(_fhisattr) \
	do { \
		_FHISATTR_SET((_fhisattr), mode, 0, 0); \
		_FHISATTR_SET((_fhisattr), uid, 0, -1); \
		_FHISATTR_SET((_fhisattr), gid, 0, -1); \
		_FHISATTR_SET((_fhisattr), size, 0, 0); \
		_FHISATTR_SET((_fhisattr), atime, 0, 0); \
		_FHISATTR_SET((_fhisattr), mtime, 0, 0); \
	} while (0);

/*
 * Arguments for file handle information directory operations.
 */
struct file_handle_info_dirop_args {
	struct file_handle_info *fhida_dir;
	const char *fhida_path;
};

extern int SYSIO_INTERFACE_NAME(fhi_export)(const void *key,
					    size_t keylen,
					    const char *source,
					    unsigned flags,
					    struct file_handle_info_export
					     **fhiexpp);
extern int SYSIO_INTERFACE_NAME(fhi_unexport)(struct file_handle_info_export
					       *fhiexp);
extern ssize_t
SYSIO_INTERFACE_NAME(fhi_root_of)(struct file_handle_info_export *fhiexp,
                                  struct file_handle_info *fhi);
extern ssize_t SYSIO_INTERFACE_NAME(fhi_iowait)(ioid_t ioid);
extern int SYSIO_INTERFACE_NAME(fhi_iodone)(ioid_t ioid);
extern int SYSIO_INTERFACE_NAME(fhi_getattr)(struct file_handle_info *fhi,
					     struct stat64 *buf);
extern int SYSIO_INTERFACE_NAME(fhi_setattr)(struct file_handle_info *fhi,
					     struct file_handle_info_sattr
					      *fhisattr);
extern ssize_t SYSIO_INTERFACE_NAME(fhi_lookup)(struct file_handle_info
						 *parent_fhi,
						const char *path,
						unsigned iopmask,
						struct file_handle_info
						 *result);
extern ssize_t SYSIO_INTERFACE_NAME(fhi_readlink)(struct file_handle_info *fhi,
						  char *buf,
						  size_t bufsiz);
extern int SYSIO_INTERFACE_NAME(fhi_iread64x)(struct file_handle_info *fhi,
					      const struct iovec *iov,
					      size_t iov_count,
					      const struct xtvec64 *xtv,
					      size_t xtv_count,
					      ioid_t *ioidp);
extern int SYSIO_INTERFACE_NAME(fhi_iwrite64x)(struct file_handle_info *fhi,
					       const struct iovec *iov,
					       size_t iov_count,
					       const struct xtvec64 *xtv,
					       size_t xtv_count,
					       ioid_t *ioidp);
extern int SYSIO_INTERFACE_NAME(fhi_create)(struct file_handle_info_dirop_args
					     *where,
					    mode_t mode);
extern int SYSIO_INTERFACE_NAME(fhi_unlink)(struct file_handle_info_dirop_args
					     *where);
extern int SYSIO_INTERFACE_NAME(fhi_rename)(struct file_handle_info_dirop_args
					     *from,
					    struct file_handle_info_dirop_args
					     *to);
extern int SYSIO_INTERFACE_NAME(fhi_link)(struct file_handle_info_dirop_args
					   *from,
					  struct file_handle_info_dirop_args
					   *to);
extern int SYSIO_INTERFACE_NAME(fhi_symlink)(const char *from,
					     struct file_handle_info_dirop_args
					      *to);
extern int SYSIO_INTERFACE_NAME(fhi_mkdir)(struct file_handle_info_dirop_args
					    *where,
					   mode_t mode);
extern int SYSIO_INTERFACE_NAME(fhi_rmdir)(struct file_handle_info_dirop_args
					    *where);
extern ssize_t SYSIO_INTERFACE_NAME(fhi_getdirentries64)(struct file_handle_info *fhi,
							 char *buf,
							 size_t nbytes,
							 off64_t
							  * __restrict basep);
extern int SYSIO_INTERFACE_NAME(fhi_statvfs)(struct file_handle_info *fhi,
					     struct statvfs64 *buf);
