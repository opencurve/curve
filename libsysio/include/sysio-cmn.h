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

#ifndef SYSIO_CMN_H_INCLUDE
#define SYSIO_CMN_H_INCLUDE

#include <stdio.h>
#include "smp.h"

/*
 * System IO common information.
 */

#if !defined(__IS_UNUSED) && defined(__GNUC__)
#define __IS_UNUSED	__attribute__ ((unused))
#else
#define __IS_UNUSED
#endif

/*
 * Define internal file-offset type and it's maximum value.
 */
#ifdef _LARGEFILE64_SOURCE
#define _SYSIO_OFF_T			off64_t
#ifdef LLONG_MAX
#define _SYSIO_OFF_T_MAX		(LLONG_MAX)
#else
/*
 * Don't have LLONG_MAX before C99. We'll need to define it ourselves.
 */
#define _SYSIO_OFF_T_MAX		(9223372036854775807LL)
#endif
#else
#define _SYSIO_OFF_T			off_t
#define _SYSIO_OFF_T_MAX		LONG_MAX
#endif

/*
 * Internally, all file status is carried in the 64-bit capable
 * structure.
 */
#ifdef _LARGEFILE64_SOURCE
#define intnl_xtvec xtvec64
#else
#define intnl_xtvec xtvec
#endif
struct intnl_xtvec;

struct iovec;

/*
 * Return offset to given member of given structure or union.
 */
#define OFFSETOF(type, _mbr) \
	((size_t )&((struct type *)0)->_mbr)

/*
 * Return ptr to record containing the passed member.
 */
#define CONTAINER(type, _mbr, ptr) \
	(struct type *)((ptrdiff_t )(ptr) - OFFSETOF(type, _mbr))

/*
 * Symbol composition.
 */
#define _PREPEND_HELPER(p, x) \
	p ## x
#define PREPEND(p, x) \
	_PREPEND_HELPER(p, x)

/*
 * SYSIO name label macros
 */
#ifdef SYSIO_LABEL_NAMES
#define SYSIO_INTERFACE_NAME(x) \
	PREPEND(SYSIO_LABEL_NAMES, x)
#else
#define SYSIO_INTERFACE_NAME(x) x
#endif

/* for debugging */
#if 0
#define ASSERT(cond)							\
	if (!(cond)) {							\
		printf("ASSERTION(" #cond ") failed: " __FILE__ ":"	\
			__FUNCTION__ ":%d\n", __LINE__);		\
		abort();						\
	}

#define ERROR(fmt, a...)						\
	do {								\
		printf("ERROR(" __FILE__ ":%d):" fmt, __LINE__, ##a);	\
	while(0)

#else
#define ERROR(fmt) 	do{}while(0)
#define ASSERT		do{}while(0)
#endif

/*
 * SYSIO interface frame macros
 *
 * + DISPLAY_BLOCK; Allocates storage on the stack for use by the set of
 *	macros.
 * + ENTER; Performs entry point work
 * + RETURN; Returns a value and performs exit point work
 *
 * NB: For RETURN, the arguments are the return value and value for errno.
 * If the value for errno is non-zero then that value, *negated*, is set
 * into errno.
 */
#define SYSIO_INTERFACE_DISPLAY_BLOCK \
	int _saved_errno;
#define SYSIO_INTERFACE_ENTER(tag, fmt, ...) \
	do { \
		_saved_errno = errno; \
		SYSIO_ENTER(tag, (fmt), __VA_ARGS__); \
	} while (0)
#define SYSIO_INTERFACE_RETURN(rtn, err, tag, fmt, ...) \
	do { \
		SYSIO_LEAVE(tag, (fmt), (err), (rtn), __VA_ARGS__); \
		errno = (err) ? -(err) : _saved_errno; \
		return (rtn); \
	} while(0) 

//#define CURVEFS_SYSIO_TRACING
#ifdef CURVEFS_SYSIO_TRACING 
/* Trace print functions */
#define CURVEFS_DPRINTF(fmt, ...)	\
	do {							\
		printf(" file=%s func=%s line=%d ", __FILE__, __func__, __LINE__);	\
		printf(fmt, ##__VA_ARGS__);	\
	} while (0)

#else

#define CURVEFS_DPRINTF(fmt, ...)						\
	do { } while (0)

#endif
#ifdef SYSIO_TRACING

#define SYSIO_TTAG(name) \
	PREPEND(_SYSIO_TRACING_, name)

/*
 * Supported tracing tags.
 */
typedef enum {
	SYSIO_TTAG(unsupported)	= -1,
	SYSIO_TTAG(access),
	SYSIO_TTAG(chdir),
	SYSIO_TTAG(getcwd),
	SYSIO_TTAG(chmod),
	SYSIO_TTAG(fchmod),
	SYSIO_TTAG(chown),
	SYSIO_TTAG(fchown),
	SYSIO_TTAG(dup2),
	SYSIO_TTAG(dup),
	SYSIO_TTAG(vfcntl),
	SYSIO_TTAG(ifsync),
	SYSIO_TTAG(ifdatasync),
	SYSIO_TTAG(getdirentries),
#ifdef _LARGEFILE64_SOURCE
	SYSIO_TTAG(getdirentries64),
#endif
	SYSIO_TTAG(ioctl),
	SYSIO_TTAG(iodone),
	SYSIO_TTAG(iowait),
	SYSIO_TTAG(link),
	SYSIO_TTAG(lseek),
#ifdef _LARGEFILE64_SOURCE
	SYSIO_TTAG(lseek64),
#endif
	SYSIO_TTAG(llseek),
	SYSIO_TTAG(mkdir),
	SYSIO_TTAG(xmknod),
	SYSIO_TTAG(mount),
	SYSIO_TTAG(umount),
	SYSIO_TTAG(open),
	SYSIO_TTAG(close),
	SYSIO_TTAG(ireadx),
	SYSIO_TTAG(iwritex),
	SYSIO_TTAG(ireadv),
	SYSIO_TTAG(iwritev),
	SYSIO_TTAG(rename),
	SYSIO_TTAG(rmdir),
	SYSIO_TTAG(xstatnd),
	SYSIO_TTAG(fxstat),
	SYSIO_TTAG(statvfs),
	SYSIO_TTAG(fstatvfs),
#ifdef _LARGEFILE64_SOURCE
	SYSIO_TTAG(statvfs64),
	SYSIO_TTAG(fstatvfs64),
#endif
	SYSIO_TTAG(opendir),
	SYSIO_TTAG(closedir),
	SYSIO_TTAG(symlink),
	SYSIO_TTAG(readlink),
	SYSIO_TTAG(truncate),
	SYSIO_TTAG(ftruncate),
	SYSIO_TTAG(unlink),
	SYSIO_TTAG(utime),
#ifdef FILE_HANDLE_INTERFACE
	SYSIO_TTAG(fhi_export),
	SYSIO_TTAG(fhi_unexport),
	SYSIO_TTAG(fhi_root_of),
	SYSIO_TTAG(fhi_iowait),
	SYSIO_TTAG(fhi_getattr),
	SYSIO_TTAG(fhi_setattr),
	SYSIO_TTAG(fhi_lookup),
	SYSIO_TTAG(fhi_readlink),
	SYSIO_TTAG(fhi_read64x),
	SYSIO_TTAG(fhi_write64x),
	SYSIO_TTAG(fhi_create),
	SYSIO_TTAG(fhi_unlink),
	SYSIO_TTAG(fhi_rename),
	SYSIO_TTAG(fhi_link),
	SYSIO_TTAG(fhi_symlink),
	SYSIO_TTAG(fhi_mkdir),
	SYSIO_TTAG(fhi_rmdir),
	SYSIO_TTAG(fhi_getdirentries64),
	SYSIO_TTAG(fhi_statvfs64),
#endif
} tracing_tag;

extern void *_sysio_initializer_trace_q;
extern void *_sysio_entry_trace_q;
extern void *_sysio_exit_trace_q;

extern int _sysio_trace_init(void);
extern void _sysio_trace_shutdown(void);

extern void *_sysio_register_trace(void *q,
				   void (*)(const char *file,
					    const char *func,
					    int line,
					    void *data,
					    tracing_tag tag,
					    const char *fmt,
					    va_list ap),
				   void *data,
				   void (*destructor)(void *data));
extern void _sysio_remove_trace(void *q, void *p);
extern void _sysio_run_trace_q(void *q,
			       const char *file,
			       const char *func,
			       int line,
			       tracing_tag tag,
			       const char *fmt,
			       ...);

#if 0
/* Trace print functions */
#define CURVEFS_DPRINTF(fmt, ...)	\
	do {							\
		printf(" file=%s func=%s line=%d ", __FILE__, __func__, __LINE__);	\
		printf(fmt, ##__VA_ARGS__);	\
	} while (0)

#endif

/* Trace enter/leave hook functions  */
#define _SYSIO_TRACE_ENTER(tag, fmt, ...)				\
	do { \
		_sysio_run_trace_q(_sysio_entry_trace_q,		\
				   __FILE__, __func__, __LINE__,	\
				   SYSIO_TTAG(tag),			\
				   (fmt),				\
				   __VA_ARGS__);			\
	} while (0)

#define _SYSIO_TRACE_LEAVE(tag, fmt, ...)				\
	do { \
		_sysio_run_trace_q(_sysio_exit_trace_q,			\
				   __FILE__, __func__, __LINE__,	\
				   SYSIO_TTAG(tag),			\
				   (fmt),				\
				   __VA_ARGS__);			\
	} while (0)

#else /* !defined(SYSIO_TRACING) */
#if 0
#define CURVEFS_DPRINTF(fmt, ...)						\
	do { } while (0)
#endif

#define _SYSIO_TRACE_ENTER(tag, fmt, ...)				\
	do { } while (0)
#define _SYSIO_TRACE_LEAVE(tag, fmt, ...)				\
	do { } while (0)
#endif /* !defined(SYSIO_TRACING) */

extern mutex_t _sysio_biglock;

/* Interface enter/leave hook functions  */
#define SYSIO_ENTER(tag, fmt, ...)					\
	do {								\
		mutex_lock(&_sysio_biglock);				\
		_SYSIO_TRACE_ENTER(tag, fmt, __VA_ARGS__);		\
	} while (0)
#define SYSIO_LEAVE(tag, fmt, ...)					\
	do {								\
		_SYSIO_TRACE_LEAVE(tag, fmt, __VA_ARGS__);		\
		mutex_unlock(&_sysio_biglock);				\
	} while (0)

/* Accounting for IO stats; Read and write character count. */
#if defined(REDSTORM)
#define _SYSIO_UPDACCT(w, cc) \
	do { \
		if ((cc) < 0) \
			break; \
		if (w) \
			_add_iostats(0, (size_t )(cc)); \
		else \
			_add_iostats((size_t )(cc), 0); \
	} while(0)
#else
#define _SYSIO_UPDACCT(w, cc)
#endif

extern ssize_t _sysio_validx(const struct intnl_xtvec *xtv, size_t xtvlen,
			     const struct iovec *iov, size_t iovlen,
			     _SYSIO_OFF_T limit);
extern ssize_t _sysio_enumerate_extents(const struct intnl_xtvec *xtv,
					size_t xtvlen,
					const struct iovec *iov,
					size_t iovlen,
					ssize_t (*f)(const struct iovec *,
						     int,
						     _SYSIO_OFF_T,
						     ssize_t,
						     void *),
					void *arg);
extern ssize_t _sysio_enumerate_iovec(const struct iovec *iov,
				      size_t count,
				      _SYSIO_OFF_T off,
				      ssize_t limit,
				      ssize_t (*f)(void *,
						   size_t,
						   _SYSIO_OFF_T,
						   void *),
				      void *arg);
extern ssize_t _sysio_doio(const struct intnl_xtvec *xtv, size_t xtvlen,
			   const struct iovec *iov, size_t iovlen,
			   ssize_t (*f)(void *, size_t, _SYSIO_OFF_T, void *),
			   void *arg);


#endif