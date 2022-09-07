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

#ifndef CURVEFS_TEST_INCLUDE
#define CURVEFS_TEST_INCLUDE

//#define FUSE_USE_VERSION 34

#include <stdio.h>

#include "fs_curve.h"
#include "fuse_lowlevel.h"

/* use poll to improve the request latency */
//#define CURVEFS_CLIENT_POLL
#define CURVEFS_SERVER_CO_POOL

#define NUMBER_OF_NOTIFICATIONS 1

//#define CURVEFS_TEST_DEBUG

#ifdef CURVEFS_TEST_DEBUG

#define CURVEFS_FUSE_PRINTF(level, fmt, ...) 	\
	do {									\
		fuse_log (level, " file=%s func=%s line=%d ", __FILE__, __func__, __LINE__); 	\
		fuse_log (level, fmt, ##__VA_ARGS__);	\
	} while (0)

#else

#define CURVEFS_FUSE_PRINTF(level, fmt, ...) 	\
	do { } while (0)

#endif

struct lo_inode {
	struct lo_inode *next; /* protected by lo->mutex */
	struct lo_inode *prev; /* protected by lo->mutex */
	int fd;
	ino_t ino;
	dev_t dev;
	uint64_t refcount; /* protected by lo->mutex */
};

enum {
	CACHE_NEVER,
	CACHE_NORMAL,
	CACHE_ALWAYS,
};

struct lo_data {
	pthread_mutex_t mutex;
	int debug;
	int writeback;
	int flock;
	int xattr;
	const char *source;
	double timeout;
	int cache;
	int timeout_set;
	struct lo_inode root; /* protected by lo->mutex */
};

extern const struct fuse_lowlevel_ops lo_oper;
extern const struct fuse_opt lo_opts[];

extern int (*drvinits[])(void);

extern int drv_init_all(void);

int curvefs_session_mount(struct fuse_session *se, const char *mountpoint);

extern int _test_curvefs_sysio_startup(void);

extern void _test_curvefs_sysio_shutdown(void);

extern void send_curvefs_request(void* msg);

#ifdef CURVEFS_SERVER_CO_POOL

typedef  void* (*CO_FUNC_t) (void*);

extern void co_pool_create(stCoRoutine_t** co, CO_FUNC_t func, void* args);

#endif

#endif
