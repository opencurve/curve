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
 *    Cplant(TM) Copyright 1998-2014 Sandia Corporation. 
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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/queue.h>
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Copy one file to another.
 *
 * Usage: test_copy [-o] <src> <dest>
 *
 * Destination will not be overwritten if it already exist.
 */

static int overwrite = 0;				/* over-write? */
static unsigned nrqst = 8;
static size_t bufsiz = 64 * 1024;

void	usage(void);
int	copy_file(const char *spath, const char *dpath);

int
main(int argc, char * const argv[])
{
	int	i;
	int	err;
	const char *spath, *dpath;

	/*
	 * Parse command-line args.
	 */
	while ((i = getopt(argc,
			   argv,
			   "o"
			   )) != -1)
		switch (i) {

		case 'o':
			overwrite = 1;
			break;
		default:
			usage();
		}

	if (!(argc - optind))
		usage();
	err = _test_sysio_startup();
	if (err) {
		errno = -err;
		perror("sysio startup");
		exit(1);
	}

	/*
	 * Source
	 */
	spath = argv[optind++];
	if (!(argc - optind))
		usage();
	/*
	 * Destination
	 */
	dpath = argv[optind++];
	if (argc - optind)
		usage();

	err = copy_file(spath, dpath);

	_test_sysio_shutdown();

	return err;
}

void
usage()
{

	(void )fprintf(stderr,
		       "Usage: test_copy "
		       " source destination\n");
	exit(1);
}

int
open_file(const char *path, int flags, mode_t mode)
{
	int	fd;

	fd = SYSIO_INTERFACE_NAME(open)(path, flags, mode);
	if (fd < 0)
		perror(path);

	return fd;
}

static long
gcd(long a, long b)
{
	long	c;

	if (a < 0)
		a = -a;
	if (b < 0)
		b = -b;
	if (a < b) {
		c = a;
		a = b;
		b = c;
	}
	while ((c = a % b)) {
		a = b;
		b = c;
	}
	return b;
}

static long
lcm(long a, long b)
{

	if (a < 0)
		a = -a;
	if (b < 0)
		b = -b;
	a /= gcd(a, b);
	if (LONG_MAX / a < b) {
		errno = ERANGE;
		return -1;
	}
	return a * b;
}

struct request {
	const char *path;
	int	fd;
	char	*buf;
	size_t	len;
	size_t	remain;
	off_t	off;
	ioid_t	ioid;
};

int
copy_file(const char *spath, const char *dpath)
{
	int	sfd, dfd;
	int	flags;
	int	rtn;
	struct stat stat;
	long	l;
	size_t	siz, tmp;
	off_t	tail, nxttail;
	struct request *requests, *rqst;
	unsigned n, u, count, nxtu;
	ssize_t	cc;
	ioid_t	(*f)(int, void *, size_t, off_t);

	sfd = dfd = -1;
	requests = NULL;
	rtn = -1;

	sfd = open_file(spath, O_RDONLY, 0);
	if (sfd < 0)
		goto out;
	flags = O_CREAT|O_WRONLY;
	if (!overwrite)
		flags |= O_EXCL;
	dfd = open_file(dpath, flags, 0666);
	if (dfd < 0)
		goto out;
	rtn = SYSIO_INTERFACE_NAME(fstat)(dfd, &stat);
	if (rtn != 0) {
		perror(dpath);
		goto out;
	}
	l = (long )stat.st_blksize;
	rtn = SYSIO_INTERFACE_NAME(fstat)(sfd, &stat);
	if (rtn != 0) {
		perror(dpath);
		goto out;
	}
	if ((l = lcm(l, (long )stat.st_blksize)) < 0 ||
	    (l = lcm(l, (long )bufsiz)) < 0) {
		perror("Can't calculate required buffer size");
		goto out;
	}
	siz = l;
	if (siz != bufsiz)
		(void )fprintf(stderr, "Buffer size reset to %zu\n", siz);

	requests = malloc(nrqst * sizeof(struct request));
	if (requests == NULL) {
		perror(dpath);
		goto out;
	}
	count = 0;
	tail = 0;
	n = nrqst;
	for (u = 0, rqst = requests; u < n; u++, rqst++) {
		if (!(tail < stat.st_size))
			break;
		rqst->buf = malloc(siz);
		if (rqst->buf == NULL) {
			perror("Cannot allocate request buffer");
			break;
		}
		rqst->fd = sfd;
		rqst->remain = rqst->len = siz;
		rqst->off = tail;
		tail += siz;
		rqst->ioid =
		    SYSIO_INTERFACE_NAME(ipread)(rqst->fd,
						 rqst->buf,
						 siz,
						 rqst->off);
		if (rqst->ioid == IOID_FAIL) {
			perror(spath);
			break;
		}
		count++;
	}
	n = u;
	nxtu = 0;
	while (count) {
		u = nxtu++;
		if (!(nxtu < n))
			nxtu = 0;
		rqst = &requests[u];
		if (rqst->ioid == IOID_FAIL)
			continue;
		if (!SYSIO_INTERFACE_NAME(iodone)(rqst->ioid))
			continue;
		cc = SYSIO_INTERFACE_NAME(iowait)(rqst->ioid);
		count--;
		rqst->ioid = IOID_FAIL;
		if (cc < 0) {
			perror(rqst->fd == sfd ? spath : dpath);
			continue;
		}
		nxttail = tail;
		rqst->remain -= (size_t )cc;
		if (rqst->fd == dfd) {
			do {
				if (rqst->remain)
					break;
				rqst->fd = sfd;
				rqst->remain = rqst->len = siz;
				rqst->off = tail;
				nxttail += siz;
			} while (0);
			if (!(rqst->off < stat.st_size)) {
				assert(rqst->fd == sfd);
				continue;
			}
		} else {
			assert(rqst->fd == sfd);
			if (!(cc || rqst->len - rqst->remain))
				continue;
			do {
				if (rqst->remain && cc)
					break;
				rqst->fd = dfd;
				rqst->remain = rqst->len = siz - rqst->remain;
			} while (0);
		}
		tmp = rqst->len - rqst->remain;
		if (rqst->fd == sfd)
			f = 
			    (ioid_t (*)(int,
					void *,
					size_t,
					off_t))SYSIO_INTERFACE_NAME(ipread);
		else
			f = 
			    (ioid_t (*)(int,
					void *,
					size_t,
					off_t))SYSIO_INTERFACE_NAME(ipwrite);
		rqst->ioid =
		    (*f)(rqst->fd,
		    	 (char *)rqst->buf + tmp,
			 rqst->remain,
			 rqst->off + tmp);
		if (rqst->ioid == IOID_FAIL) {
			perror(rqst->fd == sfd ? spath : dpath);
assert(0);
			continue;
		}
		tail = nxttail;
		count++;
	}

out:
	if (requests != NULL) {
		for (u = 0, rqst = requests; u < n; u++, rqst++) {
			assert(rqst->ioid == IOID_FAIL);
			free(rqst->buf);
		}
		free(requests);
	}
	if (sfd >= 0 && SYSIO_INTERFACE_NAME(close)(sfd) != 0)
		perror(spath);
	if (dfd >= 0 &&
	    (SYSIO_INTERFACE_NAME(fsync)(dfd) != 0 ||
	     SYSIO_INTERFACE_NAME(close)(dfd) != 0))
		perror(dpath);

	return 0;
}
