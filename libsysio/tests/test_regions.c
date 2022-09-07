/*
 *    This Cplant(TM) source code is the property of Sandia National
 *    Laboratories.
 *
 *    This Cplant(TM) source code is regionsrighted by Sandia National
 *    Laboratories.
 *
 *    The redistribution of this Cplant(TM) source code is subject to the
 *    terms of the GNU Lesser General Public License
 *    (see cit/LGPL or http://www.gnu.org/licenses/lgpl.html)
 *
 *    Cplant(TM) Copyright 1998-2004 Sandia Corporation. 
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
 * You should have received a regions of the GNU Lesser General Public
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
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Copy one file to another.
 *
 * Usage: test_regions [-x] [-e] \
 * 		{r,w} <off> <count> <path>
 *
 * Destination will not be overwritten if it already exist.
 */

#if defined(_LARGEFILE64_SOURCE)
#define	GO64
#else
#warning Cannot prompt the 64-bit interface
#endif

char	which;
#ifdef GO64
int	use64 = 0;					/* 64-bit interface? */
#endif

void	usage(void);

int
main(int argc, char * const argv[])
{
	int	i;
	int	keep;
	int	err;
	long	l;
	off_t	off;
#ifdef GO64
	long long ll;
	off64_t	off64;
#endif
	char	*cp;
	unsigned long nbytes;
	const char *path;
	char	*buf;
	int	flags;
	int	fd;
	ssize_t	cc;
	extern int _test_sysio_startup(void);

	/*
	 * Parse command-line args.
	 */
	keep = 0;
	while ((i = getopt(argc,
			   argv,
#ifdef __GLIBC__
			   "+"
#endif
#ifdef GO64
			   "x"
#endif
			   "e")) != -1)
		switch (i) {

#ifdef GO64
		case 'x':
			use64 = 1;
			break;
#endif
		case 'e':
			keep = 1;
			break;
		default:
			usage();
		}

	if (argc - optind != 4)
		usage();

	which = *argv[optind];
	if (strlen(argv[optind]) != 1 || !(which == 'r' || which == 'w')) {
		(void )fprintf(stderr, "Which op?\n");
		exit(1);
	}
	optind++;
	off = l =
#ifdef GO64
	    ll = strtoll(argv[optind++], &cp, 0);
#else
	    strtol(argv[optind++], &cp, 0);
#endif
#ifdef GO64
	off64 = ll;
#endif
	if (*cp != '\0' ||
#ifdef GO64
	    ((ll == LLONG_MIN || ll == LLONG_MAX) && errno == ERANGE) ||
	    off64 != ll || (!use64 && off != ll)
#else
	    ((l == LONG_MIN || l == LONG_MAX) && errno == ERANGE) ||
	    off != l
#endif
	   ) {
		(void )fprintf(stderr, "Offset out of range\n");
		exit(1);
	}
	nbytes = strtoul(argv[optind++], &cp, 0);
	if (*cp != '\0' || (nbytes == ULONG_MAX && errno == ERANGE)) {
		(void )fprintf(stderr, "Transfer count out of range\n");
		exit(1);
	}
	if (!(argc - optind))
		usage();
	path = argv[optind++];

	err = _test_sysio_startup();
	if (err) {
		errno = -err;
		perror("sysio startup");
		exit(1);
	}	

	(void )umask(022);

	buf = malloc(nbytes);
	if (!buf) {
		perror("malloc");
		err = 1;
		goto out;
	}
	(void )memset(buf, 0, nbytes);

	err = 0;
	flags = 0;
	if (!keep)
		flags |= O_CREAT|O_EXCL;
	flags |= which == 'r' ? O_RDONLY : O_WRONLY;
#ifdef GO64
	if (use64)
		flags |= O_LARGEFILE;
#endif
	fd = SYSIO_INTERFACE_NAME(open)(path, flags, 0666);
	if (fd < 0) {
		perror(path);
		err = 1;
		goto error;
	}
#ifdef GO64
	if (use64)
		off64 = SYSIO_INTERFACE_NAME(lseek64)(fd, off64, SEEK_SET);
	else
		off64 =
#endif
		  off = SYSIO_INTERFACE_NAME(lseek)(fd, off, SEEK_SET);
#ifdef GO64
	if ((use64 && off64 < 0) || (!use64 && off < 0)) {
		perror(use64 ? "lseek64" : "lseek");
		err = 1;
		goto error;
	}
#else
	if (off < 0) {
		perror("lseek");
		err = 1;
		goto error;
	}
#endif
	if (which == 'r')
		cc = SYSIO_INTERFACE_NAME(read)(fd, buf, nbytes);
	else
		cc = SYSIO_INTERFACE_NAME(write)(fd, buf, nbytes);
	if (cc < 0) {
		perror(path);
		err = 1;
		goto error;
	}
#ifdef GO64
	if (use64) {
		off64 = SYSIO_INTERFACE_NAME(lseek64)(fd, 0, SEEK_CUR);
	} else
		off64 =
#endif
		  off = SYSIO_INTERFACE_NAME(lseek)(fd, 0, SEEK_CUR);
	(void )printf(("%s%s@"
#ifdef GO64
		       "%lld"
#else
		       "%ld"
#endif
		       ": %ld, off "
#ifdef GO64
		       "%lld"
#else
		       "%ld"
#endif
		       "\n"),
		      which == 'r' ? "read" : "write",
#ifdef GO64
		      use64 ? "64" : "",
		      ll,
#else
		      "",
		      l,
#endif
		      (long )cc,
#ifdef GO64
		      (long long int)off64
#else
		      off
#endif
		      );

error:
	if (fd > 0 && SYSIO_INTERFACE_NAME(close)(fd) != 0)
		perror(path);
	free(buf);
out:
	_test_sysio_shutdown();

	return err;
}

void
usage()
{

	(void )fprintf(stderr,
		       "Usage: test_regions "
#ifdef GO64
		       "[-x] "
#endif
		       " {r,w} <offset> <nbytes> <path>\n");
	exit(1);
}
