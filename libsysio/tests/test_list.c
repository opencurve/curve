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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/uio.h>
#include <sys/queue.h>
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Stat files.
 *
 * Usage: test_list [path...]
 *
 * Without any path arguments, the program reads from standard-in, dealing with
 * each line as an absolute or relative path until EOF.
 */

static int listit(const char *path);
static void usage(void);

int
main(int argc, char *const argv[])
{
	int	i;
	int	err;
	int	n;
	extern int _test_sysio_startup(void);

	/*
	 * Parse command line arguments.
	 */
	while ((i = getopt(argc, argv, "")) != -1)
		switch (i) {

		default:
			usage();
		}

	/*
	 * Init sysio lib.
	 */
	err = _test_sysio_startup();
	if (err) {
		errno = -err;
		perror("sysio startup");
		exit(1);
	}

	n = argc - optind;

	/*
	 * Try path(s) listed on command-line.
	 */
	while (optind < argc) {
		const char *path;

		path = argv[optind++];
		(void )listit(path);
	}

	/*
	 * If no command-line arguments, read from stdin until EOF.
	 */
	if (!n) {
		int	doflush;
		static char buf[4096];
		size_t	len;
		char	*cp;
		char	c;

		doflush = 0;
		while (fgets(buf, sizeof(buf), stdin) != NULL) {
			len = strlen(buf);
			cp = buf + len - 1;
			c = *cp;
			*cp = '\0';
			if (!doflush)
				listit(buf);
			doflush = c == '\n' ? 0 : 1;
		}
	}

	/*
	 * Clean up.
	 */
	_test_sysio_shutdown();

	return 0;
}

static int
listit(const char *path)
{
	int	fd;
	size_t	n;
	struct dirent *buf, *dp;
	off_t	base = 0;
	ssize_t	cc;

	fd = SYSIO_INTERFACE_NAME(open)(path, O_RDONLY);
	if (fd < 0) {
		perror(path);
		return -1;
	}

	n = 16 * 1024;
	buf = malloc(n);
	if (!buf) {
		perror(path);
		cc = -1;
		goto out;
	}

	while ((cc = SYSIO_INTERFACE_NAME(getdirentries)(fd,
							 (char *)buf,
							 n,
							 &base)) > 0) {
		dp = buf;
		while (cc > 0) {
			(void )printf("\t%s: ino %llu type %u\n",
				      dp->d_name,
				      (unsigned long long )dp->d_ino,
				      (int )dp->d_type);
			cc -= dp->d_reclen;
			dp = (struct dirent *)((char *)dp + dp->d_reclen);
		}
	}

out:
	if (cc < 0)
		perror(path);

	free(buf);
	{
		int	oerrno = errno;

		if (SYSIO_INTERFACE_NAME(close)(fd) != 0) {
			perror(path);
			if (cc < 0)
				errno = oerrno;
			else
				cc = -1;
		}
	}

	return (int )cc;
}

static void
usage()
{

	(void )fprintf(stderr,
		       "Usage: list_path"
		       " [<path> ...\n]");

	exit(1);
}
