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
#include <sys/uio.h>
#include <sys/queue.h>
#include <dirent.h>
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "mount.h"

#include "test.h"

/*
 * Test getcwd()
 *
 * Usage: test_cwd [-v] [<working-dir>...]
 *
 * Without any path arguments, the program reads from standard-in, dealing with
 * each line as an absolute or relative path until EOF.
 *
 * The -v option tells it to chdir back up tot he root and print the
 * working directory at each point.
 */

static int verify = 0;					/* verify path? */

static int doit(const char *path);
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
	while ((i = getopt(argc, argv, "v")) != -1)
		switch (i) {

		case 'v':
			verify = 1;
			break;
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
		(void )doit(path);
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
				doit(buf);
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
doit(const char *path)
{
	char	*buf;
	struct stat stbufs[2], *st1, *st2;
	unsigned count;

	if (SYSIO_INTERFACE_NAME(chdir)(path) != 0) {
		perror(path);
		return -1;
	}
	st1 = &stbufs[0];
	if (SYSIO_INTERFACE_NAME(stat)(".", st1) != 0) {
		perror(".");
		return -1;
	}
	count = 0;
	do {
		buf = SYSIO_INTERFACE_NAME(getcwd)(NULL, 0);
		if (!buf) {
			perror(path);
			return -1;
		}
		(void )printf("%s\n", buf);
		free(buf);
		if (SYSIO_INTERFACE_NAME(chdir)("..") != 0) {
			perror("..");
			return -1;
		}
		count++;
		st2 = st1;
		st1 = &stbufs[count & 1];
		if (SYSIO_INTERFACE_NAME(stat)(".", st1) != 0) {
			perror(".");
			return -1;
		}
	} while (verify &&
		 (st1->st_dev != st2->st_dev || st1->st_ino != st2->st_ino));
	return 0;
}

static void
usage()
{

	(void )fprintf(stderr,
		       "Usage: test_getcwd "
		       " [<path> [...]]\n");

	exit(1);
}
