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
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/uio.h>
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Stat files.
 *
 * Usage: test_path [path...]
 *
 * Without any path arguments, the program reads from standard-in, dealing with
 * each line as an absolute or relative path until EOF.
 */

static int statit(const char *path);
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
		(void )statit(path);
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
				statit(buf);
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
statit(const char *path)
{
	int	err;
	struct stat stbuf;
	char	t;
	static char buf[4096];
	ssize_t	cc;

	/*
	 * Get file attrs.
	 */
	err = SYSIO_INTERFACE_NAME(lstat)(path, &stbuf);
	if (err) {
		perror(path);
		return -1;
	}

	/*
	 * Get readable representation of file type.
	 */
	if (S_ISDIR(stbuf.st_mode))
		t = 'd';
	else if (S_ISCHR(stbuf.st_mode))
		t = 'c';
	else if (S_ISBLK(stbuf.st_mode))
		t = 'b';
	else if (S_ISREG(stbuf.st_mode))
		t = 'f';
#ifdef S_ISFIFO
	else if (S_ISFIFO(stbuf.st_mode))
		t = 'p';
#endif
#ifdef S_ISLNK
	else if (S_ISLNK(stbuf.st_mode))
		t = 'S';
#endif
#ifdef S_ISSOCK
	else if (S_ISSOCK(stbuf.st_mode))
		t = 's';
#endif
#ifdef S_TYPEISMQ
	else if (S_TYPEISMQ(&stbuf))
		t = 'q';
#endif
#ifdef S_TYPEISSEM
	else if (S_TYPEISSEM(&stbuf))
		t = 'M';
#endif
#ifdef S_TYPEISSHM
	else if (S_TYPEISSHM(&stbuf))
		t = 'm';
#endif
	else
		t = '?';

	/*
	 * Print path and type.
	 */
	cc = 0;
	if (S_ISLNK(stbuf.st_mode)) {
		cc = SYSIO_INTERFACE_NAME(readlink)(path, buf, sizeof(buf));
		if (cc < 0) {
			perror(path);
			return -1;
		}
	}
	(void )printf("%s: %c", path, t);
	if (S_ISLNK(stbuf.st_mode) && (size_t )cc < sizeof(buf))
		(void )printf(" %.*s", (int )cc, buf);
	(void )putchar('\n');

	return 0;
}

static void
usage()
{

	(void )fprintf(stderr,
		       "Usage: test_path"
		       " [<path> ...\n]");

	exit(1);
}
