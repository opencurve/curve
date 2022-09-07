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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <getopt.h>
#include <dirent.h>
#include <sys/types.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Test {open, read, close}dir functions
 * 
 * Usage: test_stddir [path, ...]
 */
static int testit(const char *);
static void usage(void);

int 
main (int argc, char** argv)
{
	int	err;
	int	i;
	int	n;
	const char *path;

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

	/*
	 * If no command-line arguments, read from stdin until EOF.
	 */
	n = argc - optind;
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
				err = testit(buf);
			if (err)
				break;
			doflush = c == '\n' ? 0 : 1;
		}
	}

	/*
	 * Try path(s) listed on command-line.
	 */
	while (optind < argc) {
		path = argv[optind++];
		err = testit(path);
		if (err)
			break;
	}

	/*
	 * Clean up.
	 */
	_test_sysio_shutdown();

	return err;
}

int
testit(const char *path)
{
	DIR 	*d;
	struct dirent *de;

	printf("testing directory functions on %s\n", path);

	if ((d = SYSIO_INTERFACE_NAME(opendir)(path)) == NULL) {
		perror(path);	
		return errno;
	}

	while ((de = SYSIO_INTERFACE_NAME(readdir)(d)) != NULL)
		printf("\t %s: ino %lu off %lu type %u\n",
			de->d_name, (unsigned long )de->d_ino, 
			(unsigned long )de->d_off, (int )de->d_type);

	if (SYSIO_INTERFACE_NAME(closedir)(d)) {
		perror("closedir");
		return errno;
	}

	return 0;
}

static void
usage()
{

	(void )fprintf(stderr,
		       "Usage: test_stddir [<path> ...]\n");

	exit(1);
}
