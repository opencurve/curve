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
 *    Cplant(TM) Copyright 1998-2006 Sandia Corporation. 
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
#if 0
#include <dirent.h>
#endif
#include <sys/uio.h>
#include <sys/queue.h>
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Test soft links
 *
 * Usage: symlink oldpath newpath
 *
 */

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
	if (n < 2) usage();

	/*
	 * Try paths listed on command-line.
	 */
	while (optind < argc) {
		const char *old, *new;
		struct stat stbuf;

		old = argv[optind++];
		new = argv[optind++];
		if ((err = SYSIO_INTERFACE_NAME(symlink)(old, new)) != 0) {
			perror("link");
			break;
		}
		if ((err = SYSIO_INTERFACE_NAME(lstat)(new, &stbuf)) != 0) {
			perror(new);
			break;
		}
	}

	/*
	 * Clean up.
	 */
	_test_sysio_shutdown();

	return err ? -1 : 0;
}

static void
usage()
{

	(void )fprintf(stderr,
		       "Usage: symlink"
		       " oldpath newpath\n");

	exit(1);
}
