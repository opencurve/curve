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
 *    Cplant(TM) Copyright 1998-2007 Sandia Corporation. 
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
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Test chown call
 *
 * Usage: chown <path> <uid> <gid>
 *
 */

static void usage(void);

int
main(int argc, char *const argv[])
{
	int	(*chown_func)(const char *, uid_t, gid_t);
	int	(*stat_func)(const char *, struct stat *);
	int	i;
	int	err;
	int	n;
	char	*path;
	uid_t	uid;
	gid_t	gid;
	struct stat stbuf;
	extern int _test_sysio_startup(void);

	chown_func = SYSIO_INTERFACE_NAME(chown);
	stat_func = SYSIO_INTERFACE_NAME(stat);

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
	if (n < 3) usage();

	path = argv[optind++];
	uid = atoi(argv[optind++]);
	gid = atoi(argv[optind++]);

	do {
		err = (*chown_func)(path, uid, gid);
		if (err != 0) {
			perror(path);
			break;
		}
		err = (*stat_func)(path, &stbuf);
		if (err != 0) {
			perror(path);
			break;
		}
		(void )printf("uid now %ld, gid now %ld\n",
			      (long )stbuf.st_uid, (long )stbuf.st_gid);
	} while (0);

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
		       "Usage: chown"
		       " <path> <uid> <gid>\n");

	exit(1);
}
