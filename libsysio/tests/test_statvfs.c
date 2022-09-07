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
 * Albuquerque, NM 87185-1319
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
#include <sys/statvfs.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "test.h"

/*
 * Get stats of file and file system.
 *
 * Usage: test_statvfs [<path> ...]
 */

void	usage(void);
void	do_statvfs(const char *path);

int
main(int argc, char * const argv[])
{
	int	i;
	int	err;
	extern int _test_sysio_startup(void);

	/*
	 * Parse command-line args.
	 */
	while ((i = getopt(argc, argv, "")) != -1)
		switch (i) {

		default:
			usage();
		}

	err = _test_sysio_startup();
	if (err) {
		errno = -err;
		perror("sysio startup");
		exit(1);
	}	

	(void )SYSIO_INTERFACE_NAME(umask)(022);

	while (optind < argc)
		do_statvfs(argv[optind++]);

	/*
	 * Clean up.
	 */
	_test_sysio_shutdown();

	return 0;
}

void
usage()
{

	(void )fprintf(stderr,
		       "Usage: test_statvfs"
		       " <path> ...\n");
	exit(1);
}

void
do_statvfs(const char *path)
{
	int	fd;
	int	err;
	struct statvfs stvfsbuf1, stvfsbuf2;

	fd = SYSIO_INTERFACE_NAME(open)(path, O_RDONLY);
	if (fd < 0) {
		perror(path);
		return;
	}
	err = SYSIO_INTERFACE_NAME(fstatvfs)(fd, &stvfsbuf1);
	if (!err)
		err = SYSIO_INTERFACE_NAME(statvfs)(path, &stvfsbuf2);
	else
		fd = -1;
	if (err)
		perror(path);
	if (fd >= 0 && SYSIO_INTERFACE_NAME(close)(fd) != 0)
		perror(path);
	if (err)
		return;
	if (stvfsbuf1.f_fsid != stvfsbuf2.f_fsid) {
		(void )fprintf(stderr, "%s: [f]statvfs info mismatch\n", path);
		return;
	}
	printf("%s:"
	       " bsize %lu,"
	       " frsize %lu,"
	       " blocks %llu,"
	       " bfree %llu,"
	       " bavail %llu,"
	       " files %llu,"
	       " ffree %llu,"
	       " favail %llu,"
	       " fsid %lu,"
	       " flag %lu,"
	       " namemax %lu,"
	       "\n",
	       path,
	       (unsigned long )stvfsbuf1.f_bsize,
	       (unsigned long )stvfsbuf1.f_frsize,
	       (unsigned long long )stvfsbuf1.f_blocks,
	       (unsigned long long )stvfsbuf1.f_bfree,
	       (unsigned long long )stvfsbuf1.f_bavail,
	       (unsigned long long )stvfsbuf1.f_files,
	       (unsigned long long )stvfsbuf1.f_ffree,
	       (unsigned long long)stvfsbuf1.f_favail,
	       (unsigned long )stvfsbuf1.f_fsid,
	       (unsigned long )stvfsbuf1.f_flag,
	       (unsigned long)stvfsbuf1.f_namemax);
}
