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
#ifdef notdef
#include <sys/statvfs.h>
#endif
#include <sys/uio.h>
#include <getopt.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"

/*
 * Get stats of file and file system.
 *
 * Usage: test_stats [<path> ...]
 */

void	usage(void);
void	do_stats(const char *path);

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

	if (optind < argc) {
		while (optind < argc)
			do_stats(argv[optind++]);
	} else {
		int     doflush;
		static char buf[4096];
		size_t  len;
		char    *cp;
		char    c;

		doflush = 0;
		while (fgets(buf, sizeof(buf), stdin) != NULL) {
			len = strlen(buf);
			cp = buf + len - 1;
			c = *cp;
			*cp = '\0';
			if (!doflush)
				do_stats(buf);
			doflush = c == '\n' ? 0 : 1;
		}
	}

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
		       "Usage: test_stats"
		       " source destination\n");
	exit(1);
}

void
do_stats(const char *path)
{
	int	fd;
	int	err;
	struct stat stbuf1, stbuf2;
#ifdef  notdef
	struct statvfs stvfsbuf1, stvfsbuf2;
#endif

	fd = SYSIO_INTERFACE_NAME(open)(path, O_RDONLY);
	if (fd < 0) {
		perror(path);
		return;
	}
	err = SYSIO_INTERFACE_NAME(fstat)(fd, &stbuf1);
	if (!err)
		err = SYSIO_INTERFACE_NAME(stat)(path, &stbuf2);
#ifdef notdef
	if (!err)
		err = SYSIO_INTERFACE_NAME(fstatvfs)(fd, &stvfsbuf1);
	if (!err)
		err = SYSIO_INTERFACE_NAME(statvfs)(path, &stvfsbuf1);
#endif
	if (err) {
		perror(path);
		goto out;
	}
	if (stbuf1.st_dev != stbuf2.st_dev ||
	    stbuf1.st_ino != stbuf2.st_ino) {
		(void )fprintf(stderr, "%s: [f]stat info mismatch\n", path);
		goto out;
	}
#ifdef notdef
	if (stvfsbuf1.f_fsid != stvfsbuf2.f_fsid) {
		(void )fprintf(stderr, "%s: [f]statvfs info mismatch\n", path);
	}
#endif
	printf("%s:"
	       " dev %lu,"
	       " ino %lu,"
	       " mode %lu,"
	       " nlink %lu,"
	       " uid %lu,"
	       " gid %lu,"
	       " rdev %lu,"
	       " size %llu,"
	       " blksize %lu,"
	       " blocks %lu,"
	       " atime %lu,"
	       " mtime %lu,"
	       " ctime %lu"
	       "\n",
	       path,
	       (unsigned long )stbuf1.st_dev,
	       (unsigned long )stbuf1.st_ino,
	       (unsigned long )stbuf1.st_mode,
	       (unsigned long )stbuf1.st_nlink,
	       (unsigned long )stbuf1.st_uid,
	       (unsigned long )stbuf1.st_gid,
	       (unsigned long )stbuf1.st_rdev,
	       (unsigned long long)stbuf1.st_size,
	       (unsigned long )stbuf1.st_blksize,
	       (unsigned long )stbuf1.st_blocks,
	       (unsigned long )stbuf1.st_atime,
	       (unsigned long )stbuf1.st_mtime,
	       (unsigned long )stbuf1.st_ctime);
out:
	if (SYSIO_INTERFACE_NAME(close)(fd) != 0)
		perror("closing file");
}
