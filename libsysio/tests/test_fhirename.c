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
 *    Cplant(TM) Copyright 1998-2009 Sandia Corporation. 
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
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/uio.h>
#include <getopt.h>
#include <sys/statvfs.h>

#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "test.h"
#include "../misc/fhi.h"
#include "fhi_support.h"

/*
 * Link files.
 *
 * Usage: test_fhirename <old-path> <new-path>
 */

static int key;

static char root_handle_data[128];
static struct file_handle_info root_handle = {
	NULL,
	root_handle_data,
	sizeof(root_handle_data)
};

static int renameit(const char *from, const char *to);
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

	if (!(argc - optind))
		usage();
	key = 1;
	if (_test_fhi_start(&key, argv[optind], &root_handle)) {
		exit(1);
	}
	optind++;

	n = argc - optind;
	if (n != 2)
		usage();

	n = renameit(argv[optind], argv[optind + 1]);

	/*
	 * Clean up.
	 */
	err = SYSIO_INTERFACE_NAME(fhi_unexport)(root_handle.fhi_export);
	if (err) {
		perror("FHI unexport");
		exit(1);
	}
	_test_sysio_shutdown();

	return n ? 1 : 0;
}

static int
renameit(const char *from, const char *to)
{
	char	handle_data[128];
	struct file_handle_info handle = {
		NULL,
		handle_data,
		sizeof(handle_data)
	};
	int	err;
	struct file_handle_info_dirop_args args[2];

	err = _test_fhi_find(&root_handle, to, &handle);
	if (err == ENOENT)
		err = 0;
	else if (!err)
		err = -EEXIST;
	if (err) {
		perror(to);
		return -1;
	}
	args[0].fhida_path = from;
	args[0].fhida_dir = &root_handle;
	args[1].fhida_path = to;
	args[1].fhida_dir = &root_handle;
	err = SYSIO_INTERFACE_NAME(fhi_rename)(&args[0], &args[1]);
	if (err) {
		perror("Can't rename");
		return -1;
	}
	err = _test_fhi_find(&root_handle, to, &handle);
	if (err) {
		perror(to);
		return -1;
	}

	return 0;
}

static void
usage()
{

	(void )fprintf(stderr,
		       "Usage: test_fhirename"
		       " <old-path> "
		       " <new-path>\n");

	exit(1);
}
