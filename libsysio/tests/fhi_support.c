#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#if defined(SYSIO_LABEL_NAMES)
#include "sysio.h"
#endif
#include "xtio.h"
#include "../misc/fhi.h"

int
_test_fhi_check_handle(const char *path,
		       struct file_handle_info *handle,
		       ssize_t nbytes)
{
	int	err;

	err = errno;
	do {
		if (nbytes < 0) {
			perror(path);
			break;
		}
		if ((size_t )nbytes > handle->fhi_handle_len) {
			(void )fprintf(stderr,
				       "Handle data for '%s' is too large\n",
				       path);
			err = EINVAL;
			break;
		}
		if (!nbytes) {
			(void )fprintf(stderr,
				       "Illegal handle for '%s'\n",
				       path);
			err = EINVAL;
			break;
		}
		err = 0;
	} while (0);
	return err;
}

int
_test_fhi_start(int *key,
		const char *path,
		struct file_handle_info *root)
{
	int	err;
	ssize_t	cc;

	err =
	    SYSIO_INTERFACE_NAME(fhi_export)(key,
					     sizeof(*key),
					     path,
					     0,
					     &root->fhi_export);
	if (err) {
		(void )fprintf(stderr,
			       "Export of '%s' failed: %s\n",
			       path,
			       strerror(errno));
		return errno;
	}
	cc =
	    SYSIO_INTERFACE_NAME(fhi_root_of)(root->fhi_export,
					      root);
	if (cc < 0) {
		(void )fprintf(stderr,
			       "Can't get root handle for '%s': %s\n",
			       path,
			       strerror(errno));
		return errno;
	}
	return _test_fhi_check_handle(path, root, cc);
}

int
_test_fhi_find(struct file_handle_info *parent,
	       const char *path,
	       struct file_handle_info *fhi)
{
	ssize_t	cc;

	cc = SYSIO_INTERFACE_NAME(fhi_lookup)(parent, path, 0, fhi);
	if (cc < 0)
		return errno;
	return _test_fhi_check_handle(path, fhi, cc);
}
