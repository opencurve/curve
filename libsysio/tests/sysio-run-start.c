#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <xtio.h>
#include "test.h"

void _sysio_startup(void) __attribute__ ((constructor));

void
_sysio_startup()
{
	int	err;

	err = _test_sysio_startup();
	if (err) {
		errno = -err;
		perror("sysio startup");
		abort();
	}
	if (atexit(_test_sysio_shutdown) != 0) {
		perror("atexit");
		abort();
	}
}
