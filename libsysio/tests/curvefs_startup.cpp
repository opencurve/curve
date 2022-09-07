#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/queue.h>

#include "curvefs_test.h"

#include "sysio.h"

#define CURVEFS_TEST_OPTS "{mnt, dev=\"fuse:/data/yzhu/bbfs\", dir=/b_fuse/, fl =2}"


int (*curvefs_drvinits[])(void) = {
	_sysio_curvefs_init,
	NULL
};

static int curvefs_drv_init_all() {
    int (**f)(void);
    int err;

    err = 0;
    f = curvefs_drvinits;
    while (*f) {
        err = (**f++)();
        if (err)
            return err;
    }

    return 0;
}

int
_test_curvefs_sysio_startup()
{
	int	err;
	char* arg;

	err = _sysio_init();
	if (err)
		return err;
	err = curvefs_drv_init_all();
	if (err)
		return err;

	return 0;
}

void
_test_curvefs_sysio_shutdown()
{
	_sysio_shutdown();
}
