#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/queue.h>

#include "test.h"

#include "sysio.h"
#include "xtio.h"

int
_test_sysio_startup()
{
	int	err;
	char	*arg;

	err = _sysio_init();
	if (err)
		return err;
	err = drv_init_all();
	if (err)
		return err;
	/*
	 * namespace
	 */
	arg = getenv("SYSIO_NAMESPACE");
	if (!(arg || (arg = getenv("SYSIO_MANUAL")))) {
		/*
		 * Assume a native mount at root with automounts enabled.
		 */
		arg = "{mnt,dev=\"native:/\",dir=/,fl=2}";
	}
	err = _sysio_boot("namespace", arg);
	if (err)
		return err;
#ifdef DEFER_INIT_CWD
	/*
	 * Current working directory.
	 */
	arg = getenv("SYSIO_CWD");
	if (!arg)
		arg = "/";
	err = _sysio_boot("cwd", arg);
	if (err)
		return err;
#endif
#ifdef SYSIO_TRACING
	/*
	 * tracing
	 */
	arg = getenv("SYSIO_TRACING");
	err = _sysio_boot("trace", arg);
	if (err)
		return err;
#endif
	return 0;
}

void
_test_sysio_shutdown()
{

	_sysio_shutdown();
}
