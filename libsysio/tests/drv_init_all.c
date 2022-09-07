#include <stdio.h>

extern int (*drvinits[])(void);

/*
 * Init all the drivers we know about.
 */
int
drv_init_all()
{
	int (**f)(void);
	int	err;

	err = 0;
	f = drvinits;
	while (*f) {
		err = (**f++)();
		if (err)
			return err;
	}

	return 0;
}
