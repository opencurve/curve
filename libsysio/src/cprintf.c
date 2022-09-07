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

#if defined(__linux__) && !defined(_BSD_SOURCE)
#define _BSD_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>

#include "sysio.h"
#include "native.h"

#if !(defined(_HAVE_ASPRINTF) && _HAVE_ASPRINTF)
/*
 * Print a string to allocated memory.
 */
static int
vasprintf(char **strp, const char *fmt, va_list ap)
{
	size_t	siz;
	int	oerrno;
	char	*s;
	va_list	aq;
	int	n;

	siz = 50;
	oerrno = errno;
	if (!(s = malloc(siz))) {
		errno = oerrno;
		return -1;
	}
	for (;;) {
		va_copy(aq, ap);
		n = vsnprintf (s, siz, fmt, aq);
		va_end(aq);
		if (n > -1 && (size_t )n < siz)
			break;
		if (n > -1)				/* glibc 2.1 */
			siz = n+1;			/* precise */
		else					/* glibc 2.0 */
			siz *= 2;			/* twice the old */
		if (!(s = realloc (s, siz)))
			break;
	}
	*strp = s;
	errno = oerrno;
	return n;
}

static int
asprintf(char **strp, const char *fmt, ...)
{
	va_list	ap;
	int	n;

	va_start(ap, fmt);
	n = vasprintf(strp, fmt, ap);
	va_end(ap);
	return n;
}
#endif /* !(defined(_HAVE_ASPRINTF) && _HAVE_ASPRINTF) */

static void
_sysio_cwrite(const char *buf, size_t len)
{
	int	oerrno;

	oerrno = errno;
	(void )syscall(SYSIO_SYS_write, STDERR_FILENO, buf, len);
	errno = oerrno;
}

/*
 * Console printf.
 */
void
_sysio_cprintf(const char *fmt, ...)
{
	va_list	ap;
	int	len;
	char	*buf;

	va_start(ap, fmt);
	buf = NULL;
	len = vasprintf(&buf, fmt, ap);
	va_end(ap);
	if (len < 0)
		return;
	_sysio_cwrite(buf, len);
	free(buf);
}

#ifndef NDEBUG
/*
 * Diagnostic printf.
 */
void
_sysio_diag_printf(const char *funidnam, const char *fmt, ...)
{
	char	*pre;
	va_list	ap;
	char	*msg;

	if (asprintf(&pre, "%s: ", funidnam) < 0)
		pre = NULL;
	va_start(ap, fmt);
	if (vasprintf(&msg, fmt, ap) < 0)
		msg = NULL;
	va_end(ap);
	if (pre && msg)
		_sysio_cprintf("%s%s", pre, msg);
	if (msg)
		free(msg);
	if (msg)
		free(pre);
}
#endif
