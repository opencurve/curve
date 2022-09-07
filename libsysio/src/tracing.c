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

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/queue.h>

#include "sysio.h"

#ifdef SYSIO_TRACING
/*
 * Tracing callback record.
 */
struct trace_callback {
	TAILQ_ENTRY(trace_callback) links;		/* trace list links */
	void	(*f)(const char *file,			/* callback function */
		     const char *func,
		     int line,
		     void *data,
		     int tag,
		     const char *fmt,
		     va_list ap);
	void	*data;					/* callback data */
	void	(*destructor)(void *data);		/* data destructor */
};

/*
 * Initialize a tracing callback record.
 */
#define TCB_INIT(__tcb, __f, __d, __destroy) \
	do { \
		(__tcb)->f = (__f); \
		(__tcb)->data = (__d); \
		(__tcb)->destructor = (__destroy); \
	} while (0);

/*
 * Trace queue head record.
 */
TAILQ_HEAD(trace_q, trace_callback);

/*
 * The entry and exit queue heads, and queue pointers.
 */
static struct trace_q _sysio_entry_trace_head;
void *_sysio_entry_trace_q = &_sysio_entry_trace_head;
static struct trace_q _sysio_exit_trace_head;
void *_sysio_exit_trace_q = &_sysio_exit_trace_head;

/*
 * Initializer pseudo-event queue head and pointer.
 */
static struct trace_q _sysio_initializer_trace_head;
void *_sysio_initializer_trace_q = &_sysio_initializer_trace_head;

/*
 * Register a trace callback.
 *
 * The pointer to the trace record is returned.
 */
void *
_sysio_register_trace(void *q,
		      void (*f)(const char *file,
				const char *func,
				int line,
				void *data,
				int tag,
				const char *fmt,
				va_list ap),
		      void *data,
		      void (*destructor)(void *data))
{
	struct trace_callback *tcb;

	tcb = malloc(sizeof(struct trace_callback));
	if (!tcb)
		return NULL;
	TCB_INIT(tcb, f, data, destructor);
	TAILQ_INSERT_TAIL((struct trace_q *)q, tcb, links);
	return tcb;
}

/*
 * Remove a registered trace callback.
 */
void
_sysio_remove_trace(void *q, void *p)
{
	struct trace_callback *tcb;

	tcb = (struct trace_callback *)p;

	if (tcb->destructor)
		(*tcb->destructor)(tcb->data);
	TAILQ_REMOVE((struct trace_q *)q, tcb, links);
	free(tcb);
}

void
/*
 * Run a trace queue, making all the callbacks.
 */
_sysio_run_trace_q(void *q,
		   const char *file,
		   const char *func,
		   int line,
		   int tag,
		   const char *fmt,
		   ...)
{
	va_list ap, aq;
	struct trace_callback *tcb;

	va_start(ap, fmt);
	tcb = ((struct trace_q *)q)->tqh_first;
	while (tcb) {
		va_copy(aq, ap);
		(*tcb->f)(file, func, line, tcb->data, tag, fmt, ap);
		va_end(aq);
		tcb = tcb->links.tqe_next;
	}
	va_end(ap);
}

int
_sysio_trace_init()
{
	/*
	 * Initialize initializer callback queue.
	 */
	TAILQ_INIT(&_sysio_initializer_trace_head);
	/*
	 * Initialize tracing callback queues.
	 */
	TAILQ_INIT(&_sysio_entry_trace_head);
	TAILQ_INIT(&_sysio_exit_trace_head);

	return 0;
}

/*
 * Sysio library shutdown.
 */
void
_sysio_trace_shutdown()
{
	struct trace_callback *tcb;

	/*
	 * Empty the initializer queue and free the entries.
	 */
	while ((tcb = _sysio_initializer_trace_head.tqh_first) != NULL)
		_sysio_remove_trace(&_sysio_initializer_trace_head, tcb);
	/*
	 * Empty the trace queues and free the entries.
	 */
	while ((tcb = _sysio_entry_trace_head.tqh_first) != NULL)
		_sysio_remove_trace(&_sysio_entry_trace_head, tcb);
	while ((tcb = _sysio_exit_trace_head.tqh_first) != NULL)
		_sysio_remove_trace(&_sysio_exit_trace_head, tcb);
}
#endif /* defined(SYSIO_TRACING) */
