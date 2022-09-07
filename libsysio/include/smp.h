#ifndef __SMP_H__
#define __SMP_H__
/*
 * Symmetric multi-processing abstraction.
 *
 * Allows compilation and execution to take place in two modes, single or
 * multi threaded.
 *
 * In single thread mode (define SINGLE_THREAD) most locking calls are
 * emulated. Others cause panics. For instance, it is very hard to
 * wait on a conditional variable with only a single thread -- no thread
 * to awaken things later.
 *
 * Structure:
 *
 * The structure of this file is blocks of implementation for a
 * mode or thread architecture surrounded by ifdef's. In the first part,
 * "real" thread packages are abstracted to the POSIX interface. In the
 * second (unsurrounded) part, common usage with asserts to catch errors,
 * that should never happen, is defined.
 *
 * More often than not, the unsurrounded interface should be used. You
 * can't though if the common usage doesn't match yours. For example,
 * if you want to take a mutex but are interested in the return code or
 * can't have the program abort on failure, use the under-score prepended
 * version of the routine.
 *
 * $Id: smp.h,v 1.1 2008/06/17 17:18:57 lward Exp $
 */

#include <assert.h>

#ifdef THREAD_MODEL_POSIX
#include <pthread.h>
#include <semaphore.h>

#define THREAD_T_FMT			"%lu"

typedef pthread_t thread_t;
#define thread_self()			pthread_self()

typedef pthread_mutex_t mutex_t;

#define MUTEX_NORMAL			PTHREAD_MUTEX_FAST_NP
#define MUTEX_RECURSIVE			PTHREAD_MUTEX_RECURSIVE_NP

extern int _posix_mutex_init_helper(mutex_t *mp, int typ);

#define _mutex_init(mp, _typ)		_posix_mutex_init_helper((mp), (_typ))
#define _mutex_destroy(mp)		pthread_mutex_destroy(mp)
#define _mutex_lock(mp)			pthread_mutex_lock(mp)
#define _mutex_trylock(mp)		pthread_mutex_trylock(mp)
#define _mutex_unlock(mp)		pthread_mutex_unlock(mp)

typedef pthread_cond_t cond_t;
#define COND_INITIALIZER		PTHREAD_COND_INITIALIZER

#define _cond_init(condp)		pthread_cond_init((condp), NULL)
#define _cond_destroy(condp)		pthread_cond_destroy(condp)
#define _cond_wait(condp, mp)		pthread_cond_wait((condp), (mp))
#define _cond_timedwait(condp, mp, tsp)	\
	pthread_cond_timedwait((condp), (mp), (tsp))
#define _cond_signal(condp)		pthread_cond_signal(condp)
#define _cond_broadcast(condp)		pthread_cond_broadcast(condp)

typedef pthread_once_t once_t;
#define ONCE_INIT			PTHREAD_ONCE_INIT

#define _thread_once(oncp, f)		pthread_once((oncp), (f))

typedef pthread_key_t thread_key_t;

#define _thread_key_create(keyp, f)	pthread_key_create((keyp), (f))
#define _thread_key_delete(key)		pthread_key_delete(key)
#define _thread_setspecific(key, p)	pthread_setspecific((key), (p))
#define _thread_getspecific(key)	pthread_getspecific(key)

#define _thread_yield()			sched_yield()
#else

typedef unsigned thread_t;
#define thread_self()			((thread_t )(0))

typedef struct {
	unsigned
		initialized		: 1,
		typ			: 2;
	unsigned lckcnt;
} mutex_t;

#define MUTEX_NORMAL			(1)
#define MUTEX_RECURSIVE			(2)

#define __MUTEX_IF_INIT(mp, action) \
	((mp)->initialized ? (action) : EINVAL)
#define __MUTEX_ON_TYPE(mp, normal, recursive) \
	((mp)->typ == MUTEX_NORMAL \
	   ? (normal) \
	   : ((mp)->typ == MUTEX_RECURSIVE \
		? (recursive) \
		: EINVAL))

#define _mutex_init(mp, _typ) \
	((mp)->initialized = 1, \
	 (mp)->typ = (_typ), \
	 (mp)->lckcnt = 0, \
	 0)
#define _mutex_destroy(mp) \
	__MUTEX_IF_INIT((mp), \
			(!(mp)->lckcnt \
			   ? ((mp)->initialized = 0, 0) \
			   : EBUSY))
#define _mutex_lock(mp) \
	__MUTEX_IF_INIT((mp), \
			__MUTEX_ON_TYPE((mp), \
					((mp)->lckcnt \
					   ? EDEADLK \
					   : (mp)->lckcnt++), \
					(++(mp)->lckcnt \
					   ? 0 \
					   : ERANGE)))
#define _mutex_trylock(mp) \
	__MUTEX_IF_INIT((mp), \
			__MUTEX_ON_TYPE((mp), \
					((mp)->lckcnt \
					   ? EBUSY \
					   : (mp)->lckcnt++), \
					(++(mp)->lckcnt \
					   ? 0 \
					   : ERANGE)))
#define _mutex_unlock(mp) \
	__MUTEX_IF_INIT((mp), ((mp)->lckcnt ? (--(mp)->lckcnt, 0) : ERANGE))

typedef unsigned cond_t;

#define COND_INITIALIZER		(1)

#define __COND_IF_INIT(cond, action) \
	(*(cond) == COND_INTIALIZER ? (action) : EINVAL)
#define _cond_init(condp)		((int )(*(condp) = COND_INITIALIZER))
#define _cond_destroy(condp)		((int )(*(condp) = 0))
#define _cond_wait(condp, mp) \
	(__COND_IF_INIT((condp), \
			(__MUTEX_IF_INIT((mp), ENOSUP))))
#define _cond_timedwait(condp, mp, tsp) \
	(__COND_IF_INIT((condp), \
			(__MUTEX_IF_INIT((mp), ENOSUP))))
#define _cond_signal(condp)		(0)
#define _cond_broadcast(condp)		(0)

typedef int once_t;
#define ONCE_INIT			(0)

#define _thread_once(oncp, f)		(*(oncp) ? 0 : (*(f))(), *(oncp) = 1, 0)

typedef struct _thread_key *thread_key_t;

#define _thread_key_create(keyp, f)	_single_thread_key_create((keyp), (f))
#define _thread_key_delete(key)		_single_thread_key_delete(key)
#define _thread_setspecific(key, p)	_single_thread_setspecific((key), (p))
#define _thread_getspecific(key)	_single_thread_getspecific(key)

#define _thread_yield()			((int )0)

extern int _single_thread_key_create(thread_key_t *, void (*)(void *));
extern int _single_thread_key_delete(thread_key_t);
extern int _single_thread_setspecific(thread_key_t, void *);
extern void *_single_thread_getspecific(thread_key_t);
#endif /* thread model */

#define mutex_init(mp, _typ)		assert(_mutex_init((mp), (_typ)) == 0)
#define mutex_destroy(mp)		assert(_mutex_destroy(mp) == 0)
#define mutex_lock(mp)			assert(_mutex_lock(mp) == 0)
#define mutex_trylock(mp)		_mutex_trylock(mp)
#define mutex_unlock(mp)		assert(_mutex_unlock(mp) == 0)

#define cond_init(condp)		assert(_cond_init(condp) == 0)
#define cond_destroy(condp)		assert(_cond_destroy(condp) == 0)
#define cond_wait(condp, mp)		assert(_cond_wait((condp), (mp)) == 0)
#define cond_timedwait(condp, mp, tsp) \
	_cond_timedwait((condp), (mp), (tsp))
#define cond_signal(condp)		assert(_cond_signal(condp) == 0)
#define cond_broadcast(condp)		assert(_cond_broadcast(condp) == 0)

#define sema_init(semp, val)		assert(_sema_init((semp), (val)) == 0)
#define sema_destroy(semp)		assert(_sema_destroy(semp) == 0)
#define sema_wait(semp)			assert(_sema_wait(semp) == 0)
#define sema_trywait(semp)		_sema_trywait(semp)
#define sema_post(semp)			assert(_sema_post(semp) == 0)
#define sema_getvalue(semp, valp) \
	assert(_sema_getvalue((semp), (valp)) == 0)

#define thread_once(oncp, f)		assert(_thread_once((oncp), (f)) == 0)

#define thread_key_create(keyp, f) \
	assert(_thread_key_create((keyp), (f)) == 0)
#define thread_key_delete(key)		assert(_thread_key_delete(key) == 0)
#define thread_setspecific(key, p) \
	assert(_thread_setspecific((key), (p)) == 0)
#define thread_getspecific(key)		_thread_getspecific(key)

#define thread_yield()			assert(_thread_yield() == 0)
#endif /* !defined(__SMP_H__) */
