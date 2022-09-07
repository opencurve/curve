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

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/queue.h>

#include "sysio.h"
#include "inode.h"
#include "fs.h"
#include "mount.h"
#include "file.h"
#include "dev.h"

#ifdef STDFD_DEV
#include "stdfd.h"
#endif
#ifdef STDDEV_DEV
#include "stddev.h"
#endif

mutex_t _sysio_biglock;					/* API big lock */

/*
 * White space characters.
 */
#define IGNORE_WHITE		" \t\r\n"

/*
 * Check if long overflows integer range.
 */
#if LONG_MAX <= INT_MAX
#define _irecheck(_l, _e) \
	((_l) == LONG_MAX && (_e) == ERANGE)
#else
#define _irecheck(_l, _e) \
	((_l) > INT_MAX)
#endif

/*
 * In sysio_init we'll allow simple comments, strings outside {}
 * delimited by COMMENT_INTRO, and '\n' or '\0'
 */
#define COMMENT_INTRO          '#'

#ifdef SYSIO_TRACING
static void *entry_tcb = NULL;				/* entry callback */
static void *exit_tcb = NULL;				/* exit callback */
#endif /* defined(SYSIO_TRACING) */

#ifdef SYSIO_TRACING
static void
trace_entry(const char *file __IS_UNUSED,
	    const char *func,
	    int line __IS_UNUSED,
	    void *data __IS_UNUSED,
	    tracing_tag tag,
	    const char *fmt __IS_UNUSED,
	    va_list ap __IS_UNUSED)
{

	_sysio_cprintf("+ENTER+ %s (%d)\n", func, tag);
}

static void
trace_exit(const char *file __IS_UNUSED,
	   const char *func,
	   int line __IS_UNUSED,
	   void *data __IS_UNUSED,
	   tracing_tag tag,
	   const char *fmt __IS_UNUSED,
	   va_list ap __IS_UNUSED)
{

	_sysio_cprintf("+EXIT+ %s (%d)\n", func, tag);
}

/*
 * Start/Stop our simple tracer.
 *
 * Normally, these things would remove themselves from the trace
 * initializers queue when called. For backward compatibility, this one
 * must stick around so it can be turned on and off. Ugh! It all does get
 * cleaned up at exit, though.
 */
static void
trace_initializer(const char *file __IS_UNUSED,
		  const char *func,
		  int line __IS_UNUSED,
		  void *data __IS_UNUSED,
		  tracing_tag tag __IS_UNUSED,
		  const char *fmt __IS_UNUSED,
		  va_list ap __IS_UNUSED)
{
	long	l;
	char	*cp;

	/*
	 * Func is overloaded, for initializers. It contains the initializer
	 * argument string.
	 *
	 * We are looking for an integer value. If non-zero, turn our
	 * little tracer on. Otherwise, off.
	 */
	l = strtol(func, (char **)&cp, 0);
	if (*func != '\0' && *cp != '\0') {
		/*
		 * Not for us. Must be some other.
		 */
		return;
	}
	if (l) {
		if (!entry_tcb)
			entry_tcb =
			    _sysio_register_trace(_sysio_entry_trace_q,
						  &trace_entry,
						  NULL,
						  NULL);
		if (!exit_tcb)
			exit_tcb =
			    _sysio_register_trace(_sysio_exit_trace_q,
						  &trace_exit,
						  NULL,
						  NULL);
	} else {
		if (entry_tcb) {
			_sysio_remove_trace(_sysio_entry_trace_q,
					    entry_tcb);
			entry_tcb = NULL;
		}
		if (exit_tcb) {
			_sysio_remove_trace(_sysio_exit_trace_q,
					    exit_tcb);
			exit_tcb = NULL;
		}
	}
}
#endif /* defined(SYSIO_TRACING) */

/*
 * Sysio library initialization. Must be called before anything else in the
 * library.
 */
int
_sysio_init()
{
	int	err;

	mutex_init(&_sysio_biglock, MUTEX_RECURSIVE);

#ifdef SYSIO_TRACING
	err = _sysio_trace_init();
	if (err)
		goto error;
	err =
	    _sysio_register_trace(_sysio_initializer_trace_q,
				  &trace_initializer,
				  NULL,
				  NULL)
	  ? 0
	  : -ENOMEM;
	if (err)
		goto error;
#endif
	err = _sysio_ioctx_init();
	if (err)
		goto error;
	err = _sysio_i_init();
	if (err)
		goto error;
	err = _sysio_mount_init();
	if (err)
		goto error;
	err = _sysio_dev_init();
	if (err)
		goto error;
#ifdef STDFD_DEV
	err = _sysio_stdfd_init();
	if (err)
		goto error;
#endif
#ifdef STDDEV_DEV
	err = _sysio_stddev_init();
	if (err)
		goto error;
#endif

	goto out;
error:
	errno = -err;
out:
	/*
	 * Unlike all other _sysio routines, this one returns with errno
	 * set. It also returns the error, as usual.
	 */
	return err;
}

/*
 * Sysio library shutdown.
 */
void
_sysio_shutdown()
{

#ifdef SYSIO_TRACING
	_sysio_trace_shutdown();
#endif
	if (!(_sysio_fd_close_all() == 0 &&
	      _sysio_unmount_all() == 0))
			abort();
#ifdef ZERO_SUM_MEMORY
	_sysio_fd_shutdown();
	_sysio_i_shutdown();
	_sysio_fssw_shutdown();
	_sysio_access_shutdown();
#endif

	mutex_destroy(&_sysio_biglock);
}

/* 
 * (kind of)Duplicates strtok function.
 *
 * Given a buffer, returns the longest string
 * that does not contain any delim characters.  Will
 * remove ws and any characters in the ignore string.  
 * Returns the token.  
 *
 * The parameter controlling acceptance controls whether a positive
 * match for some delimiter be made or not. If set, then either a delimiter
 * or NUL character is success.
 *
 */
const char *
_sysio_get_token(const char *buf,
	  int accepts,
	  const char *delim,
	  const char *ignore,
	  char *tbuf)
{
	char	c;
	int	escape, quote;

	/* 
	 * Find the first occurance of delim, recording how many
	 * characters lead up to it.  Ignore indicated characters.
	 */
	escape = quote = 0;
	while ((c = *buf) != '\0') {
		buf++;
		if (!escape) {
			if (c == '\\') {
				escape = 1;
				continue;
			}
			if (c == '\"') {
				quote ^= 1;
				continue;
			}
			if (!quote) {
				if (strchr(delim, c) != NULL) {
					accepts = 1;
					break;
				}
				if (strchr(ignore, c) != NULL)
					continue;
			}
		} else
			escape = 0;
		*tbuf++ = c;
	}
	if (!accepts)
		return NULL;
	*tbuf = '\0';						/* NUL term */
	return buf;
}

/*
 * Parse and record named arguments given as `name = value', comma-separated
 * pairs.
 *
 * NB: Alters the passed buffer.
 */
char *
_sysio_get_args(char *buf, struct option_value_info *vec)
{
	char	*nxt;
	char	*name, *value;
	struct option_value_info *v;

	for (;;) {
		nxt =
		    (char *)_sysio_get_token(buf,
					     1,
					     "=,",
					     IGNORE_WHITE,
					     name = buf);
		if (!nxt ||
		    (nxt != buf && *name == '\0' && buf + strlen(buf) == nxt)) {
			buf = NULL;
			break;
		}
		if (*name == '\0')
			break;
		buf =
		    (char *)_sysio_get_token(nxt,
					     1,
					     ",",
					     IGNORE_WHITE,
					     value = nxt);
		if (*value == '\0')
			value = NULL;
		for (v = vec; v->ovi_name; v++)
			if (strcmp(v->ovi_name, name) == 0)
				break;
		if (!v->ovi_name)
			return NULL;
		v->ovi_value = value;
	}

	return buf;
}

static int
parse_mm(const char *s, dev_t *devp)
{
	unsigned long ul;
	char	*cp;
	dev_t   major, minor;

	ul = strtoul(s, &cp, 0);
	if (*cp != '+' || ul > USHRT_MAX)
		return -EINVAL;
	major = ul;
	if (major != ul)
		return -ERANGE;
	s = (const char *)++cp;
	ul = strtoul(s, &cp, 0);
	if (*cp != '\0' || ul > USHRT_MAX)
		return -EINVAL;
	minor = ul;
	if (minor != ul)
		return -ERANGE;
	*devp = SYSIO_MKDEV(major, minor);
	return 0;
}

/*
 * Performs the creat command for the namespace assembly
 *
 * NB: Alters the passed buffer.
 */
static int 
do_creat(char *args) 
{
	size_t	len;
	struct option_value_info v[] = {
		{ "ft",		NULL },			/* file type */
		{ "nm",		NULL },			/* name */
		{ "pm",		NULL },			/* permissions */
		{ "ow",		NULL },			/* owner */
		{ "gr",		NULL },			/* group */
		{ "mm",		NULL },			/* major + minor */
		{ "str",	NULL },			/* file data */
		{ NULL,		NULL }
	};
	const char *cp;
	long	perms;
	long	owner, group;
	struct pnode *dir, *pno;
	mode_t	mode;
	struct intent intent;
	dev_t	dev;
	int	err;
	enum {
		CREATE_DIR	= 1,
		CREATE_CHR	= 2,
		CREATE_BLK	= 3,
		CREATE_FILE	= 4
	} op;
	int	intent_mode;
	int	i;
  
	len = strlen(args);
	if (_sysio_get_args(args, v) - args != (ssize_t )len ||
	    !(v[0].ovi_value &&
	      v[1].ovi_value &&
	      v[2].ovi_value))
		return -EINVAL;
	perms = strtol(v[2].ovi_value, (char **)&cp, 0);
	if (*cp ||
	    perms < 0 ||
	    (perms == LONG_MAX && errno == ERANGE) ||
	    ((unsigned)perms & ~07777))
		return -EINVAL;
	if (v[3].ovi_value) {
		owner = strtol(v[3].ovi_value, (char **)&cp, 0);
		if (*cp ||
		    ((owner == LONG_MIN || owner == LONG_MAX)
		     && errno == ERANGE))
			return -EINVAL;
	} else
		owner = getuid();
	if (v[4].ovi_value) {
		group = strtol(v[4].ovi_value, (char **)&cp, 0);
		if (*cp ||
		    ((group == LONG_MIN || group == LONG_MAX) &&
		     errno == ERANGE))
			return -EINVAL;
	} else
		group = getegid();

	if (!(dir = _sysio_cwd) && !(dir = _sysio_root))
		return -ENOENT;

	/*
	 * Init, get the operation, setup the intent.
	 */
	err = 0;
	mode = perms;
	op = 0;
	if (strcmp(v[0].ovi_value, "dir") == 0) {
		op = CREATE_DIR;
	} else if (strcmp(v[0].ovi_value, "chr") == 0) {
		op = CREATE_CHR;
		mode |= S_IFCHR;
		if (!(v[5].ovi_value && parse_mm(v[5].ovi_value, &dev) == 0))
			err = -EINVAL;
	} else if (strcmp(v[0].ovi_value, "blk") == 0) {
		op = CREATE_BLK;
		mode |= S_IFBLK;
		if (!(v[5].ovi_value && parse_mm(v[5].ovi_value, &dev) == 0))
			err = -EINVAL;
	} else if (strcmp(v[0].ovi_value, "file") == 0) {
		op = CREATE_FILE;
		intent_mode = O_CREAT|O_EXCL;
	} else
		err = -EINVAL;
	if (err)
		return err;
	INTENT_INIT(&intent, INT_CREAT|INT_UPDPARENT, &mode, &intent_mode);

	/*
	 * Lookup the given path.
	 */
	err =
	    _sysio_namei(dir,
			 v[1].ovi_value,
			 ND_NEGOK|ND_NOPERMCHECK|ND_WANTPARENT,
			 &intent,
			 &pno);
	if (err)
		return err;

	/*
	 * Perform.
	 */
	switch (op) {
	case CREATE_DIR:
		err = _sysio_mkdir(pno, mode);
		break;
	case CREATE_CHR:
	case CREATE_BLK:
		err = _sysio_mknod(pno, mode, dev);
		break;
	case CREATE_FILE:
		err = _sysio_open(pno, O_CREAT|O_EXCL, mode);
		if (err)
			break;
		if (v[6].ovi_value) {
			size_t	count;
			struct ioctx *ioctx;

			/*
			 * Deposit optional file content.
			 */
			count = strlen(v[6].ovi_value);
			err =
			    _sysio_p_awrite(pno,
					    0,
					    v[6].ovi_value,
					    count,
					    &ioctx);
			if (!err) {
				ssize_t	cc;

				cc = _sysio_ioctx_wait(ioctx);
				if (cc < 0)
					err = cc;
				else if ((size_t )cc != count)
					err = -EIO;		/* huh? */
			}
		}
		i = PNOP_CLOSE(pno);
		if (!err)
			err = i;
		break;
	default:
		abort();
	}

	P_PUT(pno->p_parent);
	P_PUT(pno);
	return err;
}

/*
 * Do mount.
 *
 * NB: The passed buffer is altered.
 */
static int 
do_mnt(char *args) 
{
	size_t	len;
	struct option_value_info v[] = {
		{ "dev",	NULL },			/* source (type:dev) */
		{ "dir",	NULL },			/* target dir */
		{ "fl",		NULL },			/* flags */
		{ "da",		NULL },			/* mount data */
		{ NULL,		NULL }
	};
	char	*ty, *name;
	unsigned long flags;
	struct pnode *dir;
	int	err;
  
	len = strlen(args);
	if (_sysio_get_args(args, v) - args != (ssize_t )len ||
	    !(v[0].ovi_value && v[1].ovi_value))
		return -EINVAL;
	ty =
	    (char *)_sysio_get_token(v[0].ovi_value,
				     1,
				     ":",
				     "",
				     name = v[0].ovi_value);
	flags = 0;
	if (v[2].ovi_value) {
		char	*cp;

		/*
		 * Optional flags.
		 */
		flags = strtoul(v[2].ovi_value, &cp, 0);
		if (*cp || (flags == ULONG_MAX && errno == ERANGE))
			return -EINVAL;
	}

	if (strlen(v[1].ovi_value) == 1 && v[1].ovi_value[0] == PATH_SEPARATOR) {
		/*
		 * Aha! It's root they want. Have to do that special.
		 */
		return _sysio_mount_root(ty, name, flags, v[3].ovi_value);
	}

	if (!(dir = _sysio_cwd) && !(dir = _sysio_root))
		return -ENOENT;
	P_GET(dir);
	err = _sysio_mount(dir,
			   ty,
			   v[1].ovi_value,
			   name,
			   flags,
			   v[3].ovi_value);
	P_PUT(dir);
	return err;
}


#if 0
/*
 * Chdir
 *
 * NB: Alters the passed buffer.
 */
static int 
do_cd(char *args) 
{
	size_t	len;
	struct option_value_info v[] = {
		{ "dir",	NULL },			/* directory */
		{ NULL,		NULL }
	};
	int	err;
	struct pnode *dir, *pno;

	len = strlen(args);
	if (_sysio_get_args(args, v) - args != (ssize_t )len || !v[0].ovi_value)
		return -EINVAL;

	if (!(dir = _sysio_cwd) && !(dir = _sysio_root)) {
		/*
		 * We have no namespace yet. They really need to give us
		 * something to work with.
		 */
		return -ENOENT;
	}
	err = _sysio_namei(dir, v[0].ovi_value, 0, NULL, &pno);
	if (err)
		return err;
	err = _sysio_p_chdir(pno);
	P_PUT(pno);
	return err;
}
#endif

/*
 * Does a chmod
 *
 * NB: Alters passed buffer.
 */
static int 
do_chmd(char *args)
{
	size_t	len;
	struct option_value_info v[] = {
		{ "src",	NULL },			/* path */
		{ "pm",		NULL },			/* perms */
		{ NULL,		NULL }
	};
	long	perms;
	char	*cp;
	struct intnl_stat stbuf;
	int	err;
	struct pnode *dir, *pno;
  
	len = strlen(args);
	if (_sysio_get_args(args, v) - args != (ssize_t )len ||
	    !(v[0].ovi_value && v[1].ovi_value))
		return -EINVAL;
	perms = strtol(v[1].ovi_value, &cp, 0);
	if (*cp ||
	    perms < 0 ||
	    (perms == LONG_MAX && errno == ERANGE) ||
	    ((unsigned)perms & ~07777))
		return -EINVAL;
	(void )memset(&stbuf, 0, sizeof(stbuf));
	stbuf.st_mode = (mode_t)perms;

	if (!(dir = _sysio_cwd) && !(dir = _sysio_root))
		return -ENOENT;
	err = _sysio_namei(dir, v[0].ovi_value, ND_NOPERMCHECK, NULL, &pno);
	if (err)
		return err;
	err = _sysio_p_setattr(pno, SETATTR_MODE, &stbuf);
	P_PUT(pno);

	return err;
}

static int
do_open(char *args)
{
	size_t	len;
	struct option_value_info v[] = {
		{ "nm",		NULL },			/* path */
		{ "fd",		NULL },			/* fildes */
		{ "m",		NULL },			/* mode */
		{ NULL,		NULL }
	};
	char	*cp;
	long	l;
	int	fd;
	unsigned long ul;
	mode_t	m;
	struct pnode *dir, *pno;
	struct intent intent;
	int	err;
	struct file *fil;

	len = strlen(args);
	if (_sysio_get_args(args, v) - args != (ssize_t )len ||
	    !(v[0].ovi_value && v[1].ovi_value && v[2].ovi_value))
		return -EINVAL;
	l = strtol(v[1].ovi_value, (char **)&cp, 0);
	if (*cp || l < 0 || _irecheck(l, errno))
		return -EINVAL;
	fd = (int )l;
	ul = strtoul(v[1].ovi_value, (char **)&cp, 0);
	if (*cp ||
	    (ul == ULONG_MAX && errno == ERANGE))
		return -EINVAL;
	m = (mode_t )ul & (O_RDONLY|O_WRONLY|O_RDWR);

	if (!(dir = _sysio_cwd) && !(dir = _sysio_root))
		return -ENOENT;
	INTENT_INIT(&intent, INT_OPEN, &m, NULL);
	pno = NULL;
	err = _sysio_namei(dir, v[0].ovi_value, ND_NOPERMCHECK, &intent, &pno);
	if (err)
		return err;
	fil = NULL;
	do {
		err = _sysio_open(pno, m, 0);
		if (err)
			break;
		fil = _sysio_fnew(pno, m);
		if (!fil) {
			err = -ENOMEM;
			break;
		}
		pno = NULL;
		err = _sysio_fd_set(fil, fd, 1);
		if (err < 0)
			break;
		err = 0;
	} while (0);
	if (fil)
		FIL_PUT(fil);
	if (pno)
		P_PUT(pno);
	return err;
}

/*
 * Execute the given cmd.
 *
 * NB: Buf is altered.
 */
static int 
do_command(char *buf)
{
	size_t	len;
	char	*args, *cmd;

	len = strlen(buf);
	args = (char *)_sysio_get_token(buf, 1, ",", IGNORE_WHITE, cmd = buf);
	if (args) {
		if (strcmp("creat", cmd) == 0)
			return do_creat(args);
		if (strcmp("mnt", cmd) == 0)
			return do_mnt(args);
#if 0
		if (strcmp("cd", cmd) == 0)
			return do_cd(args);
#endif
		if (strcmp("chmd", cmd) == 0)
			return do_chmd(args);
		if (strcmp("open", cmd) == 0)
			return do_open(args);
	}
	return -EINVAL;
}

#ifdef SYSIO_TRACING
/*
 * Set/Unset tracing.
 */
static int
_sysio_boot_tracing(const char *arg)
{

	if (!arg) {
		/*
		 * NULL arg means not set at all. We really shouldn't
		 * have even been called. However, sometimes we are.
		 * It's not an error, then. Just a no-op.
		 */
		return 0;
	}

	_sysio_run_trace_q(_sysio_initializer_trace_q,
			   NULL,
			   arg,
			   -1,
			   -1,
			   NULL);
	return 0;
}
#endif

/*
 * Initialize the namespace.
 */
static int
_sysio_boot_namespace(const char *arg)
{
	char	c, *tok;
	ssize_t	len;
	int	err;
	unsigned count;
	/*
	 * Allocate token buffer.
	 */
	len = strlen(arg);
	tok = malloc(len ? len : 1);
	if (!tok)
		return -ENOMEM;
	err = 0;
	count = 0;
	while (1) {
		/*
		 * Discard leading white space.
		 */
		while ((c = *arg) != '\0' && strchr(IGNORE_WHITE, c))
			arg++;
		if (COMMENT_INTRO == c) {
			/*
			 * Discard comment.
			 */
			while (*arg && (*arg != '\n')) {
				++arg;
			}
			continue;
		}

		if (c == '\0')
			break;
		if (c != '{') {
			err = -EINVAL;
			break;
		}
		/*
		 * Get the command.
		 */
		*tok = '\0';
		arg =
		    (char *)_sysio_get_token(arg + 1,
					     0,
					     "}",
					     IGNORE_WHITE,
					     tok);
		if (!arg) {
			err = -EINVAL;
			break;
		}
		count++;
		/*
		 * Perform.
		 */
		err = do_command(tok);
		if (err)
			break;
	}
#ifdef SYSIO_TRACING
	if (err)
		_sysio_cprintf("+NS init+ failed at expr %u (last = %s): %s\n", 
			       count,
			       tok && *tok ? tok : "NULL",
			       strerror(-err));
#endif
	free(tok);
	return err;
}

#ifdef DEFER_INIT_CWD
/*
 * Set deferred initial working directory.
 */
static int
_sysio_boot_cwd(const char *arg)
{

	_sysio_init_cwd = arg;
	return 0;
}
#endif

/*
 * Given an identifier and it's arguments, perform optional initializations.
 */
int 
_sysio_boot(const char *opt, const char *arg)
{
	struct option_value_info vec[] = {
#ifdef SYSIO_TRACING
		{ "trace",	NULL },			/* tracing? */
#endif
		{ "namespace",	NULL },			/* init namespace? */
#ifdef DEFER_INIT_CWD
		{ "cwd",	NULL },			/* init working dir */
#endif
		{ NULL,		NULL }
	};
	struct option_value_info *v;
	unsigned u;
	static int (*f[])(const char *) = {
#ifdef SYSIO_TRACING
		_sysio_boot_tracing,
#endif
		_sysio_boot_namespace,
#ifdef DEFER_INIT_CWD
		_sysio_boot_cwd,
#endif
		NULL					/* can't happen */
	};

	for (v = vec, u = 0; v->ovi_name; v++, u++)
		if (strcmp(v->ovi_name, opt) == 0)
			break;
	if (!v->ovi_name)
		return -EINVAL;
	return (*f[u])(arg);
}
