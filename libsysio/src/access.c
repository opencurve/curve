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
 *    Cplant(TM) Copyright 1998-2006 Sandia Corporation. 
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
 * Albuquerque, NM 87185-1110
 *
 * lee@sandia.gov
 */

#include <stddef.h>						/* ptrdiff_t */
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/queue.h>

#include "sysio.h"
#include "mount.h"
#include "fs.h"
#include "inode.h"
#include "sysio-symbols.h"

static int get_credentials(struct creds **crp, int effective);
static void put_credentials(struct creds *cr);
static int check_permission(struct pnode *pno, struct creds *cr, int amode);

/*
 * If the application will be managing credentials externally, these
 * are the interface. Before calling this librariy's initialization
 * routines, reset them to your own.
 */
int	(*_sysio_get_credentials)(struct creds **, int) =
	  get_credentials;
void	(*_sysio_put_credentials)(struct creds *) =
	  put_credentials;
int	(*_sysio_check_permission)(struct pnode *, struct creds *, int) =
	  check_permission;

static struct user_credentials {
	struct creds creds;
	unsigned refs;
} *_current_ucreds = NULL;

/*
 * Check given access type on given inode.
 */
static int
check_permission(struct pnode *pno, struct creds *cr, int amode)
{
	mode_t	mask;
	struct inode *ino;
	int	err;
	struct intnl_stat *stat;
	gid_t	*gids;
	int	ngids;
	int	group_matched;

	assert(pno);

	/*
	 * Check amode.
	 */
	if ((amode & (R_OK|W_OK|X_OK)) != amode)
		return -EINVAL;

	if (!amode)
		return 0;

	mask = 0;
	if (amode & R_OK)
		mask |= S_IRUSR;
	if (amode & W_OK)
		mask |= S_IWUSR;
	if (amode & X_OK)
		mask |= S_IXUSR;

	assert(P_ISLOCKED(pno) && PB_ISLOCKED(pno->p_base));
	ino = pno->p_base->pb_ino;
	assert(ino && I_ISLOCKED(ino));

	err = -EACCES;					/* assume error */
	stat = &ino->i_stbuf;
	do {
#ifdef _SYSIO_ROOT_UID
		/*
		 * Root?
		 */
		if (_sysio_is_root(cr)) {
			err = 0;
			break;
		}
#endif

		/*
		 * Owner?
		 */
		if (stat->st_uid == cr->creds_uid) {
			if ((stat->st_mode & mask) == mask)
				err = 0;
			break;
		}

		/*
		 * Group?
		 */
		mask >>= 3;
		group_matched = 0;
		gids = cr->creds_gids;
		ngids = cr->creds_ngids;
		while (ngids) {
			ngids--;
			if (stat->st_gid == *gids++) {
				group_matched = 1;
				if ((stat->st_mode & mask) == mask)
					err = 0;
			}
		}
		if (group_matched)
			break;

		/*
		 * Other?
		 */
		mask >>= 3;
		if ((stat->st_mode & mask) == mask)
			err = 0;
	} while (0);
	if (err)
		return err;

	/*
	 * Check for RO access to the file due to mount
	 * options.
	 */
	if (amode & W_OK && IS_RDONLY(pno))
		return -EROFS;

	return 0;
}

/*
 * (Re)fill credentials
 */
static int
ldgroups(gid_t gid0, gid_t **gidsp, int *gidslenp)
{
	int	n, i;
	void	*p;

	n = *gidslenp;
	if (n < 8)
		n = 8;
	for (;;) {
		if (n > *gidslenp) {
			p = realloc(*gidsp, (size_t )n * sizeof(gid_t));
			if (!p)
				return -errno;
			*gidsp = p;
			*gidslenp = n;
		}
		(*gidsp)[0] = gid0;
		i = getgroups(n - 1, *gidsp + 1);
		if (i < 0) {
			if (errno != EINVAL)
				return -errno;
			if (INT_MAX / 2 < n)
				return -EINVAL;
			n *= 2;
			continue;
		}
		break;
	}
	/*
	 * increment count to include gidsp[0], we always 
	 * have at least that group.
	 */
	return i + 1;
}

/*
 * Get current credentials.
 */
static int
ldcreds(uid_t uid, gid_t gid, struct creds *cr)
{
	int	n;

	n = ldgroups(gid, &cr->creds_gids, &cr->creds_ngids);
	if (n < 0)
		return n;
	cr->creds_ngids = n;
	cr->creds_uid = uid;

	return 0;
}

static int
get_credentials(struct creds **crp, int effective)
{
	int	err;
	uid_t	uid;
	gid_t	gid;
	
	/*
	 * This is not a particularly efficient implementation. We
	 * don't expect it to ever be used in a thread-safe environment
	 * though, so expect no problems because of it.
	 */
	err = 0;
	mutex_lock(&_sysio_biglock);
	do {
		if (!_current_ucreds || _current_ucreds->refs) {
			_current_ucreds =
			    malloc(sizeof(struct user_credentials));
			if (!_current_ucreds) {
				err = -ENOMEM;
				break;
			}
			_current_ucreds->creds.creds_uid = -1;
			_current_ucreds->creds.creds_gids = NULL;
			_current_ucreds->creds.creds_ngids = 0;
			_current_ucreds->refs = 0;
		}
		if (effective) {
			uid = geteuid();
			gid = getegid();
		} else {
			uid = getuid();
			gid = getgid();
		}
		err =
		    ldcreds(uid,
			    gid,
			    &_current_ucreds->creds);
		if (err)
			break;
		_current_ucreds->refs++;
	} while (0);
	mutex_unlock(&_sysio_biglock);
	if (err)
		return err;
	*crp = &_current_ucreds->creds;
	return 0;
}

static void
release_ucreds(struct user_credentials *ucr)
{

	free(ucr->creds.creds_gids);
	free(ucr);
}

static void
put_credentials(struct creds *cr)
{
	struct user_credentials *ucr;

	mutex_lock(&_sysio_biglock);
	do {
		ucr = CONTAINER(user_credentials, creds, cr);
		assert(ucr->refs);
		if (--ucr->refs || ucr == _current_ucreds) {
			ucr = NULL;
			break;
		}
	} while (0);
	mutex_unlock(&_sysio_biglock);
	if (ucr)
		release_ucreds(ucr);
}

/*
 * Common permission check.
 */
int
_sysio_p_generic_perms_check(struct pnode *pno, int amode, int effective)
{
	int	err;
	struct creds *cr;

	do {
		if ((err = (*_sysio_get_credentials)(&cr, effective))) {
			cr = NULL;
			break;
		}
		assert(P_ISLOCKED(pno) &&
		       PB_ISLOCKED(pno->p_base) &&
		       pno->p_base->pb_ino && I_ISLOCKED(pno->p_base->pb_ino));
		err = (*_sysio_check_permission)(pno, cr, amode);
	} while (0);
	if (cr)
		(*_sysio_put_credentials)(cr);
	return err;
}

int
_sysio_epermitted(struct pnode *pno, int amode, int effective)
{

	return PNOP_PERMS_CHECK(pno, amode, effective);
}

/*
 * Determine if a given access is permitted to a given file.
 */
int
_sysio_permitted(struct pnode *pno, int amode)
{

	return _sysio_epermitted(pno, amode, 1);
}

#ifdef ZERO_SUM_MEMORY
/*
 * Clean up persistent resource on shutdown.
 */
void
_sysio_access_shutdown()
{

	if (_current_ucreds) {
		release_ucreds(_current_ucreds);
		_current_ucreds = NULL;
	}
}
#endif

int
SYSIO_INTERFACE_NAME(access)(const char *path, int amode)
{
	struct intent intent;
	int	err;
	struct pnode *pno;

	SYSIO_INTERFACE_DISPLAY_BLOCK;

	SYSIO_INTERFACE_ENTER(access, "%s%d", path, amode);

	do {
		INTENT_INIT(&intent, INT_GETATTR, NULL, NULL);
		err = _sysio_namei(_sysio_cwd, path, 0, &intent, &pno);
		if (err) {
			pno = NULL;
			break;
		}
		/*
		 * Check, using real IDs.
		 */
		err = _sysio_epermitted(pno, amode, 0);
	} while (0);
	if (pno)
		P_PUT(pno);
	SYSIO_INTERFACE_RETURN(err ? -1 : 0, err, access, "%d", 0);
}

#ifdef REDSTORM
#undef __access
sysio_sym_weak_alias(SYSIO_INTERFACE_NAME(access),
		     PREPEND(__, SYSIO_INTERFACE_NAME(access)))
#endif
