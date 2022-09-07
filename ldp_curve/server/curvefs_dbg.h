#ifndef CURVEFS_DBG_H
#define CURVEFS_DBG_H

#include <unistd.h>

#define SERVER_DEBUG_LEVEL 1

#define LDBG1(_lvl, _fmt, _args...) 					                    \
    if (SERVER_DEBUG_LEVEL > _lvl) {									        \
        int errno_saved = errno;						                \
        fprintf (stderr, "" _fmt "", ##_args);	    \
        errno = errno_saved;						                    \
    }

#define LDBG(_lvl, _fmt, _args...) 					                    \
    if (SERVER_DEBUG_LEVEL > _lvl) {									        \
        int errno_saved = errno;						                \
        fprintf (stderr, "curvefs_fuse <%d>: " _fmt "\n", getpid(), ##_args);	    \
        errno = errno_saved;						                    \
    }

#endif
