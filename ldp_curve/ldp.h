#ifndef INCLUDE_LDP_H
#define INCLUDE_LDP_H

#include "ldp_glibc_curvefs.h"

#define LDP_ENV_CONFIGFILE "LDP_CONFIGFILE"

#define LDP_CLIENT_POOL 16

#define LDP_DEBUG_INIT 0

#define LDP_SID_BIT_MIN   5
#define LDP_SID_BIT_MAX   30

#define LDP_APP_NAME_MAX  256

#define PREDICT_FALSE(x) __builtin_expect((x),0)
#define PREDICT_TRUE(x) __builtin_expect((x),1)

//#define CURVEFS_TEST_DEBUG

#ifdef CURVEFS_TEST_DEBUG

#define LDBG(_lvl, fmt, ...) 	\
	do {									\
		fprintf (stderr, " file=%s func=%s line=%d ", __FILE__, __func__, __LINE__); 	\
		fprintf (stderr, fmt "\n", ##__VA_ARGS__);	\
	} while (0)

#else

#define LDBG(_lvl, fmt, ...) 	\
	do { } while (0)

#endif

/* use poll to improve the request latency */
//#define CURVEFS_CLIENT_POLL

#endif
