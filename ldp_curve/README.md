# the client of the FUSE client
sh autogen.sh
./configure
make

/* FOR TEST */
LD_PRELOAD=.././lib/.libs/libldpcurve.so  ./test_file_opt

/* configure in ldp.h */
/* debug options on */
#define CURVEFS_TEST_DEBUG

/* use poll to improve the request latency */
#define CURVEFS_CLIENT_POLL
