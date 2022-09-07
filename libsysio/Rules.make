if WITH_STDFD_DEV
STDFD_DEV_CPPFLAGS =-DSTDFD_DEV=1 -I$(top_srcdir)/dev/stdfd
else
STFD_DEV_CPPFLAGS =
endif
if WITH_STDDEV_DEV
STDDEV_DEV_CPPFLAGS =-DSTDDEV_DEV=1 -I$(top_srcdir)/dev
else
STDEV_DEV_CPPFLAGS =
endif

STD_CURVEFS_DRIVER_CPPFLAGS = -I$(top_srcdir)/include/libfuse -I$(top_srcdir)/include/libco -I$(top_srcdir)/include/atomic_queue -I$(top_srcdir)/drivers/curvefs

DEV_CPPFLAGS = $(STDFD_DEV_CPPFLAGS) $(STDDEV_DEV_CPPFLAGS) $(STD_CURVEFS_DRIVER_CPPFLAGS)

if WITH_THREAD_MODEL_POSIX
THREAD_MODEL_POSIX_COMPILER_FLAGS=-pthread
else
THREAD_MODEL_POSIX_COMPILER_FLAGS=
endif

AM_CPPFLAGS = \
	$(THREAD_MODEL_POSIX_COMPILER_FLAGS) \
	$(TRACING) \
	$(AUTOMOUNT) $(ZERO_SUM_MEMORY) $(DEV_CPPFLAGS) \
	$(DEFER_INIT_CWD) $(SYSIO_LABEL_NAMES) $(_HAVE_STATVFS) \
	-I$(top_srcdir)/include \
	-I/usr/local/include/iceoryx/v2.90.0/
