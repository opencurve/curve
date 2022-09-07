AUTOMAKE_OPTIONS=1.6

if WITH_TESTS
TESTDIR = tests
else
TESTDIR =
endif

include $(top_srcdir)/src/module.mk 
include $(top_srcdir)/include/module.mk
include $(top_srcdir)/tests/module.mk
include $(top_srcdir)/dev/module.mk 
include $(top_srcdir)/dev/stdfd/module.mk 
include $(top_srcdir)/drivers/incore/module.mk 
include $(top_srcdir)/drivers/native/module.mk 
include $(top_srcdir)/drivers/curvefs/module.mk 
include $(top_srcdir)/misc/module.mk 

lib_LIBRARIES = ${LIBBUILD_DIR}/libsysio.a

if WITH_STDFD_DEV
OPTIONAL_STDFD_SRCS = $(STDFD_SRCS)
else
OPTIONAL_STDFD_SRCS =
endif

if WITH_STDDEV_DEV
OPTIONAL_STDDEV_SRCS = $(STDDEV_SRCS)
else
OPTIONAL_STDDEV_SRCS =
endif

if WITH_INCORE_DRIVER
OPTIONAL_INCORE_SRCS = $(INCORE_SRCS)
else
OPTIONAL_INCORE_SRCS =
endif

if WITH_NATIVE_DRIVER
OPTIONAL_NATIVE_SRCS = $(NATIVE_SRCS)
else
OPTIONAL_NATIVE_SRCS =
endif

if WITH_CURVEFS_DRIVER
OPTIONAL_CURVEFS_SRCS = $(CURVEFS_SRCS)
else
OPTIONAL_CURVEFS_SRCS =
endif


__LIBBUILD_DIR__libsysio_a_SOURCES = \
	$(SRCDIR_SRCS) \
	$(THREAD_MODEL_POSIX_SRCS) \
	$(MISCDIR_SRCS) \
 	$(OPTIONAL_STDDEV_SRCS) \
	$(OPTIONAL_STDFD_SRCS) \
	$(OPTIONAL_INCORE_SRCS) \
	$(OPTIONAL_NATIVE_SRCS) \ 
	$(OPTIONAL_CURVEFS_SRCS)

include $(top_srcdir)/Rules.make

EXTRA_DIST = Rules.make misc/init-env.sh $(TESTS_EXTRA) $(SRCDIR_EXTRA) \
	$(INCLUDE_EXTRA) $(STDDEV_EXTRA) $(STDFD_EXTRA) $(INCORE_EXTRA) \ 
	$(NATIVE_EXTRA) $(CURVEFS_EXTRA) 

really-clean: testsclean maintainer-clean
	-rm -rf autom4te-2.53.cache
	-rm -rf .deps
	-rm -f Makefile.in 
	-rm -f compile depcomp INSTALL install-sh missing mkinstalldirs \
		configure aclocal.m4 
	-rm -f config.guess config.sub
	-rm -rf $(LIBBUILD_DIR)
	-rm -f libsysio*.tar.gz
	cd $(TESTDIR); rm -rf Makefile Makefile.in .deps

tests: $(lib_LIBRARIES) FORCE
	cd $(TESTDIR); make
testsclean: FORCE
	cd $(TESTDIR); make clean
clean: testsclean clean-am
FORCE:
