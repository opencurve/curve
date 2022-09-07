if WITH_STATVFS
STATVFS_PROG=test_statvfs
else
STATVFS_PROG=
endif

if WITH_FILE_HANDLE_INTERFACE
FHI_PROG=test_fhichmod test_fhicreate test_fhilink test_fhilist test_fhimkdir \
	test_fhipath test_fhirename test_fhirmdir \
	test_fhisymlink test_fhitrunc test_fhiunlink
else
FHI_PROG=
endif

noinst_PROGRAMS = curvefs_test curvefs_server

CLEANFILES=drv_data.c

if WITH_NATIVE_DRIVER
NATIVE_DRIVER_NAME=native
NATIVE_DRIVER_CFLAGS= -I$(top_srcdir)/drivers/native
else
NATIVE_DRIVER_NAME=
NATIVE_DRIVER_CFLAGS=
endif

if WITH_INCORE_DRIVER
INCORE_DRIVER_NAME=incore
INCORE_DRIVER_CFLAGS= -I$(top_srcdir)/drivers/incore
else
INCORE_DRIVER_NAME=
INCORE_DRIVER_CFLAGS=
endif

CURVEFS_DRIVER_NAME=curvefs
CURVEFS_DRIVER_CFLAGS= -I$(top_srcdir)/drivers/curvefs

DRIVERS=$(NATIVE_DRIVER_NAME) $(INCORE_DRIVER_NAME) $(STFD_DEV_NAME) 

CMNSRC=startup.c drv_init_all.c drv_data.c

BUILT_SOURCES=drv_data.c

CFL=$(AM_CFLAGS) $(AM_CPPFLAGS) \
	$(NATIVE_DRIVER_CFLAGS) $(INCORE_DRIVER_CFLAGS) \
	$(STDFD_DEV_CFLAGS) $(CURVEFS_DRIVER_CFLAGS)

LIBS=$(LIBBUILD_DIR)/libsysio.a -lcolib -ldl -lpthread -liceoryx_binding_c /usr/local/lib64/libfuse3.so.3.11.0 

curvefs_test_SOURCES=curvefs_test.cpp curvefs_startup.cpp curvefs_mount.cpp passthrough_ll.cpp $(CMNSRC)
curvefs_test_CFLAGS=$(CFL)
curvefs_test_LDFLAGS=-L/usr/local/lib64 -L/usr/lib64
curvefs_test_DEPENDENCIES=$(LIBS)

curvefs_server_SOURCES=curvefs_server.cpp curvefs_startup.cpp curvefs_mount.cpp passthrough_ll.cpp reqparse.cpp $(CMNSRC)
curvefs_server_CFLAGS=$(CFL)
curvefs_server_LDFLAGS=-L/usr/local/lib64 -L/usr/lib64
curvefs_server_DEPENDENCIES=$(LIBS)

CMNFHISRC=fhi_support.c

drv_data.c: $(CONFIG_DEPENDENCIES) $(top_srcdir)/tests/gendrvdata.sh
	test -z "drv_data.c" && rm -f drv_data.c; \
	$(SHELL) $(top_srcdir)/tests/gendrvdata.sh $(DRIVERS) > drv_data.c

lib_LIBRARIES=libruntime.a

libruntime_a_SOURCES=sysio-run-start.c startup.c drv_init_all.c drv_data.c

AM_CFLAGS = -L$(LIBBUILD_DIR)
include $(top_srcdir)/Rules.make
