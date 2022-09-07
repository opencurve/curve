AUTOMAKE_OPTIONS=foreign

ACLOCAL_AMFLAGS=-I m4

lib_LTLIBRARIES = ${LIBBUILD_DIR}/libldpcurve.la
#lib_LIBRARIES = libldpcurve.a

LIBS=-L/usr/local/lib/ -liceoryx_binding_c -ldl -lgflags 

__LIBBUILD_DIR__libldpcurve_la_SOURCES = ldp.cpp ldp_curvefs_wrapper.cpp
#__LIBBUILD_DIR__libldpcurve_la_LIBADD=$(LIBS) 
#__LIBBUILD_DIR__libldpcurve_la_DEPENDENCIES = libiceoryx_binding_c.a 

AM_CPPFLAGS = -I/usr/local/include/iceoryx/v2.90.0/
