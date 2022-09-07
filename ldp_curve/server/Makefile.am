
AM_CPPFLAGS = -I/usr/local/include/iceoryx/v2.90.0/

CFL=$(AM_CFLAGS) $(AM_CPPFLAGS)

noinst_PROGRAMS = curvefs_server

#LIBS= -L/usr/local/lib/ -liceoryx_binding_c $(LIBBUILD_DIR)/libldpcurve.la 
#LIBS= -L/usr/local/lib/ $(LIBBUILD_DIR)/libldpcurve.la -liceoryx_binding_c -ldl -lgflags 

curvefs_server_SOURCES=curvefs_server.cpp
curvefs_server_CFLAGS=$(CFS)
curvefs_server_LDADD= -L/usr/local/lib/ -liceoryx_binding_c -lgflags
