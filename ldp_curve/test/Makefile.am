CFL=$(AM_CFLAGS) $(AM_CPPFLAGS)

noinst_PROGRAMS = test_access test_gflags test_allocsize test_fork test_file_opt

#LIBS= -L/usr/local/lib/ -liceoryx_binding_c $(LIBBUILD_DIR)/libldpcurve.la 
#LIBS= -L/usr/local/lib/ $(LIBBUILD_DIR)/libldpcurve.la -liceoryx_binding_c -ldl -lgflags 

test_access_SOURCES=test_access.cpp
test_access_CFLAGS=$(CFS)
#test_access_LDADD=$(LIBS)

test_gflags_SOURCES=test_gflags.cpp
test_gflags_CFLAGS=$(CFS)
test_gflags_LDADD= -lgflags

test_allocsize_SOURCES=test_allocsize.cpp
test_allocsize_CFLAGS=$(CFS)

test_fork_SOURCES=test_fork.cpp
test_fork_CFLAGS=$(CFS)
test_fork_LDADD = $(LIBS)

test_file_opt_SOURCES=test_file_opt.cpp
test_file_opt_CFLAGS=$(CFS)
test_file_opt_LDADD=$(LIBS)