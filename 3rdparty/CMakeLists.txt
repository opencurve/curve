# we need shared library
option(BUILD_SHARED_LIBS "enable shared library" ON)

include(BuildLibconfig)
build_libconfig()
add_subdirectory(brpc)
add_subdirectory(googletest)
add_subdirectory(jsoncpp)

install(TARGETS brpc-shared DESTINATION lib)
install(TARGETS jsoncpp_lib DESTINATION lib)
