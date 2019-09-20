# 3rd party headers
include_directories("${Jsoncpp_INCLUDE_DIRS}")

set(CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -std=c++0x -pipe -W -Wall \
  -Wno-unused-parameter -fPIC -fno-omit-frame-pointer \
  -momit-leaf-frame-pointer -msse4.2 \
  -pthread -Wsign-compare -Wno-unused-parameter \
  -Wno-unused-variable  -Wno-missing-field-initializers \
  -Woverloaded-virtual -Wnon-virtual-dtor")
add_definitions(
  "-DGFLAGS=gflags -DOS_LINUX -DSNAPPY -DHAVE_ZLIB -DHAVE_SSE42 -DNDEBUG")

# libnebd.so
set(LIBNEBD_SRC
    libnebd.cpp
    libnebd.h
    libnebd_file.cpp
    libnebd_file.h
    nebd_client.cpp
    nebd_client.h
    nebd_lifecycle.cpp
    nebd_lifecycle.h)
set(LIBNEBD_LINK
    brpc-shared
    rt
    ssl
    crypto
    dl
    z
    pthread
    protobuf
    gflags
    jsoncpp_lib)
add_library(nebd SHARED ${LIBNEBD_SRC} ${PROTO_SRCS} ${PROTO_HDRS})
add_dependencies(nebd jsoncpp_lib brpc-shared)
target_link_libraries(nebd ${LIBNEBD_LINK} ${EXTRALIBS})
install(TARGETS nebd DESTINATION lib)