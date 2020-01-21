# 3rd party header
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${Libconfig_INCLUDE_DIRS})
include_directories("${Jsoncpp_INCLUDE_DIRS}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} \
  -std=c++0x -pipe -W -Wall -Wno-unused-parameter -fPIC \
  -fno-omit-frame-pointer -momit-leaf-frame-pointer -msse4.2 \
  -pthread -Wsign-compare -Wno-unused-parameter \
  -Wno-unused-variable -Wno-missing-field-initializers \
  -Woverloaded-virtual -Wnon-virtual-dtor")
add_definitions(
  "-DGFLAGS=gflags -DOS_LINUX -DSNAPPY -DHAVE_ZLIB -DHAVE_SSE42 -DNDEBUG")

file(GLOB
     PART2_SRC
     "*.h"
     "*.cpp")

set(PART2_LINK
    nebd_common
    brpc-shared
    config
    crypto
    dl
    gflags
    gtest
    leveldb
    client_proto
    pthread
    rt
    snappy
    ssl
    z
    jsoncpp_lib)
    curve
    bthread
    glog
    )

add_executable(nebd-server ${PART2_SRC} ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(nebd-server ${PART2_LINK} ${EXTRALIBS})
install(TARGETS nebd-server DESTINATION bin)

# libnebdserver.so, only for test
add_library(nebdserver SHARED ${PART2_SRC} ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(nebdserver ${PART2_LINK} ${EXTRALIBS})

