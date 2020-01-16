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

# libfake_file_service.so
set(FAKE_CLT_SRV_SRC fake_file_service.cpp fake_file_service.h)
set(FAKE_CLT_SRV_LINK
    brpc-shared
    client_proto
    gflags
    gtest
    gmock)
add_library(fake_file_service SHARED ${FAKE_CLT_SRV_SRC})
target_link_libraries(fake_file_service ${FAKE_CLT_SRV_LINK})
install(TARGETS fake_file_service DESTINATION lib)
