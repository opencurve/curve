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

# libfake_client_service.so
set(FAKE_CLT_SRV_SRC fake_client_service.cpp fake_client_service.h)
set(FAKE_CLT_SRV_LINK
    brpc-shared
    client_proto
    gflags
    gtest
    gmock)
add_library(fake_client_service SHARED ${FAKE_CLT_SRV_SRC})
target_link_libraries(fake_client_service ${FAKE_CLT_SRV_LINK})
install(TARGETS fake_client_service DESTINATION lib)

# client_server
set(CLT_SERVER_SRC client_server.cpp)
set(CLT_SERVER_LINK fake_client_service jsoncpp_lib)
add_executable(client_server ${CLT_SERVER_SRC})
target_link_libraries(client_server ${CLT_SERVER_LINK})
install(TARGETS client_server DESTINATION bin)

# test_part1
set(TEST_PART1_SRC nebd_test.cpp mock_client_service.h)
set(TEST_PART1_LINK
    rt
    ssl
    crypto
    dl
    z
    pthread
    gflags
    gtest
    gmock
    nebd
    nebd_common
    client_proto)

add_executable(test_part1 ${TEST_PART1_SRC})
target_link_libraries(test_part1 ${TEST_PART1_LINK})
install(TARGETS test_part1 DESTINATION bin)
add_test(NAME test_part1
         COMMAND test_part1 -uuid 1234
         WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
