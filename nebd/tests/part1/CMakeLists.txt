# 3rd party headers
include_directories("${Jsoncpp_INCLUDE_DIRS}")

set(CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -std=c++11 -pipe -W -Wall \
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

# libfake_heartbeat_service.so
#set(FAKE_HEARTBEAT_SRV_SRC fake_heartbeat_service.h)
#set(FAKE_HEARTBEAT_SRV_LINK
#    brpc-shared
#    client_proto
#    nebd_common
#    gflags
#    gtest
#    gmock)
#add_library(fake_heartbeat_service SHARED ${FAKE_HEARTBEAT_SRV_SRC})
#target_link_libraries(fake_heartbeat_service ${FAKE_HEARTBEAT_SRV_LINK})
#install(TARGETS fake_heartbeat_service DESTINATION lib)

set(MetaCacheTestSrc nebd_metacache_unittest.cpp)
set(MetaCacheTestLink gflags gtest nebd)
add_executable(nebd_metacache_unittest ${MetaCacheTestSrc})
target_link_libraries(nebd_metacache_unittest ${MetaCacheTestLink})
install(TARGETS nebd_metacache_unittest DESTINATION bin)
add_test(NAME nebd_metacache_unittest COMMAND nebd_metacache_unittest)

set(HeartbeatManagerTestSrc heartbeat_manager_unittest.cpp)
set(HeartbeatManagerTestLink gflags gtest nebd)
add_executable(heartbeat_manager_unittest ${HeartbeatManagerTestSrc})
target_link_libraries(heartbeat_manager_unittest ${HeartbeatManagerTestLink})
install(TARGETS heartbeat_manager_unittest DESTINATION bin)
add_test(NAME heartbeat_manager_unittest COMMAND heartbeat_manager_unittest)

set(NebdClientTestSrc nebd_client_unittest.cpp)
set(NebdClientTestLink gflags gtest nebd fake_file_service)
add_executable(nebd_client_unittest ${NebdClientTestSrc})
target_link_libraries(nebd_client_unittest ${NebdClientTestLink})
install(TARGETS nebd_client_unittest DESTINATION bin)
add_test(NAME nebd_client_unittest COMMAND nebd_client_unittest)