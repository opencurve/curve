include_directories(${Boost_INCLUDE_DIRS})
include_directories(${Libconfig_INCLUDE_DIRS})
include_directories(${CMock_INCLUDE_DIRS})

set(CPP_FLAGS "-DBRPC_WITH_GLOG=0 -DGFLAGS_NS=google")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPP_FLAGS} -std=c++0x -DNDEBUG -O2 \
    -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC \
    -fno-omit-frame-pointer")

set(RELOADUNIT_SRC test_reload.cpp test_reload.h TestReload.cpp)
add_executable(reload_unit ${RELOADUNIT_SRC})
target_link_libraries(reload_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS reload_unit DESTINATION bin)
add_test(NAME reload_unit COMMAND reload_unit)

set(RELOAD2UNIT_SRC test_reload2.cpp test_reload2.h TestReload2.cpp)
add_executable(reload2_unit ${RELOAD2UNIT_SRC})
target_link_libraries(reload2_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS reload2_unit DESTINATION bin)
add_test(NAME reload2_unit COMMAND reload2_unit)

set(COMMONUNIT_SRC test_common.cpp test_common.h TestCommon.cpp)
add_executable(common_unit ${COMMONUNIT_SRC})
target_link_libraries(common_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS common_unit DESTINATION bin)
add_test(NAME common_unit COMMAND common_unit)

set(RADOSUNIT_SRC test_rados.cpp test_rados.h TestRados.cpp)
add_executable(rados_unit ${RADOSUNIT_SRC})
target_link_libraries(rados_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS rados_unit DESTINATION bin)
add_test(NAME rados_unit COMMAND rados_unit)

set(RPCSERVERUNIT_SRC TestRpcServer.cpp)
add_executable(rpcserver_unit ${RPCSERVERUNIT_SRC})
target_link_libraries(rpcserver_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS rpcserver_unit DESTINATION bin)
add_test(NAME rpcserver_unit COMMAND rpcserver_unit)

set(RPCCEPHUNIT_SRC TestRpcCeph.cpp test_rpcceph.cpp test_rpcceph.h)
add_executable(rpcceph_unit ${RPCCEPHUNIT_SRC})
target_link_libraries(rpcceph_unit
                      nebdserver
                      gtest
                      gmock
                      rados
                      rbd)
install(TARGETS rpcceph_unit DESTINATION bin)
add_test(NAME rpcceph_unit COMMAND rpcceph_unit)

set(CONFIGUNIT_SRC TestConfig.cpp)
add_executable(config_unit ${CONFIGUNIT_SRC})
target_link_libraries(config_unit
                      nebdserver
                      gtest
                      gmock
                      rados
                      rbd)
install(TARGETS config_unit DESTINATION bin)
add_test(NAME config_unit COMMAND config_unit)

set(HEARTBEATUNIT_SRC TestHeartbeat.cpp test_heartbeat.cpp test_heartbeat.h)
add_executable(heartbeat_unit ${HEARTBEATUNIT_SRC})
target_link_libraries(heartbeat_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS heartbeat_unit DESTINATION bin)
add_test(NAME heartbeat_unit COMMAND heartbeat_unit)

set(HEARTBEATUNIT2_SRC TestHeartbeat2.cpp test_heartbeat2.cpp test_heartbeat2.h)
add_executable(heartbeat2_unit ${HEARTBEATUNIT2_SRC})
target_link_libraries(heartbeat2_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS heartbeat2_unit DESTINATION bin)
add_test(NAME heartbeat2_unit COMMAND heartbeat2_unit)

set(HEARTBEAT3UNIT_SRC TestHeartbeat3.cpp test_heartbeat3.cpp test_heartbeat3.h)
add_executable(heartbeat3_unit ${HEARTBEAT3UNIT_SRC})
target_link_libraries(heartbeat3_unit
                      nebdserver
                      gtest
                      gmock)
install(TARGETS heartbeat3_unit DESTINATION bin)
add_test(NAME heartbeat3_unit COMMAND heartbeat3_unit)
