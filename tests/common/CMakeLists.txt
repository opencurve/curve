set(CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -std=c++0x -pipe -W -Wall \
  -Wno-unused-parameter -fPIC -fno-omit-frame-pointer \
  -momit-leaf-frame-pointer -msse4.2 \
  -pthread -Wsign-compare -Wno-unused-parameter \
  -Wno-unused-variable  -Wno-missing-field-initializers \
  -Woverloaded-virtual -Wnon-virtual-dtor")
add_definitions(
  "-DGFLAGS=gflags -DOS_LINUX -DSNAPPY -DHAVE_ZLIB -DHAVE_SSE42 -DNDEBUG")

# test_configuration
set(TEST_CONF_SRC configuration_test.cpp)
set(TEST_CONF_LINK nebd_common gflags gtest gmock)
add_executable(test_configuration ${TEST_CONF_SRC})
target_link_libraries(test_configuration ${TEST_CONF_LINK})
install(TARGETS test_configuration DESTINATION bin)
add_test(NAME test_configuration COMMAND test_configuration)
