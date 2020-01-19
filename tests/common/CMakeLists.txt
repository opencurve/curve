set(CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -std=c++0x -pipe -W -Wall \
  -Wno-unused-parameter -fPIC -fno-omit-frame-pointer \
  -momit-leaf-frame-pointer -msse4.2 \
  -pthread -Wsign-compare -Wno-unused-parameter \
  -Wno-unused-variable  -Wno-missing-field-initializers \
  -Woverloaded-virtual -Wnon-virtual-dtor")
add_definitions(
  "-DGFLAGS=gflags -DOS_LINUX -DSNAPPY -DHAVE_ZLIB -DHAVE_SSE42 -DNDEBUG")

# COMMON_TEST_SRC
file(GLOB COMMON_TEST_SRC
    "*.h"
    "*.cpp"
)

# common_test
set(COMMON_TEST_LINK nebd_common gflags gtest gmock)
add_executable(common_test ${COMMON_TEST_SRC})
target_link_libraries(common_test ${COMMON_TEST_LINK})
install(TARGETS common_test DESTINATION bin)
add_test(NAME common_test COMMAND common_test)

