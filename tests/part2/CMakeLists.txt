include_directories(${Boost_INCLUDE_DIRS})
include_directories(${Libconfig_INCLUDE_DIRS})
include_directories(${CMock_INCLUDE_DIRS})

set(CPP_FLAGS "-DBRPC_WITH_GLOG=0 -DGFLAGS_NS=google")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPP_FLAGS} -std=c++0x -DNDEBUG -O2 \
    -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC \
    -fno-omit-frame-pointer")

set(PART2_TEST_LINK
    nebdserver
    gtest
    gmock)

# libpart2mock.so
set(PART2_MOCK_SRC
    mock_metafile_manager.h
    mock_request_executor.h)
add_library(part2mock ${PART2_MOCK_SRC})
set_target_properties(part2mock PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(part2mock
                      ${PART2_TEST_LINK})

# filerecordmap_test
set(RECORD_MAP_TEST_SRC file_record_map_unittest.cpp)
add_executable(filerecordmap_test ${RECORD_MAP_TEST_SRC})
target_link_libraries(filerecordmap_test
                      ${PART2_TEST_LINK})
install(TARGETS filerecordmap_test DESTINATION bin)
add_test(NAME filerecordmap_test COMMAND filerecordmap_test)

# filemanager_test
set(FILE_MANAGER_TEST_SRC file_manager_unittest.cpp)
add_executable(filemanager_test ${FILE_MANAGER_TEST_SRC})
target_link_libraries(filemanager_test
                      part2mock
                      ${PART2_TEST_LINK})
install(TARGETS filemanager_test DESTINATION bin)
add_test(NAME filemanager_test COMMAND filemanager_test)