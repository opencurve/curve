cmake_minimum_required(VERSION 3.15.0)

project(nebd)
set(VERSION 1.0.0)
# LINUX ONLY

include(GNUInstallDirs)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

# put all the libs and binaries in one place
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_REQUIRED_LIBRARIES pthread)
find_package(Threads REQUIRED)

option(NEBD_DEBUG_ENABLE "enable/disable debug" OFF)
option(NEBD_COVERAGE_ENABLE "enable/disable coverage" OFF)

# we use gcc-4.9-backport, so we need to set rpath for all projects
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "/usr/lib/gcc-4.9-backport/lib/;\
  ${CMAKE_INSTALL_PREFIX}/lib;${CMAKE_BINARY_DIR}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# google test headers
set(GMOCK_INCLUDE_DIRS
    "${CMAKE_SOURCE_DIR}/3rdparty/googletest/googlemock/include/gmock")
set(GTEST_INCLUDE_DIRS
    "${CMAKE_SOURCE_DIR}/3rdparty/googletest/googletest/include/gtest")
include_directories("${CMAKE_SOURCE_DIR}/3rdparty/googletest/googlemock/include")
include_directories("${CMAKE_SOURCE_DIR}/3rdparty/googletest/googletest/include")

add_subdirectory(3rdparty)
add_subdirectory(src)
enable_testing()
add_subdirectory(tests)
