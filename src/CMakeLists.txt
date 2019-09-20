enable_language(CXX)

# require c++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTRALIBS "")

# coverage option
if(${NEBD_COVERAGE_ENABLE})
  find_program(HAVE_GCOV gcov)
  if(NOT HAVE_GCOV)
    message(FATAL_ERROR "Coverage Enabled but gcov Not Found")
  endif(NOT HAVE_GCOV)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage -O0")
  list(APPEND EXTRALIBS gcov)
endif(${NEBD_COVERAGE_ENABLE})

# debug option
if(${NEBD_DEBUG_ENABLE})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g3 -gdwarf-4")
else(${NEBD_DEBUG_ENABLE})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")
endif(${NEBD_DEBUG_ENABLE})

# set 3rd party headers
set(Boost_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/3rdparty/boost")
set(Brpc_INCLUDE_DIRS "${CMAKE_BINARY_DIR}/3rdparty/brpc/output/include")
set(Jsoncpp_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/3rdparty/jsoncpp/include")
set(Libconfig_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/3rdparty/libconfig/lib")

# brpc
include_directories(${Brpc_INCLUDE_DIRS})
# our header
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

add_subdirectory(common)
add_subdirectory(part1)
add_subdirectory(part2)
