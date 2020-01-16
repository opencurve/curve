include_directories(${Boost_INCLUDE_DIRS})
include_directories(${Libconfig_INCLUDE_DIRS})
include_directories(${CMock_INCLUDE_DIRS})

set(CPP_FLAGS "-DBRPC_WITH_GLOG=0 -DGFLAGS_NS=google")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPP_FLAGS} -std=c++0x -DNDEBUG -O2 \
    -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC \
    -fno-omit-frame-pointer")
