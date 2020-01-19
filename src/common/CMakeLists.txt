# libnebd_common.so
file(GLOB COMMON_SRC
    "*.h"
    "*.cpp"
)

file (GLOB PROTO_FILES
    "*.proto"
)

set(COMMON_LINK
    brpc-shared)

add_library(nebd_common SHARED ${COMMON_SRC})
install(TARGETS nebd_common DESTINATION lib)
target_link_libraries(nebd_common ${COMMON_LINK} ${EXTRALIBS})

# libclient_proto.so
include(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER ${PROTO_FILES})
add_library(client_proto ${PROTO_HEADER} ${PROTO_SRC})
install(TARGETS client_proto DESTINATION lib)
