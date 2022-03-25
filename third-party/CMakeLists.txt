
set(ExtProj_DIR ${CMAKE_BINARY_DIR}/third-party)
set(ExtProj_DOWNLOAD_DIR ${CMAKE_CURRENT_LIST_DIR}/download)

ExternalProject_Add(leveldb
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/leveldb
	PREFIX ${ExtProj_DIR}/leveldb
	URL "https://github.com/google/leveldb/archive/a53934a3ae1244679f812d998a4f16f2c7f309a6.tar.gz"
	URL_HASH MD5=458aba294697d3fa1367a1916e4a72c0
	BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND ""
	BUILD_COMMAND make -j
	PATCH_COMMAND patch < ${CMAKE_CURRENT_LIST_DIR}/patches/leveldb/leveldb.patch
	INSTALL_COMMAND sh ${CMAKE_CURRENT_LIST_DIR}/install_leveldb.sh ${ExtProj_DIR}/leveldb/src/leveldb ${ExtProj_DIR}/local
)

ExternalProject_Add(gtest
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/gtest
	PREFIX ${ExtProj_DIR}/gtest
	GIT_REPOSITORY "https://github.com/google/googletest"
	GIT_TAG "release-1.8.0"
	GIT_PROGRESS 1
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(protobuf
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/protobuf
	PREFIX ${ExtProj_DIR}/protobuf
	URL "https://github.com/google/protobuf/archive/v3.5.0.zip"
	URL_HASH MD5=9e5839297cff6a60a9bcf8c26f0d1f35
	SOURCE_SUBDIR cmake
	CMAKE_ARGS -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(gflags
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/gflags
	PREFIX ${ExtProj_DIR}/gflags
	URL "https://mirror.bazel.build/github.com/gflags/gflags/archive/v2.2.2.tar.gz"
	URL "https://github.com/gflags/gflags/archive/v2.2.2.tar.gz"
	URL_HASH MD5=1a865b93bacfa963201af3f75b7bd64c
	CMAKE_ARGS -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(glog
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/gflags
	PREFIX ${ExtProj_DIR}/glog
	GIT_REPOSITORY "https://github.com/google/glog"
    	GIT_TAG "4cc89c9e2b452db579397887c37f302fb28f6ca1"
	GIT_PROGRESS 1
	PATCH_COMMAND git reset --hard
	COMMAND git clean -d -f
	COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/thirdparties/glog/glog.patch
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(brpc
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/brpc
	PREFIX ${ExtProj_DIR}/brpc
    	GIT_REPOSITORY "https://github.com/apache/incubator-brpc"
	GIT_TAG "1b9e00641cbec1c8803da6a1f7f555398c954cb0"
	GIT_PROGRESS 1
	PATCH_COMMAND git reset --hard
	COMMAND git clean -d -f
	COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/thirdparties/brpc/brpc.patch
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_INCLUDEDIR=${ExtProj_DIR}/local/include/brpc
		-DCMAKE_INSTALL_LIBDIR=${ExtProj_DIR}/local/lib
)

add_dependencies(brpc leveldb gflags glog protobuf)
add_dependencies(glog gflags gtest)
add_dependencies(protobuf gflags gtest)

ExternalProject_Add(jsoncpp
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/jsoncpp
	PREFIX ${ExtProj_DIR}/jsoncpp
    	GIT_REPOSITORY "https://github.com/open-source-parsers/jsoncpp.git"
	GIT_TAG "1.8.4"
	GIT_PROGRESS 1
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(aws
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/aws
	PREFIX ${ExtProj_DIR}/aws
	URL "https://github.com/awslabs/aws-c-common/archive/v0.4.29.tar.gz"
        URL "https://mirror.tensorflow.org/github.com/awslabs/aws-c-common/archive/v0.4.29.tar.gz"
	URL_HASH SHA256=01c2a58553a37b3aa5914d9e0bf7bf14507ff4937bc5872a678892ca20fcae1f
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(aws_checksums
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/aws_checksums
	PREFIX ${ExtProj_DIR}/aws_checksums
	URL "https://github.com/awslabs/aws-checksums/archive/v0.1.5.tar.gz"
	URL "https://mirror.tensorflow.org/github.com/awslabs/aws-checksums/archive/v0.1.5.tar.gz"
	URL_HASH SHA256=6e6bed6f75cf54006b6bafb01b3b96df19605572131a2260fddaf0e87949ced0
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(aws_c_event_stream
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/aws_c_event_stream
	PREFIX ${ExtProj_DIR}/aws_c_event_stream
	URL "https://github.com/awslabs/aws-c-event-stream/archive/v0.1.4.tar.gz"
	URL "https://mirror.tensorflow.org/github.com/awslabs/aws-c-event-stream/archive/v0.1.4.tar.gz"
	URL_HASH SHA256=31d880d1c868d3f3df1e1f4b45e56ac73724a4dc3449d04d47fc0746f6f077b6
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

add_dependencies(aws_c_event_stream aws_checksums)

ExternalProject_Add(google_absl
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/google_absl
	PREFIX ${ExtProj_DIR}/google_absl
	URL "https://github.com/abseil/abseil-cpp/archive/refs/tags/20210324.2.tar.gz"
	URL_HASH SHA256=59b862f50e710277f8ede96f083a5bb8d7c9595376146838b9580be90374ee1f
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

