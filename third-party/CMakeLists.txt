
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
	PATCH_COMMAND patch -R -p1 < ${CMAKE_SOURCE_DIR}/thirdparties/glog/glog.patch
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_PREFIX=${ExtProj_DIR}/local
)

ExternalProject_Add(brpc                                                                 
	DOWNLOAD_DIR ${ExtProj_DOWNLOAD_DIR}/brpc
	PREFIX ${ExtProj_DIR}/brpc
    	GIT_REPOSITORY "https://github.com/apache/incubator-brpc"
	GIT_TAG "1b9e00641cbec1c8803da6a1f7f555398c954cb0"
	PATCH_COMMAND patch -R -p1 < ${CMAKE_SOURCE_DIR}/thirdparties/brpc/brpc.patch
	CMAKE_ARGS -DCMAKE_PREFIX_PATH=${ExtProj_DIR}/local -DCMAKE_INSTALL_INCLUDEDIR=${ExtProj_DIR}/local/include/brpc
		-DCMAKE_INSTALL_LIBDIR=${ExtProj_DIR}/local/lib
) 

add_dependencies(brpc leveldb gflags glog protobuf)
add_dependencies(glog gflags)
add_dependencies(protobuf gflags)
