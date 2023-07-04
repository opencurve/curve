/*
 *  Copyright (c) 2023 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Curve
 * Created Date: 2023-09-20
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_TEST_CLIENT_VFS_HELPER_MOCK_FUSE_CLIENT_H_
#define CURVEFS_TEST_CLIENT_VFS_HELPER_MOCK_FUSE_CLIENT_H_

#include <gmock/gmock.h>

#include <string>

#include "curvefs/src/client/fuse_client.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::curvefs::client::FuseClient;

class MockFuseClient : public FuseClient {
 public:
    MockFuseClient() {}
    ~MockFuseClient() {}

    MOCK_METHOD1(Init, CURVEFS_ERROR(const FuseClientOption&));

    MOCK_METHOD0(UnInit, void());

    MOCK_METHOD0(Run, CURVEFS_ERROR());

    MOCK_METHOD0(Fini, void());

    MOCK_METHOD2(FuseOpInit, CURVEFS_ERROR(void*, struct fuse_conn_info*));

    MOCK_METHOD1(FuseOpDestroy, void(void*));

    MOCK_METHOD7(FuseOpWrite, CURVEFS_ERROR(fuse_req_t,
                                            fuse_ino_t,
                                            const char*,
                                            size_t,
                                            off_t,
                                            struct fuse_file_info*,
                                            FileOut*));

    MOCK_METHOD7(FuseOpRead, CURVEFS_ERROR(fuse_req_t,
                                           fuse_ino_t,
                                           size_t,
                                           off_t,
                                           struct fuse_file_info*,
                                           char*,
                                           size_t*));

    MOCK_METHOD4(FuseOpLookup, CURVEFS_ERROR(fuse_req_t,
                                             fuse_ino_t,
                                             const char*,
                                             EntryOut*));

    MOCK_METHOD4(FuseOpOpen, CURVEFS_ERROR(fuse_req_t,
                                           fuse_ino_t,
                                           struct fuse_file_info*,
                                           FileOut*));

    MOCK_METHOD6(FuseOpCreate, CURVEFS_ERROR(fuse_req_t,
                                             fuse_ino_t,
                                             const char*,
                                             mode_t,
                                             struct fuse_file_info*,
                                             EntryOut*));

     MOCK_METHOD6(FuseOpMkNod, CURVEFS_ERROR(fuse_req_t,
                                             fuse_ino_t,
                                             const char*,
                                             mode_t,
                                             dev_t,
                                             EntryOut*));

    MOCK_METHOD5(FuseOpMkDir, CURVEFS_ERROR(fuse_req_t,
                                            fuse_ino_t,
                                            const char*,
                                            mode_t,
                                            EntryOut*));

    MOCK_METHOD3(FuseOpUnlink, CURVEFS_ERROR(fuse_req_t,
                                             fuse_ino_t,
                                             const char*));

    MOCK_METHOD3(FuseOpRmDir, CURVEFS_ERROR(fuse_req_t,
                                            fuse_ino_t,
                                            const char*));

    MOCK_METHOD3(FuseOpOpenDir, CURVEFS_ERROR(fuse_req_t,
                                              fuse_ino_t,
                                              struct fuse_file_info*));

    MOCK_METHOD3(FuseOpReleaseDir, CURVEFS_ERROR(fuse_req_t,
                                                 fuse_ino_t,
                                                 struct fuse_file_info*));

    MOCK_METHOD8(FuseOpReadDir, CURVEFS_ERROR(fuse_req_t,
                                              fuse_ino_t,
                                              size_t,
                                              off_t,
                                              struct fuse_file_info*,
                                              char**,
                                              size_t*,
                                              bool));

    MOCK_METHOD6(FuseOpRename, CURVEFS_ERROR(fuse_req_t,
                                             fuse_ino_t,
                                             const char*,
                                             fuse_ino_t,
                                             const char*,
                                             unsigned int));

    MOCK_METHOD4(FuseOpGetAttr, CURVEFS_ERROR(fuse_req_t,
                                              fuse_ino_t,
                                              struct fuse_file_info*,
                                              struct AttrOut*));

    MOCK_METHOD6(FuseOpSetAttr, CURVEFS_ERROR(fuse_req_t,
                                              fuse_ino_t,
                                              struct stat*,
                                              int,
                                              struct fuse_file_info*,
                                              struct AttrOut*));

    MOCK_METHOD5(FuseOpGetXattr, CURVEFS_ERROR(fuse_req_t,
                                               fuse_ino_t,
                                               const char*,
                                               std::string*,
                                               size_t));

    MOCK_METHOD6(FuseOpSetXattr, CURVEFS_ERROR(fuse_req_t,
                                               fuse_ino_t,
                                               const char*,
                                               const char*,
                                               size_t,
                                               int));

    MOCK_METHOD5(FuseOpListXattr, CURVEFS_ERROR(fuse_req_t,
                                                fuse_ino_t,
                                                char*,
                                                size_t,
                                                size_t*));

    MOCK_METHOD5(FuseOpSymlink, CURVEFS_ERROR(fuse_req_t,
                                              const char*,
                                              fuse_ino_t,
                                              const char*,
                                              EntryOut*));

    MOCK_METHOD5(FuseOpLink, CURVEFS_ERROR(fuse_req_t,
                                           fuse_ino_t,
                                           fuse_ino_t,
                                           const char*,
                                           EntryOut*));

    MOCK_METHOD3(FuseOpReadLink, CURVEFS_ERROR(fuse_req_t,
                                               fuse_ino_t,
                                               std::string*));

    MOCK_METHOD3(FuseOpRelease, CURVEFS_ERROR(fuse_req_t,
                                              fuse_ino_t,
                                              struct fuse_file_info*));

    MOCK_METHOD4(FuseOpFsync, CURVEFS_ERROR(fuse_req_t,
                                            fuse_ino_t,
                                            int,
                                            struct fuse_file_info*));

    MOCK_METHOD3(FuseOpFlush, CURVEFS_ERROR(fuse_req_t,
                                            fuse_ino_t,
                                            struct fuse_file_info*));

    MOCK_METHOD3(FuseOpStatFs, CURVEFS_ERROR(fuse_req_t,
                                             fuse_ino_t,
                                             struct statvfs*));

    MOCK_METHOD2(Truncate, CURVEFS_ERROR(InodeWrapper*, uint64_t));

    MOCK_METHOD0(FlushAll, void());

 private:
    MOCK_METHOD0(FlushData, void());
};

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_VFS_HELPER_MOCK_FUSE_CLIENT_H_
