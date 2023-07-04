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

#ifndef CURVEFS_TEST_CLIENT_VFS_HELPER_EXPECT_H_
#define CURVEFS_TEST_CLIENT_VFS_HELPER_EXPECT_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <memory>

#include "curvefs/test/client/vfs/helper/mock_fuse_client.h"

namespace curvefs {
namespace client {
namespace vfs {

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;


#define OK(rc) ASSERT_EQ(rc, CURVEFS_ERROR::OK);
#define BAD_FD(rc) ASSERT_EQ(rc, CURVEFS_ERROR::BAD_FD);
#define EXIST(rc) ASSERT_EQ(rc, CURVEFS_ERROR::EXIST);
#define NOT_EXIST(rc) ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
#define NOT_PERMISSION(rc) ASSERT_EQ(rc, CURVEFS_ERROR::NO_PERMISSION);


// return
#define EXPECT_CALL_RETURN3(obj, call, code) \
do {                                         \
    EXPECT_CALL(obj, call(_, _, _))          \
        .WillOnce(Return(code));             \
} while (0)

#define EXPECT_CALL_RETURN4(obj, call, code) \
do {                                         \
    EXPECT_CALL(obj, call(_, _, _, _))       \
        .WillOnce(Return(code));             \
} while (0)

#define EXPECT_CALL_RETURN5(obj, call, code) \
do {                                         \
    EXPECT_CALL(obj, call(_, _, _, _, _))    \
        .WillOnce(Return(code));             \
} while (0)

#define EXPECT_CALL_RETURN6(obj, call, code) \
do {                                         \
    EXPECT_CALL(obj, call(_, _, _, _, _, _)) \
        .WillOnce(Return(code));             \
} while (0)

#define EXPECT_CALL_RETURN7(obj, call, code)    \
do {                                            \
    EXPECT_CALL(obj, call(_, _, _, _, _, _, _)) \
        .WillOnce(Return(code));                \
} while (0)


// invoke
#define EXPECT_CALL_INVOKE3(obj, call, fn) \
do {                                       \
    EXPECT_CALL(obj, call(_, _, _))        \
        .WillOnce(Invoke(fn));             \
} while (0)

#define EXPECT_CALL_INVOKE4(obj, call, fn) \
do {                                       \
    EXPECT_CALL(obj, call(_, _, _, _))     \
        .WillOnce(Invoke(fn));             \
} while (0)

#define EXPECT_CALL_INVOKE5(obj, call, fn) \
do {                                       \
    EXPECT_CALL(obj, call(_, _, _, _, _))  \
        .WillOnce(Invoke(fn));             \
} while (0)

#define EXPECT_CALL_INVOKE6(obj, call, fn)   \
do {                                         \
    EXPECT_CALL(obj, call(_, _, _, _, _, _)) \
        .WillOnce(Invoke(fn));               \
} while (0)

#define EXPECT_CALL_INVOKE7(obj, call, fn)      \
do {                                            \
    EXPECT_CALL(obj, call(_, _, _, _, _, _, _)) \
        .WillOnce(Invoke(fn));                  \
} while (0)





















#define EXPECT_CALL_RETURN_FuseOpMkDir(CLIENT, CODE) \
do {                                                 \
    EXPECT_CALL(CLIENT, FuseOpMkDir(_, _, _, _, _))  \
        .WillOnce(Return(CODE));                     \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpRmDir(CLIENT, CODE) \
do {                                                 \
    EXPECT_CALL(CLIENT, FuseOpRmDir(_, _, _))        \
        .WillOnce(Return(CODE));                     \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpOpenDir(CLIENT, CODE) \
do {                                                   \
    EXPECT_CALL(CLIENT, FuseOpOpenDir(_, _, _))        \
        .WillOnce(Return(CODE));                       \
} while (0)

#define EXPECT_CALL_RETURN_FuseReadDir(CLIENT, CODE) \
do {                                                 \
    EXPECT_CALL(CLIENT, FuseReadDir(_, _, _))        \
        .WillOnce(Return(CODE));                     \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpReleaseDir(CLIENT, CODE) \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpReleaseDir(_, _, _))        \
        .WillOnce(Return(CODE));                          \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpCreate(CLIENT, CODE)   \
do {                                                    \
    EXPECT_CALL(CLIENT, FuseOpCreate(_, _, _, _, _, _)) \
        .WillOnce(Return(CODE));                        \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpOpen(CLIENT, CODE) \
do {                                                \
    EXPECT_CALL(CLIENT, FuseOpOpen(_, _, _, _))     \
        .WillOnce(Return(CODE));                    \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpRead(CLIENT, CODE)     \
do {                                                     \
    EXPECT_CALL(CLIENT, FuseOpRead(_, _, _, _, _, _, _)) \
        .WillOnce(Return(CODE));                         \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpWrite(CLIENT, CODE)      \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpWrite(_, _, _, _, _, _, _)) \
        .WillOnce(Return(CODE));                          \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpFlush(CLIENT, CODE) \
do {                                                 \
    EXPECT_CALL(CLIENT, FuseOpFlush(_, _, _))        \
        .WillOnce(Return(CODE));                     \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpRelease(CLIENT, CODE) \
do {                                                   \
    EXPECT_CALL(CLIENT, FuseOpRelease(_, _, _))        \
        .WillOnce(Return(CODE));                       \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpUnlink(CLIENT, CODE) \
do {                                                  \
    EXPECT_CALL(CLIENT, FuseOpUnlink(_, _, _))        \
        .WillOnce(Return(CODE));                      \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpStatFs(CLIENT, CODE) \
do {                                                  \
    EXPECT_CALL(CLIENT, FuseOpStatFs(_, _, _))        \
        .WillOnce(Return(CODE));                      \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpLookup(CLIENT, CODE) \
do {                                                  \
    EXPECT_CALL(CLIENT, FuseOpLookup(_, _, _, _))     \
        .WillOnce(Return(CODE));                      \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpGetAttr(CLIENT, CODE) \
do {                                                   \
    EXPECT_CALL(CLIENT, FuseOpGetAttr(_, _, _, _))     \
        .WillOnce(Return(CODE));                       \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpSetAttr(CLIENT, CODE)   \
do {                                                     \
    EXPECT_CALL(CLIENT, FuseOpSetAttr(_, _, _, _, _, _)) \
        .WillOnce(Return(CODE));                         \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpReadLink(CLIENT, CODE) \
do {                                                    \
    EXPECT_CALL(CLIENT, FuseOpReadLink(_, _, _))        \
        .WillOnce(Return(CODE));                        \
} while (0)

#define EXPECT_CALL_RETURN_FuseOpRename(CLIENT, CODE)   \
do {                                                    \
    EXPECT_CALL(CLIENT, FuseOpRename(_, _, _, _, _, _)) \
        .WillOnce(Return(CODE));                        \
} while (0)

// invoke
#define EXPECT_CALL_INVOKE_FuseOpMkDir(CLIENT, CALLBACK) \
do {                                                     \
    EXPECT_CALL(CLIENT, FuseOpMkDir(_, _, _, _, _))      \
        .WillOnce(Invoke(CALLBACK));                     \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpRmDir(CLIENT, CALLBACK) \
do {                                                     \
    EXPECT_CALL(CLIENT, FuseOpRmDir(_, _, _))               \
        .WillOnce(Invoke(CALLBACK));                     \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpOpenDir(CLIENT, CALLBACK) \
do {                                                       \
    EXPECT_CALL(CLIENT, FuseOpOpenDir(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                       \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpReadDir(CLIENT, CALLBACK) \
do {                                                       \
    EXPECT_CALL(CLIENT, FuseOpReadDir(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                       \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpReleaseDir(CLIENT, CALLBACK) \
do {                                                          \
    EXPECT_CALL(CLIENT, FuseOpReleaseDir(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                          \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpCreate(CLIENT, CALLBACK) \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpCreate(_, _, _, _, _, _))   \
        .WillOnce(Invoke(CALLBACK));                      \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpOpen(CLIENT, CALLBACK) \
do {                                                    \
    EXPECT_CALL(CLIENT, FuseOpOpen(_, _, _, _))         \
        .WillOnce(Invoke(CALLBACK));                    \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpRead(CLIENT, CALLBACK)  \
do {                                                     \
    EXPECT_CALL(CLIENT, FuseOpRead(_, _, _, _, _, _, _)) \
        .WillOnce(Invoke(CALLBACK));                     \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpWrite(CLIENT, CALLBACK)  \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpWrite(_, _, _, _, _, _, _)) \
        .WillOnce(Invoke(CALLBACK));                      \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpFlush(CLIENT, CALLBACK) \
do {                                                     \
    EXPECT_CALL(CLIENT, FuseOpFlush(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                     \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpRelease(CLIENT, CALLBACK) \
do {                                                       \
    EXPECT_CALL(CLIENT, FuseOpRelease(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                       \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpUnlink(CLIENT, CALLBACK) \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpUnlink(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                      \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpStatFs(CLIENT, CALLBACK) \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpStatFs(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                      \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpLookup(CLIENT, CALLBACK) \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpLookup(_, _, _, _))         \
        .WillOnce(Invoke(CALLBACK));                      \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpGetAttr(CLIENT, CALLBACK) \
do {                                                       \
    EXPECT_CALL(CLIENT, FuseOpGetAttr(_, _, _, _))         \
        .WillOnce(Invoke(CALLBACK));                       \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpSetAttr(CLIENT, CALLBACK) \
do {                                                       \
    EXPECT_CALL(CLIENT, FuseOpSetAttr(_, _, _, _, _, _))   \
        .WillOnce(Invoke(CALLBACK));                       \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpReadLink(CLIENT, CALLBACK) \
do {                                                        \
    EXPECT_CALL(CLIENT, FuseOpReadLink(_, _, _))            \
        .WillOnce(Invoke(CALLBACK));                        \
} while (0)

#define EXPECT_CALL_INVOKE_FuseOpRename(CLIENT, CALLBACK) \
do {                                                      \
    EXPECT_CALL(CLIENT, FuseOpRename(_, _, _, _, _, _))   \
        .WillOnce(Invoke(CALLBACK));                      \
} while (0)

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_VFS_HELPER_EXPECT_H_
