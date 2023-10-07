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


// code
#define OK(rc) ASSERT_EQ(rc, CURVEFS_ERROR::OK);
#define BAD_FD(rc) ASSERT_EQ(rc, CURVEFS_ERROR::BAD_FD);
#define EXISTS(rc) ASSERT_EQ(rc, CURVEFS_ERROR::EXISTS);
#define NOT_EXIST(rc) ASSERT_EQ(rc, CURVEFS_ERROR::NOT_EXIST);
#define NOT_PERMISSION(rc) ASSERT_EQ(rc, CURVEFS_ERROR::NO_PERMISSION);
#define STALE(rc) ASSERT_EQ(rc, CURVEFS_ERROR::STALE);
#define INTERNAL_ERROR(rc) ASSERT_EQ(rc, CURVEFS_ERROR::INTERNAL);


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
#define EXPECT_CALL_INVOKE0(obj, call, fn) \
do {                                       \
    EXPECT_CALL(obj, call())               \
        .WillOnce(Invoke(fn));             \
} while (0)

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

}  // namespace vfs
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_TEST_CLIENT_VFS_HELPER_EXPECT_H_
