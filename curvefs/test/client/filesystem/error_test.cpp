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
 * Created Date: 2023-03-29
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "curvefs/src/client/filesystem/error.h"

namespace curvefs {
namespace client {
namespace filesystem {

class ErrorTest : public ::testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ErrorTest, StrErr) {
    ASSERT_EQ(StrErr(CURVEFS_ERROR::OK), "OK");
    ASSERT_EQ(StrErr(CURVEFS_ERROR::INTERNAL), "internal error");
    ASSERT_EQ(StrErr(CURVEFS_ERROR::INVALID_PARAM), "invalid argument");
    ASSERT_EQ(StrErr(CURVEFS_ERROR::STALE), "stale file handler");
    ASSERT_EQ(StrErr(CURVEFS_ERROR::UNKNOWN), "unknown");
}

TEST_F(ErrorTest, SysErr) {
    ASSERT_EQ(SysErr(CURVEFS_ERROR::OK), 0);
    ASSERT_EQ(SysErr(CURVEFS_ERROR::INTERNAL), EIO);
    ASSERT_EQ(SysErr(CURVEFS_ERROR::INVALID_PARAM), EINVAL);
    ASSERT_EQ(SysErr(CURVEFS_ERROR::STALE), ESTALE);
    ASSERT_EQ(SysErr(CURVEFS_ERROR::UNKNOWN), EIO);
}

TEST_F(ErrorTest, ToFSError) {
    ASSERT_EQ(ToFSError(MetaStatusCode::OK), CURVEFS_ERROR::OK);
    ASSERT_EQ(ToFSError(MetaStatusCode::NOT_FOUND), CURVEFS_ERROR::NOT_EXIST);
}

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
