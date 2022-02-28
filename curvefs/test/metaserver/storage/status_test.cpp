/*
 * Copyright (c) 2021 NetEase Inc.
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

/**
 * Project: Curve
 * Created Date: 2022-03-02
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "curvefs/src/metaserver/storage/status.h"

namespace curvefs {
namespace metaserver {
namespace storage {

class StatusTest : public testing::Test {
 protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StatusTest, BasicTest) {
    Status s;

    s = Status::OK();
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(s.ToString(), "OK");

    s = Status::DBClosed();
    ASSERT_TRUE(s.IsDBClosed());
    ASSERT_EQ(s.ToString(), "Database Closed");

    s = Status::NotFound();
    ASSERT_TRUE(s.IsNotFound());
    ASSERT_EQ(s.ToString(), "Not Found");

    s = Status::NotSupported();
    ASSERT_TRUE(s.IsNotSupported());
    ASSERT_EQ(s.ToString(), "Not Supported");

    s = Status::InternalError();
    ASSERT_TRUE(s.IsInternalError());
    ASSERT_EQ(s.ToString(), "Internal Error");
}

}  // namespace storage
}  // namespace metaserver
}  // namespace curvefs
