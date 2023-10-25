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
 * Created Date: 2023-09-18
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include <vector>
#include <string>

#include "curvefs/src/client/vfs/handlers.h"
#include "curvefs/test/client/vfs/helper/helper.h"

namespace curvefs {
namespace client {
namespace vfs {

class HandlersTest : public ::testing::Test {};

TEST_F(HandlersTest, Basic) {
    // NextHandler
    auto handlers = std::make_shared<FileHandlers>();
    uint64_t fd = handlers->NextHandler(100, 4096);
    ASSERT_EQ(fd, 0);

    // GetHandler
    std::shared_ptr<FileHandler> handler;
    bool yes = handlers->GetHandler(fd, &handler);
    ASSERT_TRUE(yes);
    ASSERT_EQ(handler->ino, 100);
    ASSERT_EQ(handler->offset, 4096);

    // FreeHandler
    handlers->FreeHandler(fd);
    yes = handlers->GetHandler(fd, &handler);
    ASSERT_FALSE(yes);
}

// XXX: The case is to prove that we will not reuse fd.
TEST_F(HandlersTest, NextHandler) {
    // NextHandler
    auto handlers = std::make_shared<FileHandlers>();
    for (uint64_t i = 0; i < 10000; i++) {
        uint64_t fd = handlers->NextHandler(100, 4096);
        ASSERT_EQ(fd, i);
    }

    // FreeHandler
    for (uint64_t fd = 0; fd < 10000; fd++) {
        handlers->FreeHandler(fd);
    }

    // NextHandler
    for (uint64_t i = 10000; i < 20000; i++) {
        uint64_t fd = handlers->NextHandler(100, 4096);
        ASSERT_EQ(fd, i);
    }
}

TEST_F(HandlersTest, GetHandler) {
    // NextHandler
    auto handlers = std::make_shared<FileHandlers>();
    uint64_t fd = handlers->NextHandler(100, 4096);
    ASSERT_EQ(fd, 0);
    fd = handlers->NextHandler(200, 8192);
    ASSERT_EQ(fd, 1);

    // GetHandler
    std::shared_ptr<FileHandler> handler;
    bool yes = handlers->GetHandler(0, &handler);
    ASSERT_TRUE(yes);
    ASSERT_EQ(handler->ino, 100);
    ASSERT_EQ(handler->offset, 4096);

    yes = handlers->GetHandler(1, &handler);
    ASSERT_TRUE(yes);
    ASSERT_EQ(handler->ino, 200);
    ASSERT_EQ(handler->offset, 8192);
}

}  // namespace vfs
}  // namespace client
}  // namespace curvefs
