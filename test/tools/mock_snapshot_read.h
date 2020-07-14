/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: curve
 * File Created: 2019-10-29
 * Author: charisu
 */

#ifndef TEST_TOOLS_MOCK_SNAPSHOT_READ_H_
#define TEST_TOOLS_MOCK_SNAPSHOT_READ_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/tools/snapshot_read.h"

namespace curve {
namespace tool {
class MockSnapshotRead : public SnapshotRead {
 public:
    MockSnapshotRead() {}
    ~MockSnapshotRead() {}

    MOCK_METHOD2(Init, int(const std::string&, const std::string&));
    MOCK_METHOD0(UnInit, void());
    MOCK_METHOD3(Read, int(char*, off_t, size_t));
    MOCK_METHOD1(GetSnapshotInfo, void(SnapshotRepoItem*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_SNAPSHOT_READ_H_
