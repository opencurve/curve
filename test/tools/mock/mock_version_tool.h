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
 * File Created: 2020-02-20
 * Author: charisu
 */


#ifndef TEST_TOOLS_MOCK_MOCK_VERSION_TOOL_H_
#define TEST_TOOLS_MOCK_MOCK_VERSION_TOOL_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include "src/tools/version_tool.h"
#include "test/tools/mock/mock_snapshot_clone_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockVersionTool : public VersionTool {
 public:
    MockVersionTool() : VersionTool(std::make_shared<MDSClient>(),
                                 std::make_shared<MetricClient>(),
                                 std::make_shared<MockSnapshotCloneClient>()) {}
    MOCK_METHOD2(GetAndCheckMdsVersion, int(std::string*,
                                            std::vector<std::string>*));
    MOCK_METHOD2(GetAndCheckChunkServerVersion, int(std::string*,
                                                    std::vector<std::string>*));
    MOCK_METHOD1(GetClientVersion, int(ClientVersionMapType*));
    MOCK_METHOD2(GetAndCheckSnapshotCloneVersion, int(std::string*,
                                                    std::vector<std::string>*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MOCK_VERSION_TOOL_H_
