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
 * File Created: 2020-03-17
 * Author: charisu
 */

#ifndef TEST_TOOLS_MOCK_SNAPSHOT_CLONE_CLIENT_H_
#define TEST_TOOLS_MOCK_SNAPSHOT_CLONE_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <map>
#include <memory>
#include <vector>
#include "src/tools/snapshot_clone_client.h"

namespace curve {
namespace tool {
class MockSnapshotCloneClient : public SnapshotCloneClient {
 public:
    MockSnapshotCloneClient() :
            SnapshotCloneClient(std::make_shared<MetricClient>()) {}
    MOCK_METHOD2(Init, int(const std::string&, const std::string&));
    MOCK_METHOD0(GetActiveAddrs, std::vector<std::string>());
    MOCK_METHOD1(GetOnlineStatus, void(std::map<std::string, bool>*));
    MOCK_CONST_METHOD0(GetDummyServerMap,
                    const std::map<std::string, std::string>&());
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_SNAPSHOT_CLONE_CLIENT_H_
