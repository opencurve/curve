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
 * Created Date: 2020-01-14
 * Author: lixiaocui
 */

#ifndef TEST_CHUNKSERVER_MOCK_COPYSET_NODE_MANAGER_H_
#define TEST_CHUNKSERVER_MOCK_COPYSET_NODE_MANAGER_H_

#include <gmock/gmock.h>
#include "src/chunkserver/copyset_node_manager.h"

namespace curve {
namespace chunkserver {
class MockCopysetNodeManager : public CopysetNodeManager {
 public:
    MockCopysetNodeManager() {}
    ~MockCopysetNodeManager() {}

    MOCK_METHOD0(LoadFinished, bool());
};
}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_COPYSET_NODE_MANAGER_H_
