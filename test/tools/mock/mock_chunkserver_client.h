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
 * File Created: 2019-11-28
 * Author: charisu
 */

#ifndef TEST_TOOLS_MOCK_MOCK_CHUNKSERVER_CLIENT_H_
#define TEST_TOOLS_MOCK_MOCK_CHUNKSERVER_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/tools/chunkserver_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockChunkServerClient : public ChunkServerClient {
 public:
    MockChunkServerClient() {}
    ~MockChunkServerClient() {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD1(GetRaftStatus, int(butil::IOBuf*));
    MOCK_METHOD0(CheckChunkServerOnline, bool());
    MOCK_METHOD2(GetCopysetStatus, int(const CopysetStatusRequest& request,
                                 CopysetStatusResponse* response));
    MOCK_METHOD2(GetChunkHash, int(const Chunk&, std::string*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MOCK_CHUNKSERVER_CLIENT_H_
