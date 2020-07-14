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
 * File Created: 2019-11-30
 * Author: charisu
 */

#ifndef TEST_TOOLS_MOCK_COPYSET_CHECK_CORE_H_
#define TEST_TOOLS_MOCK_COPYSET_CHECK_CORE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>

#include "src/tools/chunkserver_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockCopysetCheckCore : public CopysetCheckCore {
 public:
    MockCopysetCheckCore() : CopysetCheckCore(std::make_shared<MDSClient>(),
                                    std::make_shared<ChunkServerClient>()) {}
    ~MockCopysetCheckCore() {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD2(CheckOneCopyset, int(const PoolIdType&,
                                      const CopySetIdType&));
    MOCK_METHOD1(CheckCopysetsOnChunkServer, int(const ChunkServerIdType&));
    MOCK_METHOD1(CheckCopysetsOnChunkServer, int(const std::string&));
    MOCK_METHOD2(CheckCopysetsOnServer, int(const ServerIdType&,
                                            std::vector<std::string>*));
    MOCK_METHOD2(CheckCopysetsOnServer, int(const std::string&,
                                            std::vector<std::string>*));
    MOCK_METHOD0(CheckCopysetsInCluster, int());
    MOCK_METHOD0(GetCopysetStatistics, CopysetStatistics());
    MOCK_CONST_METHOD0(GetCopysetsRes,
                    const std::map<std::string, std::set<std::string>>&());
    MOCK_CONST_METHOD0(GetCopysetDetail, const std::string&());
    MOCK_CONST_METHOD0(GetServiceExceptionChunkServer,
                       const std::set<std::string>&());
    MOCK_CONST_METHOD0(GetCopysetLoadExceptionChunkServer,
                       const std::set<std::string>&());
    MOCK_METHOD2(CheckOperator, int(const std::string&, uint64_t));
    MOCK_METHOD1(CheckChunkServerOnline, bool(const std::string&));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_COPYSET_CHECK_CORE_H_
