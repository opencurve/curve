/*
 * Project: curve
 * File Created: 2019-11-30
 * Author: charisu
 * Copyright (c)￼ 2018 netease
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
    MOCK_METHOD2(CheckOperator, int(const std::string&, uint64_t));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_COPYSET_CHECK_CORE_H_
