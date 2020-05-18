/*
 * Project: curve
 * File Created: 2019-11-27
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */


#ifndef TEST_TOOLS_MOCK_MDS_CLIENT_H_
#define TEST_TOOLS_MOCK_MDS_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <map>
#include "src/tools/mds_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockMDSClient : public MDSClient {
 public:
    MockMDSClient() {}
    ~MockMDSClient() {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD2(Init, int(const std::string&, const std::string&));
    MOCK_METHOD2(GetFileInfo, int(const std::string&, FileInfo*));
    MOCK_METHOD2(GetAllocatedSize, int(const std::string&, uint64_t*));
    MOCK_METHOD2(ListDir, int(const std::string&, std::vector<FileInfo>*));
    MOCK_METHOD3(GetSegmentInfo, GetSegmentRes(const std::string&,
                                        uint64_t, PageFileSegment*));
    MOCK_METHOD2(DeleteFile, int(const std::string&, bool));
    MOCK_METHOD2(CreateFile, int(const std::string&, uint64_t));
    MOCK_METHOD3(GetChunkServerListInCopySet, int(const PoolIdType&,
                    const CopySetIdType&, std::vector<ChunkServerLocation>*));
    MOCK_METHOD3(GetChunkServerListInCopySets, int(const PoolIdType&,
                                        const std::vector<CopySetIdType>&,
                                        std::vector<CopySetServerInfo>*));
    MOCK_METHOD1(ListPhysicalPoolsInCluster,
                        int(std::vector<PhysicalPoolInfo>*));
    MOCK_METHOD2(ListLogicalPoolsInPhysicalPool, int(const PoolIdType&,
                        std::vector<LogicalPoolInfo>*));
    MOCK_METHOD2(ListZoneInPhysicalPool, int(const PoolIdType&,
                                          std::vector<ZoneInfo>*));
    MOCK_METHOD2(ListServersInZone, int(const ZoneIdType&,
                                    std::vector<ServerInfo>*));
    MOCK_METHOD2(ListChunkServersOnServer, int(const ServerIdType&,
                                           std::vector<ChunkServerInfo>*));
    MOCK_METHOD2(ListChunkServersOnServer, int(const std::string&,
                                           std::vector<ChunkServerInfo>*));
    MOCK_METHOD2(GetChunkServerInfo, int(const ChunkServerIdType&,
                                            ChunkServerInfo*));
    MOCK_METHOD2(GetChunkServerInfo, int(const std::string&,
                                            ChunkServerInfo*));
    MOCK_METHOD2(GetCopySetsInChunkServer, int(const ChunkServerIdType&,
                                        std::vector<CopysetInfo>*));
    MOCK_METHOD2(GetCopySetsInChunkServer, int(const std::string&,
                                        std::vector<CopysetInfo>*));
    MOCK_METHOD1(ListServersInCluster,  int(std::vector<ServerInfo>*));
    MOCK_METHOD1(ListChunkServersInCluster,
                 int(std::vector<ChunkServerInfo>*));
    MOCK_METHOD2(GetMetric, int(const std::string&, uint64_t*));
    MOCK_CONST_METHOD0(GetMdsAddrVec, const std::vector<std::string>&());
    MOCK_METHOD0(GetCurrentMds, std::vector<std::string>());
    MOCK_METHOD1(GetMdsOnlineStatus,
                    void(std::map<std::string, bool>* onlineStatus));
    MOCK_CONST_METHOD0(GetDummyServerMap,
                    const std::map<std::string, std::string>&());
    MOCK_METHOD2(ListClient, int(std::vector<std::string>*, bool));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_MDS_CLIENT_H_
