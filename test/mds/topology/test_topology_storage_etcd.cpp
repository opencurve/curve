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
 * Created Date: Wed Jun 17 2020
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "src/mds/topology/topology_storge_etcd.h"
#include "test/mds/topology/mock_topology.h"
#include "test/mds/topology/test_topology_helper.h"

using ::curve::kvstorage::MockKVStorageClient;

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::Matcher;

namespace curve {
namespace mds {
namespace topology {

class TestTopologyStorageEtcd : public ::testing::Test {
 public:
    void SetUp() {
        kvStorageClient_ = std::make_shared<MockKVStorageClient>();
        codec_ = std::make_shared<TopologyStorageCodec>();
        storage_ = std::make_shared<TopologyStorageEtcd>(
            kvStorageClient_, codec_);
    }

    void TearDown() {
        kvStorageClient_ = nullptr;
    }

 protected:
    std::shared_ptr<TopologyStorageEtcd> storage_;
    std::shared_ptr<MockKVStorageClient> kvStorageClient_;
    std::shared_ptr<TopologyStorageCodec> codec_;
};


TEST_F(TestTopologyStorageEtcd, test_LoadLogicalPool_success) {
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.replicaNum = 3;
    rap.pageFileRAP.copysetNum = 3;
    rap.pageFileRAP.zoneNum = 3;
    LogicalPool::UserPolicy policy;
    LogicalPool data(0x11, "lpool", 0x21, LogicalPoolType::PAGEFILE,
        rap, policy, 100, true);
    data.SetScatterWidth(100);
    data.SetStatus(AllocateStatus::ALLOW);

    std::string key = codec_->EncodeLogicalPoolKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeLogicalPoolData(data, &value));
    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId;
    bool ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, logicalPoolMap.size());
    ASSERT_TRUE(JudgeLogicalPoolEqual(data, logicalPoolMap[0x11]));
    ASSERT_EQ(0x11, maxLogicalPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadLogicalPool_success_ListEtcdEmpty) {
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));

    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId;
    bool ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, logicalPoolMap.size());
    ASSERT_EQ(0, maxLogicalPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadLogicalPool_ListEtcdFail) {
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId;
    bool ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);
    ASSERT_FALSE(ret);

    ASSERT_EQ(0, logicalPoolMap.size());
    ASSERT_EQ(0, maxLogicalPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadLogicalPool_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId;
    bool ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadLogicalPool_IdDuplicated) {
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.replicaNum = 3;
    rap.pageFileRAP.copysetNum = 3;
    rap.pageFileRAP.zoneNum = 3;
    LogicalPool::UserPolicy policy;
    LogicalPool data(0x11, "lpool", 0x21, LogicalPoolType::PAGEFILE,
        rap, policy, 100, true);
    data.SetScatterWidth(100);
    data.SetStatus(AllocateStatus::ALLOW);

    std::string key = codec_->EncodeLogicalPoolKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeLogicalPoolData(data, &value));
    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, LogicalPool> logicalPoolMap;
    PoolIdType maxLogicalPoolId;
    bool ret = storage_->LoadLogicalPool(&logicalPoolMap, &maxLogicalPoolId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPhysicalPool_success) {
    PhysicalPool data(0x21, "pPool", "desc");

    std::string key = codec_->EncodePhysicalPoolKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodePhysicalPoolData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId;

    bool ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, physicalPoolMap.size());
    ASSERT_TRUE(JudgePhysicalPoolEqual(data, physicalPoolMap[0x21]));
    ASSERT_EQ(0x21, maxPhysicalPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPhysicalPool_success_listEtcdEmpty) {
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));

    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId;

    bool ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, physicalPoolMap.size());
    ASSERT_EQ(0, maxPhysicalPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPhysicalPool_success_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId;

    bool ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPhysicalPool_IdDuplicated) {
    PhysicalPool data(0x21, "pPool", "desc");

    std::string key = codec_->EncodePhysicalPoolKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodePhysicalPoolData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, PhysicalPool> physicalPoolMap;
    PoolIdType maxPhysicalPoolId;

    bool ret = storage_->LoadPhysicalPool(&physicalPoolMap, &maxPhysicalPoolId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadZone_success) {
    Zone data(0x31, "zone", 0x21, "desc");

    std::string key = codec_->EncodeZoneKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeZoneData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId;

    bool ret = storage_->LoadZone(&zoneMap, &maxZoneId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, zoneMap.size());
    ASSERT_TRUE(JudgeZoneEqual(data, zoneMap[0x31]));
    ASSERT_EQ(0x31, maxZoneId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadZone_success_listEtcdEmpty) {
    std::vector<std::string> list;
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId;

    bool ret = storage_->LoadZone(&zoneMap, &maxZoneId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, zoneMap.size());
    ASSERT_EQ(0, maxZoneId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadZone_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId;

    bool ret = storage_->LoadZone(&zoneMap, &maxZoneId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadZone_IdDuplicated) {
    Zone data(0x31, "zone", 0x21, "desc");

    std::string key = codec_->EncodeZoneKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeZoneData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ZoneIdType, Zone> zoneMap;
    ZoneIdType maxZoneId;

    bool ret = storage_->LoadZone(&zoneMap, &maxZoneId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadServer_success) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080,
        0x31, 0x21, "desc");

    std::string key = codec_->EncodeServerKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeServerData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;

    bool ret = storage_->LoadServer(&serverMap, &maxServerId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, serverMap.size());
    ASSERT_TRUE(JudgeServerEqual(data, serverMap[0x41]));
    ASSERT_EQ(0x41, maxServerId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadServer_success_listEtcdEmpty) {
    std::vector<std::string> list;
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;

    bool ret = storage_->LoadServer(&serverMap, &maxServerId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, serverMap.size());
    ASSERT_EQ(0, maxServerId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadServer_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;

    bool ret = storage_->LoadServer(&serverMap, &maxServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadServer_IdDuplicated) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080,
        0x31, 0x21, "desc");

    std::string key = codec_->EncodeServerKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeServerData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ServerIdType, Server> serverMap;
    ServerIdType maxServerId;

    bool ret = storage_->LoadServer(&serverMap, &maxServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadChunkServer_success) {
    ChunkServer data(0x51, "token", "ssd", 0x41, "127.0.0.1", 8080,
        "/root", ChunkServerStatus::READWRITE, OnlineState::OFFLINE);

    ChunkServerState state;
    state.SetDiskState(DiskState::DISKNORMAL);
    state.SetDiskCapacity(1024);
    state.SetDiskUsed(512);

    data.SetChunkServerState(state);

    std::string key = codec_->EncodeChunkServerKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeChunkServerData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;

    bool ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, chunkServerMap.size());
    data.SetOnlineState(OnlineState::UNSTABLE);
    ASSERT_TRUE(JudgeChunkServerEqual(data, chunkServerMap[0x51]));
    ASSERT_EQ(0x51, maxChunkServerId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadChunkServer_success_listEtcdEmpty) {
    std::vector<std::string> list;
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;

    bool ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, chunkServerMap.size());
    ASSERT_EQ(0, maxChunkServerId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadChunkServer_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;

    bool ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadChunkServer_IdDuplcated) {
    ChunkServer data(0x51, "token", "ssd", 0x41, "127.0.0.1", 8080,
        "/root", ChunkServerStatus::READWRITE, OnlineState::OFFLINE);

    ChunkServerState state;
    state.SetDiskState(DiskState::DISKNORMAL);
    state.SetDiskCapacity(1024);
    state.SetDiskUsed(512);

    data.SetChunkServerState(state);

    std::string key = codec_->EncodeChunkServerKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeChunkServerData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<ChunkServerIdType, ChunkServer> chunkServerMap;
    ChunkServerIdType maxChunkServerId;

    bool ret = storage_->LoadChunkServer(&chunkServerMap, &maxChunkServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadCopyset_success) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    CopySetKey id(data.GetLogicalPoolId(), data.GetId());
    std::string key = codec_->EncodeCopySetKey(id);
    std::string value;
    ASSERT_TRUE(codec_->EncodeCopySetData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;

    bool ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, copySetMap.size());
    ASSERT_TRUE(JudgeCopysetInfoEqual(data,
        copySetMap[std::make_pair(0x11, 0x61)]));
    ASSERT_EQ(1, copySetIdMaxMap.size());
    ASSERT_EQ(0x61, copySetIdMaxMap[0x11]);
}

TEST_F(TestTopologyStorageEtcd, test_LoadCopyset_success_listEtcdEmpty) {
    std::vector<std::string> list;
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;

    bool ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, copySetMap.size());
    ASSERT_EQ(0, copySetIdMaxMap.size());
}

TEST_F(TestTopologyStorageEtcd, test_LoadCopyset_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;

    bool ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadCopyset_IdDuplicated) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    CopySetKey id(data.GetLogicalPoolId(), data.GetId());
    std::string key = codec_->EncodeCopySetKey(id);
    std::string value;
    ASSERT_TRUE(codec_->EncodeCopySetData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::map<CopySetKey, CopySetInfo> copySetMap;
    std::map<PoolIdType, CopySetIdType> copySetIdMaxMap;

    bool ret = storage_->LoadCopySet(&copySetMap, &copySetIdMaxMap);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageLogicalPool_success) {
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.replicaNum = 3;
    rap.pageFileRAP.copysetNum = 3;
    rap.pageFileRAP.zoneNum = 3;
    LogicalPool::UserPolicy policy;
    LogicalPool data(0x11, "lpool", 0x21, LogicalPoolType::PAGEFILE,
        rap, policy, 100, true);
    data.SetScatterWidth(100);
    data.SetStatus(AllocateStatus::ALLOW);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageLogicalPool(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageLogicalPool_putInfoEtcdFail) {
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.replicaNum = 3;
    rap.pageFileRAP.copysetNum = 3;
    rap.pageFileRAP.zoneNum = 3;
    LogicalPool::UserPolicy policy;
    LogicalPool data(0x11, "lpool", 0x21, LogicalPoolType::PAGEFILE,
        rap, policy, 100, true);
    data.SetScatterWidth(100);
    data.SetStatus(AllocateStatus::ALLOW);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageLogicalPool(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotagePhysicalPool_success) {
    PhysicalPool data(0x21, "pPool", "desc");

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StoragePhysicalPool(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotagePhysicalPool_putInfoEtcdFail) {
    PhysicalPool data(0x21, "pPool", "desc");

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StoragePhysicalPool(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageZone_success) {
    Zone data(0x31, "zone", 0x21, "desc");

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageZone(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageZone_putInfoEtcdFail) {
    Zone data(0x31, "zone", 0x21, "desc");

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageZone(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageServer_success) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080,
        0x31, 0x21, "desc");

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageServer_putInfoEtcdFail) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080,
        0x31, 0x21, "desc");

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageChunkServer_success) {
    ChunkServer data(0x51, "token", "ssd", 0x41, "127.0.0.1", 8080,
        "/root", ChunkServerStatus::READWRITE, OnlineState::OFFLINE);

    ChunkServerState state;
    state.SetDiskState(DiskState::DISKNORMAL);
    state.SetDiskCapacity(1024);
    state.SetDiskUsed(512);

    data.SetChunkServerState(state);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageChunkServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageChunkServer_putInfoEtcdFail) {
    ChunkServer data(0x51, "token", "ssd", 0x41, "127.0.0.1", 8080,
        "/root", ChunkServerStatus::READWRITE, OnlineState::OFFLINE);

    ChunkServerState state;
    state.SetDiskState(DiskState::DISKNORMAL);
    state.SetDiskCapacity(1024);
    state.SetDiskUsed(512);

    data.SetChunkServerState(state);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageChunkServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageCopyset_success) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageCopySet(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageCopyset_putInfoEtcdFail) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageCopySet(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteLogicalPool_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeleteLogicalPool(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteLogicalPool_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeleteLogicalPool(0x11);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeletePhysicalPool_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeletePhysicalPool(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeletePhysicalPool_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeletePhysicalPool(0x11);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteZone_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeleteZone(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteZone_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeleteZone(0x11);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteServer_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeleteServer(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteServer_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeleteServer(0x11);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteChunkServer_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeleteChunkServer(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteChunkServer_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeleteChunkServer(0x11);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteCopySet_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeleteCopySet(CopySetKey(0x11, 0x61));
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteCopySet_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeleteCopySet(CopySetKey(0x11, 0x61));
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadClusterInfo_success) {
    ClusterInformation data;
    data.clusterId = "xxx";

    std::string value;
    ASSERT_TRUE(codec_->EncodeClusterInfoData(data, &value));

    EXPECT_CALL(*kvStorageClient_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(value),
            Return(EtcdErrCode::EtcdOK)));

    std::vector<ClusterInformation> infoVec;
    bool ret = storage_->LoadClusterInfo(&infoVec);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, infoVec.size());
    ASSERT_EQ(data.clusterId, infoVec[0].clusterId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadClusterInfo_success_empty) {
    std::string value;

    EXPECT_CALL(*kvStorageClient_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(value),
            Return(EtcdErrCode::EtcdKeyNotExist)));

    std::vector<ClusterInformation> infoVec;
    bool ret = storage_->LoadClusterInfo(&infoVec);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, infoVec.size());
}

TEST_F(TestTopologyStorageEtcd, test_LoadClusterInfo_decodeError) {
    std::string value;

    EXPECT_CALL(*kvStorageClient_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(value),
            Return(EtcdErrCode::EtcdOK)));

    std::vector<ClusterInformation> infoVec;
    bool ret = storage_->LoadClusterInfo(&infoVec);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageClusterInfo_success) {
    ClusterInformation data;
    data.clusterId = "xxx";

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageClusterInfo(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StotageClusterInfo_fail) {
    ClusterInformation data;
    data.clusterId = "xxx";

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageClusterInfo(data);
    ASSERT_FALSE(ret);
}

}  // namespace topology
}  // namespace mds
}  // namespace curve

