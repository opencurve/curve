/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: 2021-09-05
 * Author: wanghai01
 */

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <string>
#include <utility>

#include "curvefs/src/mds/topology/topology_storge_etcd.h"
#include "curvefs/test/mds/mock/mock_topology.h"
#include "curvefs/test/mds/topology/test_topology_helper.h"

using ::curve::kvstorage::MockStorageClient;

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;
using ::testing::Matcher;

namespace curvefs {
namespace mds {
namespace topology {

class TestTopologyStorageEtcd : public ::testing::Test {
 public:
    void SetUp() {
        kvStorageClient_ = std::make_shared<MockStorageClient>();
        codec_ = std::make_shared<TopologyStorageCodec>();
        storage_ =
            std::make_shared<TopologyStorageEtcd>(kvStorageClient_, codec_);
    }

    void TearDown() { kvStorageClient_ = nullptr; }

 protected:
    std::shared_ptr<TopologyStorageEtcd> storage_;
    std::shared_ptr<MockStorageClient> kvStorageClient_;
    std::shared_ptr<TopologyStorageCodec> codec_;
};

TEST_F(TestTopologyStorageEtcd, test_LoadPool_success) {
    Pool::RedundanceAndPlaceMentPolicy rap;
    rap.replicaNum = 3;
    rap.copysetNum = 3;
    rap.zoneNum = 3;
    Pool data(0x11, "pool", rap, 0);

    std::string key = codec_->EncodePoolKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodePoolData(data, &value));
    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, Pool> poolMap;
    PoolIdType maxPoolId;
    bool ret = storage_->LoadPool(&poolMap, &maxPoolId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, poolMap.size());
    ASSERT_TRUE(ComparePool(data, poolMap[0x11]));
    ASSERT_EQ(0x11, maxPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPool_success_ListEtcdEmpty) {
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(Return(EtcdErrCode::EtcdKeyNotExist));

    std::unordered_map<PoolIdType, Pool> poolMap;
    PoolIdType maxPoolId;
    bool ret = storage_->LoadPool(&poolMap, &maxPoolId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, poolMap.size());
    ASSERT_EQ(0, maxPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPool_ListEtcdFail) {
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    std::unordered_map<PoolIdType, Pool> poolMap;
    PoolIdType maxPoolId;
    bool ret = storage_->LoadPool(&poolMap, &maxPoolId);
    ASSERT_FALSE(ret);

    ASSERT_EQ(0, poolMap.size());
    ASSERT_EQ(0, maxPoolId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPool_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, Pool> poolMap;
    PoolIdType maxPoolId;
    bool ret = storage_->LoadPool(&poolMap, &maxPoolId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPool_IdDuplicated) {
    Pool::RedundanceAndPlaceMentPolicy rap;
    rap.replicaNum = 3;
    rap.copysetNum = 3;
    rap.zoneNum = 3;
    Pool data(0x11, "pool", rap, 0);

    std::string key = codec_->EncodePoolKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodePoolData(data, &value));
    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PoolIdType, Pool> poolMap;
    PoolIdType maxPoolId;
    bool ret = storage_->LoadPool(&poolMap, &maxPoolId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadZone_success) {
    Zone data(0x31, "zone", 0x21);

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
    ASSERT_TRUE(CompareZone(data, zoneMap[0x31]));
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
    Zone data(0x31, "zone", 0x21);

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
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080, 0x31,
                0x21);

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
    ASSERT_TRUE(CompareServer(data, serverMap[0x41]));
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
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080, 0x31,
                0x21);

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

TEST_F(TestTopologyStorageEtcd, test_LoadMetaServer_success) {
    MetaServer data(0x51, "metaserver", "token", 0x41, "127.0.0.1", 8080,
                    "127.0.0.1", 8080,
                    OnlineState::OFFLINE);

    std::string key = codec_->EncodeMetaServerKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeMetaServerData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<MetaServerIdType, MetaServer> metaServerMap;
    MetaServerIdType maxMetaServerId;

    bool ret = storage_->LoadMetaServer(&metaServerMap, &maxMetaServerId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, metaServerMap.size());
    data.SetOnlineState(OnlineState::UNSTABLE);
    ASSERT_TRUE(CompareMetaServer(data, metaServerMap[0x51]));
    ASSERT_EQ(0x51, maxMetaServerId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadMetaServer_success_listEtcdEmpty) {
    std::vector<std::string> list;
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    std::unordered_map<MetaServerIdType, MetaServer> metaServerMap;
    MetaServerIdType maxMetaServerId;

    bool ret = storage_->LoadMetaServer(&metaServerMap, &maxMetaServerId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, metaServerMap.size());
    ASSERT_EQ(0, maxMetaServerId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadMetaServer_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<MetaServerIdType, MetaServer> metaServerMap;
    MetaServerIdType maxMetaServerId;

    bool ret = storage_->LoadMetaServer(&metaServerMap, &maxMetaServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadMetaServer_IdDuplcated) {
    MetaServer data(0x51, "metaserver", "token", 0x41, "127.0.0.1", 8080,
                    "127.0.0.1", 8080,
                    OnlineState::OFFLINE);

    std::string key = codec_->EncodeMetaServerKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeMetaServerData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<MetaServerIdType, MetaServer> metaServerMap;
    MetaServerIdType maxMetaServerId;

    bool ret = storage_->LoadMetaServer(&metaServerMap, &maxMetaServerId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadCopyset_success) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    CopySetKey id(data.GetPoolId(), data.GetId());
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
    ASSERT_TRUE(
        CompareCopysetInfo(data, copySetMap[std::make_pair(0x11, 0x61)]));
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

    CopySetKey id(data.GetPoolId(), data.GetId());
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
//

TEST_F(TestTopologyStorageEtcd, test_LoadPartition_success) {
    Partition data(0x01, 0x11, 0x61, 0x71, 0, 100);

    std::string key = codec_->EncodePartitionKey(data.GetPartitionId());
    std::string value;
    ASSERT_TRUE(codec_->EncodePartitionData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PartitionIdType, Partition> partitionMap;
    PartitionIdType maxPartitionId;

    bool ret = storage_->LoadPartition(&partitionMap, &maxPartitionId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, partitionMap.size());
    ASSERT_TRUE(ComparePartition(data, partitionMap[0x71]));
    ASSERT_EQ(0x71, maxPartitionId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPartition_success_listEtcdEmpty) {
    std::vector<std::string> list;
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    std::unordered_map<PartitionIdType, Partition> partitionMap;
    PartitionIdType maxPartitionId;

    bool ret = storage_->LoadPartition(&partitionMap, &maxPartitionId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, partitionMap.size());
    ASSERT_EQ(0, maxPartitionId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPartition_decodeError) {
    std::vector<std::string> list;
    list.push_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PartitionIdType, Partition> partitionMap;
    PartitionIdType maxPartitionId;

    bool ret = storage_->LoadPartition(&partitionMap, &maxPartitionId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadPartition_IdDuplicated) {
    Partition data(0x01, 0x11, 0x61, 0x71, 0, 100);

    std::string key = codec_->EncodePartitionKey(data.GetPartitionId());
    std::string value;
    ASSERT_TRUE(codec_->EncodePartitionData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<PartitionIdType, Partition> partitionMap;
    PartitionIdType maxPartitionId;

    bool ret = storage_->LoadPartition(&partitionMap, &maxPartitionId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StoragePool_success) {
    Pool::RedundanceAndPlaceMentPolicy rap;
    rap.replicaNum = 3;
    rap.copysetNum = 3;
    rap.zoneNum = 3;
    Pool data(0x11, "pool", rap, 0);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StoragePool(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StoragePool_putInfoEtcdFail) {
    Pool::RedundanceAndPlaceMentPolicy rap;
    rap.replicaNum = 3;
    rap.copysetNum = 3;
    rap.zoneNum = 3;
    Pool data(0x11, "pool", rap, 0);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StoragePool(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageZone_success) {
    Zone data(0x31, "zone", 0x21);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageZone(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageZone_putInfoEtcdFail) {
    Zone data(0x31, "zone", 0x21);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageZone(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageServer_success) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080, 0x31,
                0x21);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageServer_putInfoEtcdFail) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080, 0x31,
                0x21);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageMetaServer_success) {
    MetaServer data(0x51, "metaserver", "token", 0x41, "127.0.0.1", 8080,
                    "127.0.0.1", 8080,
                    OnlineState::OFFLINE);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageMetaServer(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageMetaServer_putInfoEtcdFail) {
    MetaServer data(0x51, "metaserver", "token", 0x41, "127.0.0.1", 8080,
                    "127.0.0.1", 8080,
                    OnlineState::OFFLINE);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageMetaServer(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageCopyset_success) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageCopySet(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageCopyset_putInfoEtcdFail) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageCopySet(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StoragePartition_success) {
    Partition data(0x01, 0x11, 0x61, 0x71, 0, 100);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StoragePartition(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StoragePartition_putInfoEtcdFail) {
    Partition data(0x01, 0x11, 0x61, 0x71, 0, 100);

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StoragePartition(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeletePool_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeletePool(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeletePool_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeletePool(0x11);
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

TEST_F(TestTopologyStorageEtcd, test_DeleteMetaServer_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeleteMetaServer(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeleteMetaServer_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeleteMetaServer(0x11);
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

TEST_F(TestTopologyStorageEtcd, test_DeletePartition_success) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->DeletePartition(0x11);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_DeletePartition_fail) {
    EXPECT_CALL(*kvStorageClient_, Delete(_))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->DeletePartition(0x11);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadClusterInfo_success) {
    ClusterInformation data;
    data.clusterId = "xxx";

    std::string value;
    ASSERT_TRUE(codec_->EncodeClusterInfoData(data, &value));

    EXPECT_CALL(*kvStorageClient_, Get(_, _))
        .WillOnce(DoAll(SetArgPointee<1>(value), Return(EtcdErrCode::EtcdOK)));

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
        .WillOnce(DoAll(SetArgPointee<1>(value), Return(EtcdErrCode::EtcdOK)));

    std::vector<ClusterInformation> infoVec;
    bool ret = storage_->LoadClusterInfo(&infoVec);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageClusterInfo_success) {
    ClusterInformation data;
    data.clusterId = "xxx";

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageClusterInfo(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageClusterInfo_fail) {
    ClusterInformation data;
    data.clusterId = "xxx";

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageClusterInfo(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadMemcacheCluster_success) {
    MemcacheCluster data(
        1, std::list<MemcacheServer>{MemcacheServer("127.0.0.1", 1),
                                     MemcacheServer("127.0.0.1", 2),
                                     MemcacheServer("127.0.0.1", 3)});

    std::string key = codec_->EncodeMemcacheClusterKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeMemcacheClusterData(data, &value));
    std::vector<std::string> list;
    list.push_back(value);

    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    MemcacheClusterIdType maxId(0);
    std::unordered_map<MemcacheClusterIdType, MemcacheCluster>
            clusterMap;
    bool ret = storage_->LoadMemcacheCluster(&clusterMap, &maxId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(1, clusterMap.size());
    ASSERT_EQ(data, clusterMap[data.GetId()]);
    ASSERT_EQ(1, maxId);
}

TEST_F(TestTopologyStorageEtcd,
       test_LoadMemcacheCluster_success_listEtcdEmpty) {
    std::vector<std::string> list;
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    MemcacheClusterIdType maxId(0);
    std::unordered_map<MemcacheClusterIdType, MemcacheCluster> clusterMap;
    bool ret = storage_->LoadMemcacheCluster(&clusterMap, &maxId);
    ASSERT_TRUE(ret);

    ASSERT_EQ(0, clusterMap.size());
    ASSERT_EQ(0, maxId);
}

TEST_F(TestTopologyStorageEtcd, test_LoadMemcacheCluster_decodeError) {
    std::vector<std::string> list;
    list.emplace_back("xxx");
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    MemcacheClusterIdType maxId(0);
    std::unordered_map<MemcacheClusterIdType, MemcacheCluster> clusterMap;
    bool ret = storage_->LoadMemcacheCluster(&clusterMap, &maxId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadMemcacheCluster_IdDuplicated) {
    MemcacheCluster data(
        1, std::list<MemcacheServer>{MemcacheServer("127.0.0.1", 1),
                                     MemcacheServer("127.0.0.1", 2),
                                     MemcacheServer("127.0.0.1", 3)});

    std::string key = codec_->EncodeMemcacheClusterKey(data.GetId());
    std::string value;
    ASSERT_TRUE(codec_->EncodeMemcacheClusterData(data, &value));

    std::vector<std::string> list;
    list.push_back(value);
    list.push_back(value);
    EXPECT_CALL(*kvStorageClient_,
                List(_, _, Matcher<std::vector<std::string>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    MemcacheClusterIdType maxId(0);
    std::unordered_map<MemcacheClusterIdType, MemcacheCluster> clusterMap;
    bool ret = storage_->LoadMemcacheCluster(&clusterMap, &maxId);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageMemcacheCluster_success) {
    MemcacheCluster data(
        1, std::list<MemcacheServer>{MemcacheServer("127.0.0.1", 1),
                                     MemcacheServer("127.0.0.1", 2),
                                     MemcacheServer("127.0.0.1", 3)});

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdOK));

    bool ret = storage_->StorageMemcacheCluster(data);
    ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageMemcacheCluster_putInfoEtcdFail) {
    MemcacheCluster data(
        1, std::list<MemcacheServer>{MemcacheServer("127.0.0.1", 1),
                                     MemcacheServer("127.0.0.1", 2),
                                     MemcacheServer("127.0.0.1", 3)});

    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageMemcacheCluster(data);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadFs2MemcacheCluster_success) {
    std::unordered_map<FsIdType, MemcacheClusterIdType> data{
        {1, 1}, {2, 2}, {3, 3}, {4, 4}};
    std::vector<std::pair<std::string, std::string>> list;
    for (auto const& kv : data) {
        auto key = codec_->EncodeFs2MemcacheClusterKey(kv.first);
        auto value = std::to_string(kv.second);
        list.emplace_back(key, value);
    }

    EXPECT_CALL(
        *kvStorageClient_,
        List(_, _,
             Matcher<std::vector<std::pair<std::string, std::string>>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<FsIdType, MemcacheClusterIdType> loadData;
    bool ret = storage_->LoadFs2MemcacheCluster(&loadData);
    ASSERT_TRUE(ret);

    ASSERT_EQ(data.size(), loadData.size());
    ASSERT_EQ(data, loadData);
}

TEST_F(TestTopologyStorageEtcd,
       test_LoadFs2MemcacheCluster_success_listEtcdEmpty) {
    std::vector<std::pair<std::string, std::string>> list;
    EXPECT_CALL(
        *kvStorageClient_,
        List(_, _,
             Matcher<std::vector<std::pair<std::string, std::string>>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list),
                        Return(EtcdErrCode::EtcdKeyNotExist)));

    std::unordered_map<FsIdType, MemcacheClusterIdType> loadData;
    bool ret = storage_->LoadFs2MemcacheCluster(&loadData);

    ASSERT_TRUE(ret);
    ASSERT_TRUE(loadData.empty());
}

TEST_F(TestTopologyStorageEtcd, test_LoadFs2MemcacheCluster_decodeValueError) {
    std::vector<std::pair<std::string, std::string>> list;
    list.emplace_back("xxx", "xxx");

    EXPECT_CALL(
        *kvStorageClient_,
        List(_, _,
             Matcher<std::vector<std::pair<std::string, std::string>>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<FsIdType, MemcacheClusterIdType> loadData;
    bool ret = storage_->LoadFs2MemcacheCluster(&loadData);

    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_LoadFs2MemcacheCluster_IdDuplicated) {
    std::unordered_map<FsIdType, MemcacheClusterIdType> data{
        {1, 1}};
    std::vector<std::pair<std::string, std::string>> list;
    for (auto const& kv : data) {
        auto key = codec_->EncodeFs2MemcacheClusterKey(kv.first);
        auto value = std::to_string(kv.second);
        list.emplace_back(key, value);
    }
    list.emplace_back(codec_->EncodeFs2MemcacheClusterKey(1), "2");
    EXPECT_CALL(
        *kvStorageClient_,
        List(_, _,
             Matcher<std::vector<std::pair<std::string, std::string>>*>(_)))
        .WillOnce(DoAll(SetArgPointee<2>(list), Return(EtcdErrCode::EtcdOK)));

    std::unordered_map<FsIdType, MemcacheClusterIdType> loadData;
    bool ret = storage_->LoadFs2MemcacheCluster(&loadData);
    ASSERT_FALSE(ret);
}

TEST_F(TestTopologyStorageEtcd, test_StorageFs2MemcacheCluster_success) {
    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillRepeatedly(Return(EtcdErrCode::EtcdOK));
        bool ret = storage_->StorageFs2MemcacheCluster(1, 1);
        ASSERT_TRUE(ret);
}

TEST_F(TestTopologyStorageEtcd,
       test_StorageFs2MemcacheCluster_putInfoEtcdFail) {
    EXPECT_CALL(*kvStorageClient_, Put(_, _))
        .WillOnce(Return(EtcdErrCode::EtcdUnknown));

    bool ret = storage_->StorageFs2MemcacheCluster(1, 1);
    ASSERT_FALSE(ret);
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs
