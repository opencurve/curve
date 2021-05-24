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
 * Created Date: Wed Jul 01 2020
 * Author: xuchaojie
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <set>

#include "src/mds/topology/topology_storage_codec.h"
#include "test/mds/topology/test_topology_helper.h"

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;


namespace curve {
namespace mds {
namespace topology {


TEST(TestTopologyStorageCodec,
    TestLogicalPoolEncodeDecodeEqual) {
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.replicaNum = 3;
    rap.pageFileRAP.copysetNum = 3;
    rap.pageFileRAP.zoneNum = 3;
    LogicalPool::UserPolicy policy;
    LogicalPool data(0x11, "lpool", 0x21, LogicalPoolType::PAGEFILE,
        rap, policy, 100, true, true);
    data.SetScatterWidth(100);
    data.SetStatus(AllocateStatus::ALLOW);

    TopologyStorageCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeLogicalPoolData(data, &value));

    LogicalPool out;
    ASSERT_TRUE(testObj.DecodeLogicalPoolData(value, &out));

    ASSERT_TRUE(JudgeLogicalPoolEqual(data, out));
}

TEST(TestTopologyStorageCodec,
    TestPhysicalPoolEncodeDecodeEqual) {
    PhysicalPool data(0x21, "pPool", "desc");

    TopologyStorageCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodePhysicalPoolData(data, &value));

    PhysicalPool out;
    ASSERT_TRUE(testObj.DecodePhysicalPoolData(value, &out));

    ASSERT_TRUE(JudgePhysicalPoolEqual(data, out));
}

TEST(TestTopologyStorageCodec,
    TestZoneEncodeDecodeEqual) {
    Zone data(0x31, "zone", 0x21, "desc");

    TopologyStorageCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeZoneData(data, &value));

    Zone out;
    ASSERT_TRUE(testObj.DecodeZoneData(value, &out));

    ASSERT_TRUE(JudgeZoneEqual(data, out));
}

TEST(TestTopologyStorageCodec,
    TestServerEncodeDecodeEqual) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080,
        0x31, 0x21, "desc");

    TopologyStorageCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeServerData(data, &value));

    Server out;
    ASSERT_TRUE(testObj.DecodeServerData(value, &out));

    ASSERT_TRUE(JudgeServerEqual(data, out));
}

TEST(TestTopologyStorageCodec,
    TestChunkServerEncodeDecodeEqual) {
    ChunkServer data(0x51, "token", "ssd", 0x41, "127.0.0.1", 8080,
        "/root", ChunkServerStatus::READWRITE, OnlineState::OFFLINE);

    ChunkServerState state;
    state.SetDiskState(DiskState::DISKNORMAL);
    state.SetDiskCapacity(1024);
    state.SetDiskUsed(512);

    data.SetChunkServerState(state);

    TopologyStorageCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeChunkServerData(data, &value));

    ChunkServer out;
    ASSERT_TRUE(testObj.DecodeChunkServerData(value, &out));

    ASSERT_TRUE(JudgeChunkServerEqual(data, out));
}

TEST(TestTopologyStorageCodec,
    TestCopySetInfoEncodeDecodeEqual) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    TopologyStorageCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeCopySetData(data, &value));

    CopySetInfo out;
    ASSERT_TRUE(testObj.DecodeCopySetData(value, &out));

    ASSERT_TRUE(JudgeCopysetInfoEqual(data, out));
}

TEST(TestTopologyStorageCodec,
    TestClusterInfoEncodeDecodeEqual) {
    ClusterInformation data;
    data.clusterId = "xxx";

    TopologyStorageCodec testObj;
    std::string value;
    ASSERT_TRUE(testObj.EncodeClusterInfoData(data, &value));

    ClusterInformation out;
    ASSERT_TRUE(testObj.DecodeCluserInfoData(value, &out));

    ASSERT_EQ(data.clusterId, out.clusterId);
}

TEST(TestTopologyStorageCodec,
    TestEncodeKeyNotEqual) {
    TopologyStorageCodec testObj;
    std::string encodeKey;
    int keyNum = 10000;
    std::set<std::string> keySet;
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodeLogicalPoolKey(i);
        keySet.insert(encodeKey);
    }
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodePhysicalPoolKey(i);
        keySet.insert(encodeKey);
    }
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodeZoneKey(i);
        keySet.insert(encodeKey);
    }
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodeServerKey(i);
        keySet.insert(encodeKey);
    }
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodeChunkServerKey(i);
        keySet.insert(encodeKey);
    }

    int keyRow = 10;
    for (int i = 0; i < keyNum; i++) {
        for (int j = 0; j < keyRow; j++) {
            encodeKey = testObj.EncodeCopySetKey({j, i});
            keySet.insert(encodeKey);
        }
    }

    ASSERT_EQ(5 * keyNum + keyNum * keyRow, keySet.size());
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
