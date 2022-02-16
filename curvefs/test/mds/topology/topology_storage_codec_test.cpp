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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <string>
#include <set>

#include "curvefs/src/mds/topology/topology_storage_codec.h"
#include "curvefs/test/mds/topology/test_topology_helper.h"

using ::testing::Return;
using ::testing::_;
using ::testing::AnyOf;
using ::testing::AllOf;
using ::testing::SetArgPointee;
using ::testing::Invoke;
using ::testing::DoAll;


namespace curvefs {
namespace mds {
namespace topology {

class TopologyStorageCodecTest : public ::testing::Test {
 protected:
    void SetUp() override {
    }

    void TearDown() override {
    }

 protected:
    TopologyStorageCodec testObj;
};

TEST_F(TopologyStorageCodecTest, TestPoolDataEncodeDecodeEqual) {
    Pool::RedundanceAndPlaceMentPolicy rap;
    rap.replicaNum = 3;
    rap.copysetNum = 3;
    rap.zoneNum = 3;

    Pool data(0x11, "pool", rap, 0);
    std::string value;
    ASSERT_TRUE(testObj.EncodePoolData(data, &value));

    Pool out;
    ASSERT_TRUE(testObj.DecodePoolData(value, &out));

    ASSERT_TRUE(ComparePool(data, out));
}

TEST_F(TopologyStorageCodecTest, TestZoneDataEncodeDecodeEqual) {
    Zone data(0x31, "zone", 0x21);
    std::string value;
    ASSERT_TRUE(testObj.EncodeZoneData(data, &value));

    Zone out;
    ASSERT_TRUE(testObj.DecodeZoneData(value, &out));

    ASSERT_TRUE(CompareZone(data, out));
}

TEST_F(TopologyStorageCodecTest, TestServerDataEncodeDecodeEqual) {
    Server data(0x41, "server", "127.0.0.1", 8080, "127.0.0.1", 8080, 0x31,
                0x21);
    std::string value;
    ASSERT_TRUE(testObj.EncodeServerData(data, &value));

    Server out;
    ASSERT_TRUE(testObj.DecodeServerData(value, &out));

    ASSERT_TRUE(CompareServer(data, out));
}

TEST_F(TopologyStorageCodecTest, TestMetaServerDataEncodeDecodeEqual) {
    MetaServer data(0x51, "metaserver", "token", 0x41, "127.0.0.1", 8080,
        "127.0.0.1", 8080, OnlineState::OFFLINE);
    std::string value;
    ASSERT_TRUE(testObj.EncodeMetaServerData(data, &value));

    MetaServer out;
    ASSERT_TRUE(testObj.DecodeMetaServerData(value, &out));

    ASSERT_TRUE(CompareMetaServer(data, out));
}

TEST_F(TopologyStorageCodecTest, TestCopySetInfoEncodeDecodeEqual) {
    CopySetInfo data(0x11, 0x61);
    data.SetEpoch(100);
    data.SetCopySetMembers({0x51, 0x52, 0x53});

    std::string value;
    ASSERT_TRUE(testObj.EncodeCopySetData(data, &value));

    CopySetInfo out;
    ASSERT_TRUE(testObj.DecodeCopySetData(value, &out));

    ASSERT_TRUE(CompareCopysetInfo(data, out));
}

TEST_F(TopologyStorageCodecTest, TestPartitionDataEncodeDecodeEqual) {
    Partition data(0x01, 0x11, 0x61, 0x71, 0, 100);

    std::string value;
    ASSERT_TRUE(testObj.EncodePartitionData(data, &value));

    Partition out;
    ASSERT_TRUE(testObj.DecodePartitionData(value, &out));

    ASSERT_TRUE(ComparePartition(data, out));
}

TEST_F(TopologyStorageCodecTest, TestClusterInfoEncodeDecodeEqual) {
    ClusterInformation data;
    data.clusterId = "xxx";

    std::string value;
    ASSERT_TRUE(testObj.EncodeClusterInfoData(data, &value));

    ClusterInformation out;
    ASSERT_TRUE(testObj.DecodeCluserInfoData(value, &out));

    ASSERT_EQ(data.clusterId, out.clusterId);
}

TEST_F(TopologyStorageCodecTest, TestEncodeKeyNumEqual) {
    std::string encodeKey;
    int keyNum = 10000;
    std::set<std::string> keySet;
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodePoolKey(i);
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
        encodeKey = testObj.EncodeMetaServerKey(i);
        keySet.insert(encodeKey);
    }
    for (int i = 0; i < keyNum; i++) {
        encodeKey = testObj.EncodePartitionKey(i);
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
}  // namespace curvefs
