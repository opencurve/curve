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
 * Created Date: Mon Nov 26 2018
 * Author: xuchaojie
 */

#include <gtest/gtest.h>

#include "src/mds/topology/topology_item.h"

namespace curve {
namespace mds {
namespace topology {

class TestTopologyItem : public ::testing::Test {
 public:
    TestTopologyItem() {}
    ~TestTopologyItem() {}
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(TestTopologyItem,
    Test_TransRedundanceAndPlaceMentPolicyFromJsonStr_success) {
    std::string jsonstr = "{\"replicaNum\":1,\"copysetNum\":2,\"zoneNum\":3}";
    LogicalPoolType type = PAGEFILE;
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    ASSERT_EQ(true,
        LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(jsonstr,
            type,
            &rap));

    ASSERT_EQ(1, rap.pageFileRAP.replicaNum);
    ASSERT_EQ(2, rap.pageFileRAP.copysetNum);
    ASSERT_EQ(3, rap.pageFileRAP.zoneNum);
}

TEST_F(TestTopologyItem,
    Test_TransRedundanceAndPlaceMentPolicyFromJsonStr_missingFeild) {
    const std::string jsonstr = {};
    LogicalPoolType type = PAGEFILE;
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    ASSERT_EQ(false,
        LogicalPool::TransRedundanceAndPlaceMentPolicyFromJsonStr(jsonstr,
            type,
            &rap));
}

TEST_F(TestTopologyItem,
    Test_GetRedundanceAndPlaceMentPolicyJsonStr_success) {
    LogicalPool::RedundanceAndPlaceMentPolicy rap;
    rap.pageFileRAP.replicaNum = 1;
    rap.pageFileRAP.copysetNum = 2;
    rap.pageFileRAP.zoneNum = 3;
    LogicalPool lpool(0x01,
        "pool1",
        0x11,
        PAGEFILE,
        rap,
        LogicalPool::UserPolicy(),
        0,
        true,
        true);

    std::string retStr = lpool.GetRedundanceAndPlaceMentPolicyJsonStr();
    std::string jsonStr =
    "{\n\t\"copysetNum\" : 2,\n\t\"replicaNum\" : 1,\n\t\"zoneNum\" : 3\n}\n";
    ASSERT_STREQ(jsonStr.c_str(), retStr.c_str());
}


TEST_F(TestTopologyItem, Test_GetCopySetMembersStr_success) {
    CopySetInfo cInfo(0x01, 0x11);
    std::set<ChunkServerIdType> idList;
    idList.insert(0x21);
    idList.insert(0x22);
    idList.insert(0x23);
    cInfo.SetCopySetMembers(idList);

    std::string retStr = cInfo.GetCopySetMembersStr();
    std::string jsonStr = "[\n\t33,\n\t34,\n\t35\n]\n";
    ASSERT_STREQ(jsonStr.c_str(), retStr.c_str());
}


TEST_F(TestTopologyItem, Test_SetCopySetMembersByJson_success) {
    std::string jsonStr = "[11, 12, 13]";
    CopySetInfo cInfo(0x01, 0x11);
    ASSERT_EQ(true, cInfo.SetCopySetMembersByJson(jsonStr));
}


TEST_F(TestTopologyItem, Test_SetCopySetMembersByJson_Fail) {
    std::string jsonStr = "[\"a\", 12, 13]";
    CopySetInfo cInfo(0x01, 0x11);
    ASSERT_EQ(false, cInfo.SetCopySetMembersByJson(jsonStr));
}

TEST(TestPhysicalPool, SerializeAndDeserializeTest) {
    {
        PhysicalPoolData data;
        data.set_physicalpoolid(1);
        data.set_physicalpoolname("pool1");
        data.set_desc("");

        std::string value;
        ASSERT_TRUE(data.SerializeToString(&value));

        PhysicalPool pool;
        ASSERT_TRUE(pool.ParseFromString(value));

        ASSERT_EQ(1, pool.GetId());
        ASSERT_EQ(UNINTIALIZE_ID, pool.GetPoolsetId());

        ASSERT_TRUE(pool.SerializeToString(&value));
        ASSERT_TRUE(data.ParseFromString(value));
        ASSERT_FALSE(data.has_poolsetid());
    }

    {
        PhysicalPoolData data;
        data.set_physicalpoolid(1);
        data.set_physicalpoolname("pool1");
        data.set_desc("");
        data.set_poolsetid(1);

        std::string value;
        ASSERT_TRUE(data.SerializeToString(&value));

        PhysicalPool pool;
        ASSERT_TRUE(pool.ParseFromString(value));

        ASSERT_EQ(1, pool.GetId());
        ASSERT_EQ(1, pool.GetPoolsetId());

        ASSERT_TRUE(pool.SerializeToString(&value));
        ASSERT_TRUE(data.ParseFromString(value));
        ASSERT_TRUE(data.has_poolsetid());
    }
}

}  // namespace topology
}  // namespace mds
}  // namespace curve
