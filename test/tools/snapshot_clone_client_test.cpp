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
 * File Created: 2019-11-26
 * Author: charisu
 */

#include <gtest/gtest.h>
#include <string>
#include "src/tools/snapshot_clone_client.h"
#include "test/tools/mock_metric_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::DoAll;

namespace curve {
namespace tool {

class SnapshotCloneClientTest : public ::testing::Test {
 protected:
    void SetUp() {
        metricClient_ = std::make_shared<MockMetricClient>();
    }

    void TearDown() {
        metricClient_ = nullptr;
    }
    std::shared_ptr<MockMetricClient> metricClient_;
};

TEST_F(SnapshotCloneClientTest, Init) {
    SnapshotCloneClient client(metricClient_);
    ASSERT_EQ(-1, client.Init("", "8081"));
    ASSERT_EQ(-1, client.Init("127.0.0.1:5555", ""));
    // dummy server与mds不匹配
    ASSERT_EQ(-1, client.Init("127.0.0.1:5555", "8081,8082,8083"));
    ASSERT_EQ(0, client.Init("127.0.0.1:5555,127.0.0.1:5556,127.0.0.1:5557",
                               "9091,9092,9093"));
    std::map<std::string, std::string> expected =
                                {{"127.0.0.1:5555", "127.0.0.1:9091"},
                                 {"127.0.0.1:5556", "127.0.0.1:9092"},
                                 {"127.0.0.1:5557", "127.0.0.1:9093"}};
    ASSERT_EQ(expected, client.GetDummyServerMap());
}

TEST_F(SnapshotCloneClientTest, GetActiveAddr) {
    // 正常情况
    SnapshotCloneClient client(metricClient_);
    ASSERT_EQ(0, client.Init("127.0.0.1:5555,127.0.0.1:5556,127.0.0.1:5557",
                               "9091"));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<2>("active"),
                        Return(MetricRet::kOK)))
        .WillOnce(DoAll(SetArgPointee<2>("active"),
                        Return(MetricRet::kOK)))
        .WillRepeatedly(DoAll(SetArgPointee<2>("standby"),
                        Return(MetricRet::kOK)));
    std::vector<std::string> activeAddr = client.GetActiveAddrs();
    ASSERT_EQ(1, activeAddr.size());
    ASSERT_EQ("127.0.0.1:5555", activeAddr[0]);

    // 有一个dummyserver显示active，服务端口访问失败
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(4)
        .WillOnce(DoAll(SetArgPointee<2>("active"),
                        Return(MetricRet::kOK)))
        .WillOnce(Return(MetricRet::kOtherErr))
        .WillRepeatedly(DoAll(SetArgPointee<2>("standby"),
                        Return(MetricRet::kOK)));
    activeAddr = client.GetActiveAddrs();
    ASSERT_TRUE(activeAddr.empty());

    // 有一个获取metric失败，其他返回standby
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(3)
        .WillOnce(Return(MetricRet::kNotFound))
        .WillRepeatedly(DoAll(SetArgPointee<2>("standby"),
                        Return(MetricRet::kOK)));
    ASSERT_TRUE(client.GetActiveAddrs().empty());

    // 有两个active状态的
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(5)
        .WillOnce(DoAll(SetArgPointee<2>("standby"),
                        Return(MetricRet::kOK)))
        .WillRepeatedly(DoAll(SetArgPointee<2>("active"),
                        Return(MetricRet::kOK)));
    activeAddr = client.GetActiveAddrs();
    ASSERT_EQ(2, activeAddr.size());
    ASSERT_EQ("127.0.0.1:5556", activeAddr[0]);
    ASSERT_EQ("127.0.0.1:5557", activeAddr[1]);
}

TEST_F(SnapshotCloneClientTest, GetOnlineStatus) {
    SnapshotCloneClient client(metricClient_);
    ASSERT_EQ(0, client.Init("127.0.0.1:5555,127.0.0.1:5556,127.0.0.1:5557",
                               "9091"));
    // 有一个在线，有一个获取metric失败，有一个listen addr不匹配
    EXPECT_CALL(*metricClient_, GetConfValueFromMetric(_, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<2>("127.0.0.1:5555"),
                        Return(MetricRet::kOK)))
        .WillOnce(DoAll(SetArgPointee<2>("127.0.0.1:5557"),
                        Return(MetricRet::kOK)))
        .WillOnce(Return(MetricRet::kNotFound));
    std::map<std::string, bool> onlineStatus;
    client.GetOnlineStatus(&onlineStatus);
    std::map<std::string, bool> expected = {{"127.0.0.1:5555", true},
                                            {"127.0.0.1:5556", false},
                                            {"127.0.0.1:5557", false}};
    ASSERT_EQ(expected, onlineStatus);
}

}  // namespace tool
}  // namespace curve
