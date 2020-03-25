/*
 * Project: curve
 * File Created: 2020-02-20
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include "src/tools/version_tool.h"
#include "test/tools/mock_mds_client.h"
#include "test/tools/mock_metric_client.h"

using ::testing::_;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;

namespace curve {
namespace tool {

class VersionToolTest : public ::testing::Test {
 protected:
    void SetUp() {
        mdsClient_ = std::make_shared<MockMDSClient>();
        metricClient_ = std::make_shared<MockMetricClient>();
    }

    void TearDown() {
        mdsClient_ = nullptr;
        metricClient_ = nullptr;
    }

    void GetCsInfoForTest(curve::mds::topology::ChunkServerInfo *csInfo,
                            uint64_t csId) {
        csInfo->set_chunkserverid(csId);
        csInfo->set_disktype("ssd");
        csInfo->set_hostip("127.0.0.1");
        csInfo->set_port(9190 + csId);
        csInfo->set_onlinestate(OnlineState::ONLINE);
        csInfo->set_status(ChunkServerStatus::READWRITE);
        csInfo->set_diskstatus(DiskState::DISKNORMAL);
        csInfo->set_mountpoint("/test");
        csInfo->set_diskcapacity(1024);
        csInfo->set_diskused(512);
    }
    std::shared_ptr<MockMDSClient> mdsClient_;
    std::shared_ptr<MockMetricClient> metricClient_;
};

TEST_F(VersionToolTest, Init) {
    VersionTool versionTool(mdsClient_, metricClient_);
    EXPECT_CALL(*mdsClient_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, versionTool.Init("127.0.0.1:6666"));
}

TEST_F(VersionToolTest, GetAndCheckMdsVersion) {
    VersionTool versionTool(mdsClient_, metricClient_);
    std::map<std::string, std::string> dummyServerMap =
                                {{"127.0.0.1:6666", "127.0.0.1:6667"},
                                 {"127.0.0.1:6668", "127.0.0.1:6669"},
                                 {"127.0.0.1:6670", "127.0.0.1:6671"}};

    // 1、正常情况
    EXPECT_CALL(*mdsClient_, GetDummyServerMap())
        .Times(1)
        .WillOnce(ReturnRef(dummyServerMap));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(3)
        .WillRepeatedly(DoAll(SetArgPointee<2>("0.0.1"),
                        Return(MetricRet::kOK)));
    std::string version;
    std::vector<std::string> failedList;
    ASSERT_EQ(0, versionTool.GetAndCheckMdsVersion(&version, &failedList));
    ASSERT_EQ("0.0.1", version);
    ASSERT_TRUE(failedList.empty());

    // 2、获取部分mds curve_version失败
    EXPECT_CALL(*mdsClient_, GetDummyServerMap())
        .Times(1)
        .WillOnce(ReturnRef(dummyServerMap));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(3)
        .WillOnce(Return(MetricRet::kOtherErr))
        .WillRepeatedly(DoAll(SetArgPointee<2>("0.0.1"),
                        Return(MetricRet::kOK)));
    ASSERT_EQ(0, versionTool.GetAndCheckMdsVersion(&version, &failedList));
    ASSERT_EQ("0.0.1", version);
    std::vector<std::string> expectedList = {"127.0.0.1:6667"};
    ASSERT_EQ(expectedList, failedList);

    // 3、dummyServerMap为空
    std::map<std::string, std::string> dummyServerMap2;
    EXPECT_CALL(*mdsClient_, GetDummyServerMap())
        .Times(1)
        .WillOnce(ReturnRef(dummyServerMap2));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(0);
    ASSERT_EQ(-1, versionTool.GetAndCheckMdsVersion(&version, &failedList));
    ASSERT_TRUE(failedList.empty());

    // 4、version不一致
    EXPECT_CALL(*mdsClient_, GetDummyServerMap())
        .Times(1)
        .WillOnce(ReturnRef(dummyServerMap));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<2>("0.0.2"),
                        Return(MetricRet::kOK)))
        .WillOnce(DoAll(SetArgPointee<2>("0.0.1"),
                        Return(MetricRet::kOK)))
        .WillOnce(Return(MetricRet::kNotFound));
    ASSERT_EQ(-1, versionTool.GetAndCheckMdsVersion(&version, &failedList));
    ASSERT_TRUE(failedList.empty());

    // 5、老版本mds
    EXPECT_CALL(*mdsClient_, GetDummyServerMap())
        .Times(1)
        .WillOnce(ReturnRef(dummyServerMap));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(3)
        .WillRepeatedly(Return(MetricRet::kNotFound));
    ASSERT_EQ(0, versionTool.GetAndCheckMdsVersion(&version, &failedList));
    ASSERT_EQ("before0.0.5.2", version);
    ASSERT_TRUE(failedList.empty());
}

TEST_F(VersionToolTest, GetChunkServerVersion) {
    VersionTool versionTool(mdsClient_, metricClient_);
    std::vector<ChunkServerInfo> chunkservers;
    ChunkServerInfo csInfo;
    for (uint64_t i = 1; i <= 5; ++i) {
        GetCsInfoForTest(&csInfo, i);
        chunkservers.emplace_back(csInfo);
    }

    // 1、正常情况
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(5)
        .WillRepeatedly(DoAll(SetArgPointee<2>("0.0.1"),
                        Return(MetricRet::kOK)));
    std::string version;
    std::vector<std::string> failedList;
    ASSERT_EQ(0, versionTool.GetAndCheckChunkServerVersion(&version,
                                                           &failedList));
    ASSERT_EQ("0.0.1", version);
    ASSERT_TRUE(failedList.empty());

    // 2、ListChunkServersInCluster失败
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, versionTool.GetAndCheckChunkServerVersion(&version,
                                                            &failedList));

    // 3、获取metric失败
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(5)
        .WillOnce(Return(MetricRet::kOtherErr))
        .WillRepeatedly(DoAll(SetArgPointee<2>("0.0.1"),
                              Return(MetricRet::kOK)));
    ASSERT_EQ(0, versionTool.GetAndCheckChunkServerVersion(&version,
                                                            &failedList));
    std::vector<std::string> expectList = {"127.0.0.1:9191"};
    ASSERT_EQ(expectList, failedList);

    // 4、chunkserverList为空
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(std::vector<ChunkServerInfo>()),
                        Return(0)));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(0);
    ASSERT_EQ(-1, versionTool.GetAndCheckChunkServerVersion(&version,
                                                            &failedList));
    ASSERT_TRUE(failedList.empty());

    // 5、version不一致
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(5)
        .WillOnce(DoAll(SetArgPointee<2>("0.0.2"),
                        Return(MetricRet::kOK)))
        .WillOnce(Return(MetricRet::kNotFound))
        .WillRepeatedly(DoAll(SetArgPointee<2>("0.0.1"),
                        Return(MetricRet::kOK)));
    ASSERT_EQ(-1, versionTool.GetAndCheckChunkServerVersion(&version,
                                                            &failedList));
    ASSERT_TRUE(failedList.empty());

    // 6、老版本
    EXPECT_CALL(*mdsClient_, ListChunkServersInCluster(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkservers),
                        Return(0)));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(5)
        .WillRepeatedly(Return(MetricRet::kNotFound));
    ASSERT_EQ(0, versionTool.GetAndCheckChunkServerVersion(&version,
                                                           &failedList));
    ASSERT_EQ("before0.0.5.2", version);
    ASSERT_TRUE(failedList.empty());
}

TEST_F(VersionToolTest, GetClientVersion) {
    VersionTool versionTool(mdsClient_, metricClient_);
    std::vector<std::string> clientAddrs =
                {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"};

    // 1、正常情况,三个client有一个访问失败，剩下两个version不一致
    EXPECT_CALL(*mdsClient_, ListClient(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(clientAddrs),
                  Return(0)));
    EXPECT_CALL(*metricClient_, GetMetric(_, _, _))
        .Times(3)
        .WillOnce(DoAll(SetArgPointee<2>("0.0.1"),
                        Return(MetricRet::kOK)))
        .WillOnce(Return(MetricRet::kNotFound))
        .WillOnce(Return(MetricRet::kOtherErr));
    VersionMapType versionMap;
    std::vector<std::string> offlineList;
    ASSERT_EQ(0, versionTool.GetClientVersion(&versionMap, &offlineList));
    VersionMapType expected = {{"0.0.1", {"127.0.0.1:8000"}},
                               {"before0.0.5.2", {"127.0.0.1:8001"}}};
    ASSERT_EQ(expected, versionMap);
    std::vector<std::string> expectedList = {"127.0.0.1:8002"};
    ASSERT_EQ(expectedList, offlineList);

    // 2、ListClient失败
    EXPECT_CALL(*mdsClient_, ListClient(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, versionTool.GetClientVersion(&versionMap, &offlineList));
}

}  // namespace tool
}  // namespace curve
