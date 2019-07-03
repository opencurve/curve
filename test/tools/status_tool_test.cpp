/*
 * Project: curve
 * File Created: 2019-07-17
 * Author: hzchenwei7
 * Copyright (c)￼ 2018 netease
 */
#include <gtest/gtest.h>
#include "src/tools/status_tool.h"
#include "test/mds/mock/mock_repo.h"

using curve::mds::MockRepo;
using curve::mds::kGB;
using ::testing::AtLeast;
using ::testing::StrEq;
using ::testing::_;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::DoAll;
using ::testing::SetArgPointee;

class StatusToolTest : public ::testing::Test {
 protected:
    void SetUp() {
        mockRepo_ = std::make_shared<MockRepo>();
    }
    void TearDown() {
        mockRepo_ = nullptr;
    }
    std::shared_ptr<MockRepo> mockRepo_;
};

TEST_F(StatusToolTest, common) {
    Configuration conf;
    conf.SetStringValue("mds.DbName", "curve_mds_repo_test");
    conf.SetStringValue("mds.DbUser", "root");
    conf.SetStringValue("mds.DbUrl", "localhost");
    conf.SetStringValue("mds.DbPassword", "qwer");
    conf.SetStringValue("mds.DbPoolSize", "2");

    curve::tool::StatusTool statusTool;
    statusTool.PrintHelp();

    EXPECT_CALL(*mockRepo_, connectDB("curve_mds_repo_test",
                                        "root", "localhost", "qwer", 2))
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createDatabase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, useDataBase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createAllTables())
        .Times(1)
        .WillOnce(Return(OperationOK));

    ASSERT_EQ(statusTool.InitMdsRepo(&conf, mockRepo_), 0);

    std::vector<ChunkServerRepoItem> chunkServerRepoList;
    chunkServerRepoList.push_back(ChunkServerRepoItem(0x41, "token1", "ssd",
        "ip1", 1024, 0x31, ChunkServerStatus::READWRITE, DiskState::DISKNORMAL,
        OnlineState::ONLINE, "/a", 100 * kGB, 99 * kGB));
    chunkServerRepoList.push_back(ChunkServerRepoItem(0x42, "token2", "ssd",
        "ip2", 1025, 0x32, ChunkServerStatus::READWRITE, DiskState::DISKNORMAL,
        OnlineState::ONLINE, "/b", 100 * kGB, 99 * kGB));
    chunkServerRepoList.push_back(ChunkServerRepoItem(0x43, "token3", "hdd",
        "ip3", 1026, 0x33, ChunkServerStatus::READWRITE, DiskState::DISKNORMAL,
        OnlineState::ONLINE, "/c", 100 * kGB, 99 * kGB));
    EXPECT_CALL(*mockRepo_, LoadChunkServerRepoItems(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkServerRepoList),
                            Return(OperationOK)));
    ASSERT_EQ(statusTool.RunCommand("space"), 0);

    EXPECT_CALL(*mockRepo_, LoadChunkServerRepoItems(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkServerRepoList),
                            Return(OperationOK)));
    ASSERT_EQ(statusTool.RunCommand("status"), 0);

    EXPECT_CALL(*mockRepo_, LoadChunkServerRepoItems(_))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<0>(chunkServerRepoList),
                            Return(OperationOK)));
    ASSERT_EQ(statusTool.RunCommand("chunkserver-list"), 0);
    ASSERT_EQ(statusTool.RunCommand("other-cmd"), 0);
}

TEST_F(StatusToolTest, dbInitError) {
    Configuration conf;
    conf.SetStringValue("mds.DbName", "curve_mds_repo_test");
    conf.SetStringValue("mds.DbUser", "root");
    conf.SetStringValue("mds.DbUrl", "localhost");
    conf.SetStringValue("mds.DbPassword", "qwer");
    conf.SetStringValue("mds.DbPoolSize", "2");

    curve::tool::StatusTool statusTool;

    // connectDB fail
    EXPECT_CALL(*mockRepo_, connectDB("curve_mds_repo_test",
                                        "root", "localhost", "qwer", 2))
        .Times(1)
        .WillOnce(Return(InternalError));
    ASSERT_EQ(statusTool.InitMdsRepo(&conf, mockRepo_), -1);

    // createDatabase fail
    EXPECT_CALL(*mockRepo_, connectDB("curve_mds_repo_test",
                                        "root", "localhost", "qwer", 2))
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createDatabase())
        .Times(1)
        .WillOnce(Return(InternalError));
    ASSERT_EQ(statusTool.InitMdsRepo(&conf, mockRepo_), -1);

    // useDataBase fail
    EXPECT_CALL(*mockRepo_, connectDB("curve_mds_repo_test",
                                        "root", "localhost", "qwer", 2))
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createDatabase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, useDataBase())
        .Times(1)
        .WillOnce(Return(InternalError));
    ASSERT_EQ(statusTool.InitMdsRepo(&conf, mockRepo_), -1);

    // createAllTables fail
    EXPECT_CALL(*mockRepo_, connectDB("curve_mds_repo_test",
                                        "root", "localhost", "qwer", 2))
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createDatabase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, useDataBase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createAllTables())
        .Times(1)
        .WillOnce(Return(InternalError));
    ASSERT_EQ(statusTool.InitMdsRepo(&conf, mockRepo_), -1);
}

TEST_F(StatusToolTest, runCommandError) {
    Configuration conf;
    conf.SetStringValue("mds.DbName", "curve_mds_repo_test");
    conf.SetStringValue("mds.DbUser", "root");
    conf.SetStringValue("mds.DbUrl", "localhost");
    conf.SetStringValue("mds.DbPassword", "qwer");
    conf.SetStringValue("mds.DbPoolSize", "2");

    curve::tool::StatusTool statusTool;
    statusTool.PrintHelp();

    EXPECT_CALL(*mockRepo_, connectDB("curve_mds_repo_test",
                                        "root", "localhost", "qwer", 2))
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createDatabase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, useDataBase())
        .Times(1)
        .WillOnce(Return(OperationOK));
    EXPECT_CALL(*mockRepo_, createAllTables())
        .Times(1)
        .WillOnce(Return(OperationOK));

    ASSERT_EQ(statusTool.InitMdsRepo(&conf, mockRepo_), 0);

    EXPECT_CALL(*mockRepo_, LoadChunkServerRepoItems(_))
        .Times(1)
        .WillOnce(Return(InternalError));
    ASSERT_EQ(statusTool.RunCommand("space"), -1);

    EXPECT_CALL(*mockRepo_, LoadChunkServerRepoItems(_))
        .Times(1)
        .WillOnce(Return(InternalError));
    ASSERT_EQ(statusTool.RunCommand("status"), -1);

    EXPECT_CALL(*mockRepo_, LoadChunkServerRepoItems(_))
        .Times(1)
        .WillOnce(Return(InternalError));
    ASSERT_EQ(statusTool.RunCommand("chunkserver-list"), -1);

    ASSERT_EQ(statusTool.RunCommand("other-cmd"), 0);
}
