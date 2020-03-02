/*
 * Project: curve
 * File Created: 2020-03-02
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <fstream>
#include <memory>
#include "src/tools/raft_log_tool.h"
#include "test/tools/mock_segment_parser.h"

DECLARE_string(fileName);
using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;

namespace curve {
namespace tool {

class RaftLogToolTest : public ::testing::Test {
 protected:
    void SetUp() {
        parser_ = std::make_shared<MockSegmentParser>();
    }
    void TearDown() {
        parser_ = nullptr;
    }

    std::shared_ptr<MockSegmentParser> parser_;
};

TEST_F(RaftLogToolTest, SupportCommand) {
    ASSERT_TRUE(RaftLogTool::SupportCommand("raft-log-meta"));
    ASSERT_FALSE(RaftLogTool::SupportCommand("chunk-meta"));
}

TEST_F(RaftLogToolTest, PrintHeaders) {
    RaftLogTool raftLogTool(parser_);
    raftLogTool.PrintHelp("raft-log-meta");
    raftLogTool.PrintHelp("chunk-meta");
    ASSERT_EQ(-1, raftLogTool.RunCommand("chunk-meta"));

    // 文件名格式不对
    FLAGS_fileName = "illegalfilename";
    ASSERT_EQ(-1, raftLogTool.RunCommand("raft-log-meta"));
    FLAGS_fileName = "/tmp/illegalfilename";
    ASSERT_EQ(-1, raftLogTool.RunCommand("raft-log-meta"));

    // parser初始化失败
    FLAGS_fileName = "/tmp/log_inprogress_002";
    EXPECT_CALL(*parser_, Init(_))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, raftLogTool.RunCommand("raft-log-meta"));

    // 解析失败
    EXPECT_CALL(*parser_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*parser_, GetNextEntryHeader(_))
        .Times(1)
        .WillOnce(Return(false));
    EXPECT_CALL(*parser_, SuccessfullyFinished())
        .Times(1)
        .WillOnce(Return(false));
    ASSERT_EQ(-1, raftLogTool.RunCommand("raft-log-meta"));

    // 正常情况
    EXPECT_CALL(*parser_, Init(_))
        .Times(1)
        .WillOnce(Return(0));
    EXPECT_CALL(*parser_, GetNextEntryHeader(_))
        .Times(3)
        .WillOnce(Return(true))
        .WillOnce(Return(true))
        .WillOnce(Return(false));
    EXPECT_CALL(*parser_, SuccessfullyFinished())
        .Times(1)
        .WillOnce(Return(true));
    ASSERT_EQ(0, raftLogTool.RunCommand("raft-log-meta"));
}

}  // namespace tool
}  // namespace curve

