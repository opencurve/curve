/*
 * Project: curve
 * File Created: 2020-03-02
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <fstream>
#include <memory>
#include "src/tools/curve_meta_tool.h"
#include "test/fs/mock_local_filesystem.h"

namespace curve {
namespace tool {

using curve::common::Bitmap;
using curve::fs::MockLocalFileSystem;
using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

const uint32_t PAGE_SIZE = 4 * 1024;
const uint32_t CHUNK_SIZE = 16 * 1024 * 1024;
const char chunkFileName[] = "chunk_001";

class CurveMetaToolTest : public ::testing::Test {
 protected:
    void SetUp() {
        localFs_ = std::make_shared<MockLocalFileSystem>();
    }
    void TearDown() {
        localFs_ = nullptr;
    }
    std::shared_ptr<MockLocalFileSystem> localFs_;
};

TEST_F(CurveMetaToolTest, SupportCommand) {
    ASSERT_TRUE(CurveMetaTool::SupportCommand("chunk-meta"));
    ASSERT_TRUE(CurveMetaTool::SupportCommand("snapshot-meta"));
    ASSERT_FALSE(CurveMetaTool::SupportCommand("raft-log-meta"));
    CurveMetaTool curveMetaTool(localFs_);
    ASSERT_EQ(-1, curveMetaTool.RunCommand("raft-log-meta"));
    curveMetaTool.PrintHelp("chunk-meta");
    curveMetaTool.PrintHelp("snapshot-meta");
    curveMetaTool.PrintHelp("raft-log-meta");
}

TEST_F(CurveMetaToolTest, PrintChunkMeta) {
    CurveMetaTool curveMetaTool(localFs_);
    // 1、文件不存在
    EXPECT_CALL(*localFs_, Open(_, _))
        .Times(6)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(1));
    EXPECT_CALL(*localFs_, Close(_))
        .Times(5)
        .WillRepeatedly(Return(-1));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("chunk-meta"));
    // 2、读取meta page失败
    EXPECT_CALL(*localFs_, Read(_, _, 0, PAGE_SIZE))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(10));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("chunk-meta"));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("chunk-meta"));
    // 3、解析失败
    char buf[PAGE_SIZE] = {0};
    EXPECT_CALL(*localFs_, Read(_, _, 0, PAGE_SIZE))
        .Times(1)
        .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + PAGE_SIZE),
                  Return(PAGE_SIZE)));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("chunk-meta"));
    // 4、普通chunk
    ChunkFileMetaPage metaPage;
    metaPage.version = 1;
    metaPage.sn = 1;
    metaPage.correctedSn = 2;
    metaPage.encode(buf);
    EXPECT_CALL(*localFs_, Read(_, _, 0, PAGE_SIZE))
        .Times(1)
        .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + PAGE_SIZE),
                  Return(PAGE_SIZE)));
    ASSERT_EQ(0, curveMetaTool.RunCommand("chunk-meta"));
    // 5、克隆chunk
    metaPage.location = "test@s3";
    uint32_t size = CHUNK_SIZE / PAGE_SIZE;
    auto bitmap = std::make_shared<Bitmap>(size);
    bitmap->Set(0, 2);
    bitmap->Set(size - 1);
    metaPage.bitmap = bitmap;
    metaPage.encode(buf);
    EXPECT_CALL(*localFs_, Read(_, _, 0, PAGE_SIZE))
        .Times(1)
        .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + PAGE_SIZE),
                  Return(PAGE_SIZE)));
    ASSERT_EQ(0, curveMetaTool.RunCommand("chunk-meta"));
}

TEST_F(CurveMetaToolTest, PrintSnapshotMeta) {
    CurveMetaTool curveMetaTool(localFs_);
    // 1、文件不存在
    EXPECT_CALL(*localFs_, Open(_, _))
        .Times(5)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(1));
    EXPECT_CALL(*localFs_, Close(_))
        .Times(4)
        .WillRepeatedly(Return(-1));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("snapshot-meta"));
    // 2、读取meta page失败
    EXPECT_CALL(*localFs_, Read(_, _, 0, PAGE_SIZE))
        .Times(2)
        .WillOnce(Return(-1))
        .WillOnce(Return(10));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("snapshot-meta"));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("snapshot-meta"));
    // 3、解析失败
    char buf[PAGE_SIZE] = {0};
    EXPECT_CALL(*localFs_, Read(_, _, 0, PAGE_SIZE))
        .Times(1)
        .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + PAGE_SIZE),
                  Return(PAGE_SIZE)));
    ASSERT_EQ(-1, curveMetaTool.RunCommand("snapshot-meta"));
    // 4、成功chunk
    SnapshotMetaPage metaPage;
    metaPage.version = 1;
    metaPage.sn = 1;
    metaPage.damaged = false;
    uint32_t size = CHUNK_SIZE / PAGE_SIZE;
    auto bitmap = std::make_shared<Bitmap>(size);
    bitmap->Set(0, 2);
    bitmap->Set(size - 1);
    metaPage.bitmap = bitmap;
    metaPage.encode(buf);
    EXPECT_CALL(*localFs_, Read(_, _, 0, PAGE_SIZE))
        .Times(1)
        .WillOnce(DoAll(SetArrayArgument<1>(buf, buf + PAGE_SIZE),
                  Return(PAGE_SIZE)));
    ASSERT_EQ(0, curveMetaTool.RunCommand("snapshot-meta"));
}
}  // namespace tool
}  // namespace curve

