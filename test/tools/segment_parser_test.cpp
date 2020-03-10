/*
 * Project: curve
 * File Created: 2020-03-06
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#include <gtest/gtest.h>
#include <fstream>
#include <memory>
#include "src/tools/raft_log_tool.h"
#include "test/fs/mock_local_filesystem.h"

namespace curve {
namespace tool {

using curve::fs::MockLocalFileSystem;
using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;
using ::testing::SetArrayArgument;

const char fileName[] = "log_inprogress_001";
const uint32_t DATA_LEN = 20;

class SetmentParserTest : public ::testing::Test {
 protected:
    void SetUp() {
        localFs_ = std::make_shared<MockLocalFileSystem>();
    }
    void TearDown() {
        localFs_ = nullptr;
    }

    void PackHeader(const EntryHeader& header, char* buf,
                    bool checkFail = false) {
        memset(buf, 0, ENTRY_HEADER_SIZE);
        const uint32_t meta_field = (header.type << 24) |
                                        (header.checksum_type << 16);
        butil::RawPacker packer(buf);
        packer.pack64(header.term)
              .pack32(meta_field)
              .pack32((uint32_t)header.data_len)
              .pack32(header.data_checksum);
        uint32_t checkSum = braft::murmurhash32(buf, ENTRY_HEADER_SIZE - 4);
        if (checkFail) {
            packer.pack32(checkSum + 1);
        } else {
            packer.pack32(checkSum);
        }
    }

    std::shared_ptr<MockLocalFileSystem> localFs_;
};

TEST_F(SetmentParserTest, Init) {
    SegmentParser parser(localFs_);
    // 1、打开文件失败
    EXPECT_CALL(*localFs_, Open(_, _))
        .Times(3)
        .WillOnce(Return(-1))
        .WillRepeatedly(Return(1));
    ASSERT_EQ(-1, parser.Init(fileName));

    // 2、获取文件大小失败
    EXPECT_CALL(*localFs_, Fstat(_, _))
        .Times(1)
        .WillOnce(Return(-1));
    ASSERT_EQ(-1, parser.Init(fileName));

    // 3、成功
    EXPECT_CALL(*localFs_, Fstat(_, _))
        .Times(1)
        .WillOnce(Return(0));
    ASSERT_EQ(0, parser.Init(fileName));

    // 4、反初始化
    EXPECT_CALL(*localFs_, Close(_))
        .Times(1)
        .WillOnce(Return(0));
    parser.UnInit();
}

TEST_F(SetmentParserTest, GetNextEntryHeader) {
    SegmentParser parser(localFs_);
    struct stat stBuf;
    stBuf.st_size = 88;

    EXPECT_CALL(*localFs_, Open(_, _))
        .Times(1)
        .WillOnce(Return(1));
    EXPECT_CALL(*localFs_, Fstat(_, _))
        .Times(1)
        .WillOnce(DoAll(SetArgPointee<1>(stBuf),
                  Return(0)));
    ASSERT_EQ(0, parser.Init(fileName));

    EntryHeader header;
    EntryHeader header2;
    header.term = 10;
    header.type = 2;
    header.checksum_type = 0;
    header.data_len = DATA_LEN;
    header.data_checksum = 73235795;
    char header_buf[ENTRY_HEADER_SIZE] = {0};

    // 读出来的数据大小不对
    EXPECT_CALL(*localFs_, Read(_, _, _, _))
        .Times(1)
        .WillOnce(Return(22));
    ASSERT_FALSE(parser.GetNextEntryHeader(&header2));
    ASSERT_FALSE(parser.SuccessfullyFinished());

    // 校验失败
    PackHeader(header, header_buf, true);
    EXPECT_CALL(*localFs_, Read(_, _, _, _))
        .Times(1)
        .WillOnce(DoAll(SetArrayArgument<1>(header_buf,
                                            header_buf + ENTRY_HEADER_SIZE),
                  Return(24)));
    ASSERT_FALSE(parser.GetNextEntryHeader(&header2));
    ASSERT_FALSE(parser.SuccessfullyFinished());

    // 正常情况
    PackHeader(header, header_buf);
    EXPECT_CALL(*localFs_, Read(_, _, _, _))
        .Times(2)
        .WillRepeatedly(DoAll(SetArrayArgument<1>(header_buf,
                                              header_buf + ENTRY_HEADER_SIZE),
                  Return(24)));
    ASSERT_TRUE(parser.GetNextEntryHeader(&header2));
    ASSERT_EQ(header, header2);
    ASSERT_TRUE(parser.GetNextEntryHeader(&header2));
    ASSERT_EQ(header, header2);
    ASSERT_FALSE(parser.GetNextEntryHeader(&header2));
    ASSERT_EQ(header, header2);
    ASSERT_TRUE(parser.SuccessfullyFinished());
}

}  // namespace tool
}  // namespace curve

