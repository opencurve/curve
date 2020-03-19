/*
 * Project: curve
 * File Created: 2020-03-06
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */


#ifndef TEST_TOOLS_MOCK_SEGMENT_PARSER_H_
#define TEST_TOOLS_MOCK_SEGMENT_PARSER_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include "src/tools/raft_log_tool.h"
#include "test/fs/mock_local_filesystem.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockSegmentParser : public SegmentParser {
 public:
    MockSegmentParser() : SegmentParser(
                std::make_shared<curve::fs::MockLocalFileSystem>()) {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD0(UnInit, void());
    MOCK_METHOD1(GetNextEntryHeader, bool(EntryHeader* header));
    MOCK_METHOD0(SuccessfullyFinished, bool());
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_SEGMENT_PARSER_H_
