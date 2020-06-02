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
 * File Created: 2020-03-06
 * Author: charisu
 */


#ifndef TEST_TOOLS_MOCK_MOCK_SEGMENT_PARSER_H_
#define TEST_TOOLS_MOCK_MOCK_SEGMENT_PARSER_H_

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
#endif  // TEST_TOOLS_MOCK_MOCK_SEGMENT_PARSER_H_
