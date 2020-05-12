/*
 * Project: curve
 * File Created: 2019-12-3
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */


#ifndef TEST_TOOLS_MOCK_NAMESPACE_TOOL_CORE_H_
#define TEST_TOOLS_MOCK_NAMESPACE_TOOL_CORE_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <utility>
#include <memory>
#include "src/tools/chunkserver_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockNameSpaceToolCore : public NameSpaceToolCore {
 public:
    MockNameSpaceToolCore() : NameSpaceToolCore(
                        std::make_shared<MDSClient>()) {}
    ~MockNameSpaceToolCore() {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD2(GetFileInfo, int(const std::string&, FileInfo*));
    MOCK_METHOD2(ListDir, int(const std::string&, std::vector<FileInfo>*));
    MOCK_METHOD3(GetChunkServerListInCopySet, int(const PoolIdType&,
                                     const CopySetIdType&,
                                     std::vector<ChunkServerLocation>*));
    MOCK_METHOD2(DeleteFile, int(const std::string&, bool));
    MOCK_METHOD2(CreateFile, int(const std::string&, uint64_t));
    MOCK_METHOD3(GetAllocatedSize, int(const std::string&, uint64_t*,
                                       uint64_t*));
    MOCK_METHOD2(GetFileSegments, int(const std::string&,
                                  std::vector<PageFileSegment>*));
    MOCK_METHOD4(QueryChunkCopyset, int(const std::string&, uint64_t,
                          uint64_t*,
                          std::pair<uint32_t, uint32_t>*));
    MOCK_METHOD1(CleanRecycleBin, int(const std::string&));
    MOCK_METHOD2(GetFileSize, int(const std::string&, uint64_t*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_NAMESPACE_TOOL_CORE_H_
