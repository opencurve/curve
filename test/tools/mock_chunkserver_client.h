/*
 * Project: curve
 * File Created: 2019-11-28
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef TEST_TOOLS_MOCK_CHUNKSERVER_CLIENT_H_
#define TEST_TOOLS_MOCK_CHUNKSERVER_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include "src/tools/chunkserver_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockChunkServerClient : public ChunkServerClient {
 public:
    MockChunkServerClient() {}
    ~MockChunkServerClient() {}
    MOCK_METHOD1(Init, int(const std::string&));
    MOCK_METHOD1(GetCopysetStatus, int(butil::IOBuf*));
    MOCK_METHOD0(CheckChunkServerOnline, bool());
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_CHUNKSERVER_CLIENT_H_
