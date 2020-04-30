/*
 * Project: curve
 * File Created: 2020-02-20
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */


#ifndef TEST_TOOLS_MOCK_VERSION_TOOL_H_
#define TEST_TOOLS_MOCK_VERSION_TOOL_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include "src/tools/version_tool.h"
#include "test/tools/mock_snapshot_clone_client.h"

using ::testing::Return;
namespace curve {
namespace tool {
class MockVersionTool : public VersionTool {
 public:
    MockVersionTool() : VersionTool(std::make_shared<MDSClient>(),
                                 std::make_shared<MetricClient>(),
                                 std::make_shared<MockSnapshotCloneClient>()) {}
    MOCK_METHOD2(GetAndCheckMdsVersion, int(std::string*,
                                            std::vector<std::string>*));
    MOCK_METHOD2(GetAndCheckChunkServerVersion, int(std::string*,
                                                    std::vector<std::string>*));
    MOCK_METHOD1(GetClientVersion, int(ClientVersionMapType*));
    MOCK_METHOD2(GetAndCheckSnapshotCloneVersion, int(std::string*,
                                                    std::vector<std::string>*));
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_VERSION_TOOL_H_
