/*
 * Project: curve
 * File Created: 2020-03-17
 * Author: charisu
 * Copyright (c)￼ 2018 netease
 */

#ifndef TEST_TOOLS_MOCK_SNAPSHOT_CLONE_CLIENT_H_
#define TEST_TOOLS_MOCK_SNAPSHOT_CLONE_CLIENT_H_

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <string>
#include <map>
#include <memory>
#include <vector>
#include "src/tools/snapshot_clone_client.h"

namespace curve {
namespace tool {
class MockSnapshotCloneClient : public SnapshotCloneClient {
 public:
    MockSnapshotCloneClient() :
            SnapshotCloneClient(std::make_shared<MetricClient>()) {}
    MOCK_METHOD2(Init, int(const std::string&, const std::string&));
    MOCK_METHOD0(GetActiveAddrs, std::vector<std::string>());
    MOCK_METHOD1(GetOnlineStatus, void(std::map<std::string, bool>*));
    MOCK_CONST_METHOD0(GetDummyServerMap,
                    const std::map<std::string, std::string>&());
};
}  // namespace tool
}  // namespace curve
#endif  // TEST_TOOLS_MOCK_SNAPSHOT_CLONE_CLIENT_H_
