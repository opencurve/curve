/*
 * Project: curve
 * Created Date: 2020-01-14
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef TEST_CHUNKSERVER_MOCK_COPYSET_NODE_MANAGER_H_
#define TEST_CHUNKSERVER_MOCK_COPYSET_NODE_MANAGER_H_

#include <gmock/gmock.h>
#include "src/chunkserver/copyset_node_manager.h"

namespace curve {
namespace chunkserver {
class MockCopysetNodeManager : public CopysetNodeManager {
 public:
    MockCopysetNodeManager() {}
    ~MockCopysetNodeManager() {}

    MOCK_METHOD0(LoadFinished, bool());
};
}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_COPYSET_NODE_MANAGER_H_
