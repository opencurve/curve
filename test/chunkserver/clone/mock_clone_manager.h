/*
 * Project: curve
 * Created Date: Saturday March 30th 2019
 * Author: yangyaokai
 * Copyright (c) 2019 netease
 */

#ifndef TEST_CHUNKSERVER_CLONE_MOCK_CLONE_MANAGER_H_
#define TEST_CHUNKSERVER_CLONE_MOCK_CLONE_MANAGER_H_

#include <gmock/gmock.h>
#include <string>

#include "src/chunkserver/clone_manager.h"

namespace curve {
namespace chunkserver {

class MockCloneManager : public CloneManager {
 public:
    MockCloneManager() = default;
    ~MockCloneManager() = default;
    MOCK_METHOD1(Init, int(const CloneOptions&));
    MOCK_METHOD0(Run, int());
    MOCK_METHOD0(Fini, int());
    MOCK_METHOD2(GenerateCloneTask, std::shared_ptr<CloneTask>(
        std::shared_ptr<ReadChunkRequest>, ::google::protobuf::Closure*));
    MOCK_METHOD1(IssueCloneTask, bool(std::shared_ptr<CloneTask>));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_CLONE_MOCK_CLONE_MANAGER_H_
