/*
 * Project: curve
 * Created Date: 19-05-03
 * Author: wudemiao
 * Copyright (c) 2019 netease
 */

#ifndef TEST_CHUNKSERVER_MOCK_NODE_H_
#define TEST_CHUNKSERVER_MOCK_NODE_H_

#include <gmock/gmock.h>
#include <gmock/gmock-generated-function-mockers.h>
#include <gmock/internal/gmock-generated-internal-utils.h>
#include <braft/raft.h>

#include <string>
#include <vector>

#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

class MockNode : public braft::Node {
 public:
    MockNode(const LogicPoolID &logicPoolId,
             const CopysetID &copysetId)
        : braft::Node(ToGroupIdString(logicPoolId, copysetId),
                      PeerId("127.0.0.1:3200:0")) {
    }

    MOCK_METHOD4(conf_changes, bool(Configuration *old_conf,
                                    Configuration *adding,
                                    Configuration *removing,
                                    PeerId *transferee_peer));
};

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_MOCK_NODE_H_
