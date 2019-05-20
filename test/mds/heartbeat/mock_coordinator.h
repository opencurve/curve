/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef TEST_MDS_HEARTBEAT_MOCK_COORDINATOR_H_
#define TEST_MDS_HEARTBEAT_MOCK_COORDINATOR_H_

#include <gmock/gmock.h>
#include "src/mds/schedule/coordinator.h"

namespace curve {
namespace mds {
namespace heartbeat {
class MockCoordinator : public ::curve::mds::schedule::Coordinator {
 public:
    MockCoordinator() {}
    ~MockCoordinator() {}

    MOCK_METHOD2(CopySetHeartbeat,
                 ChunkServerIDType(
                    const ::curve::mds::topology::CopySetInfo &originInfo,
                    ::curve::mds::heartbeat::CopysetConf *newConf));
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
#endif  // TEST_MDS_HEARTBEAT_MOCK_COORDINATOR_H_

