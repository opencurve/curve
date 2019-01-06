/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */


#ifndef CURVE_TEST_MDS_HEARTBEAT_MOCK_COORDINATOR_H
#define CURVE_TEST_MDS_HEARTBEAT_MOCK_COORDINATOR_H

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
               bool(const ::curve::mds::schedule::CopySetInfo &originInfo,
                   ::curve::mds::schedule::CopySetConf *newConf));
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
#endif  // CURVE_TEST_MDS_HEARTBEAT_MOCK_COORDINATOR_H

