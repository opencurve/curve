/*
 * Project: curve
 * Created Date: Sat Jan 05 2019
 * Author: lixiaocui
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_TEST_MDS_HEARTBEAT_COMMON_H
#define CURVE_TEST_MDS_HEARTBEAT_COMMON_H

#include "proto/heartbeat.pb.h"

namespace curve {
namespace mds {
namespace heartbeat {
ChunkServerHeartbeatRequest GetChunkServerHeartbeatRequestForTest();
}  // namespace heartbeat
}  // namespace mds
}  // namespace curve
#endif  // CURVE_TEST_MDS_HEARTBEAT_COMMON_H


