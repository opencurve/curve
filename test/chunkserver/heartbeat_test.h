/*
 * Copyright (C) 2018 NetEase Inc. All rights reserved.
 * Project: Curve
 *
 * History:
 *          2018/12/23  Wenyu Zhou   Initial version
 */

#ifndef TEST_CHUNKSERVER_HEARTBEAT_TEST_H_
#define TEST_CHUNKSERVER_HEARTBEAT_TEST_H_

#include <braft/node_manager.h>
#include <braft/node.h>                  // NodeImpl

#include <string>
#include <atomic>
#include <thread>  // NOLINT

#include "include/chunkserver/chunkserver_common.h"
#include "src/chunkserver/copyset_node_manager.h"
#include "proto/heartbeat.pb.h"

namespace curve {
namespace chunkserver {

int RmDirData(std::string uri);
int RemovePeersData(bool rmChunkServerMeta = false);

}  // namespace chunkserver
}  // namespace curve

#endif  // TEST_CHUNKSERVER_HEARTBEAT_TEST_H_
