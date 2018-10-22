/*
 * Project: curve
 * Created Date: 18-11-25
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_TEST_UTIL_H
#define CURVE_CHUNKSERVER_TEST_UTIL_H

#include <butil/status.h>

#include <string>

#include "include/chunkserver/chunkserver_common.h"

namespace curve {
namespace chunkserver {

std::string Exec(const char *cmd);

int StartChunkserver(const char *ip,
                     int port,
                     const char *copysetdir,
                     const char *confs,
                     const int snapshotInterval);

butil::Status WaitLeader(const LogicPoolID &logicPoolId,
                         const CopysetID &copysetId,
                         const Configuration &conf,
                         PeerId *leaderId,
                         int electionTimeoutMs);

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_TEST_UTIL_H
