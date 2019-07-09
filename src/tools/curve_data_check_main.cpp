/*
 * Project: curve
 * File Created: Wednesday, 26th June 2019 10:47:52 am
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <vector>
#include <string>

#include "proto/copyset.pb.h"
#include "src/client/mds_client.h"
#include "src/client/config_info.h"
#include "src/client/client_config.h"
#include "src/client/client_common.h"
#include "src/client/metacache_struct.h"
#include "src/tools/consistency_check.h"

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::ParseCommandLineFlags(&argc, &argv, false);

    CheckFileConsistency cfc;

    bool ret = cfc.Init();
    LOG_IF(ERROR, !ret) << "init failed!";
    if (!ret) { return -1; }

    ret = cfc.FetchFileCopyset();
    LOG_IF(ERROR, !ret) << "FetchFileCopyset failed!";
    if (!ret) { return -1; }

    int rc = cfc.ReplicasConsistency() ? 0 : -1;

    rc == 0 ? LOG(INFO) << "consistency check success!"
            : LOG(ERROR) << "consistency check failed!";

    cfc.UnInit();

    return rc;
}
