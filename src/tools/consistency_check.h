/*
 * Project: curve
 * File Created: Saturday, 29th June 2019 12:35:00 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_TOOLS_CONSISTENCY_CHECK_H_
#define SRC_TOOLS_CONSISTENCY_CHECK_H_

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <brpc/channel.h>
#include <brpc/controller.h>

#include <vector>
#include <string>
#include <iostream>

#include "proto/copyset.pb.h"
#include "src/client/mds_client.h"
#include "src/client/config_info.h"
#include "src/client/client_config.h"
#include "src/client/client_common.h"
#include "src/client/metacache_struct.h"

DECLARE_string(config_path);
DECLARE_string(filename);
DECLARE_uint64(filesize);
DECLARE_uint64(chunksize);
DECLARE_uint64(segmentsize);
DECLARE_string(username);
DECLARE_bool(check_hash);

class CheckFileConsistency {
 public:
    CheckFileConsistency() {}
    ~CheckFileConsistency() = default;

    bool Init();

    bool FetchFileCopyset();

    bool ReplicasConsistency();

    void UnInit();

    void PrintHelp();

 private:
    curve::client::LogicPoolID  lpid_;
    curve::client::MDSClient    mdsclient_;
    std::vector<curve::client::CopysetInfo_t> copysetInfo;
};

#endif  // SRC_TOOLS_CONSISTENCY_CHECK_H_
