/*
 * Project: curve
 * File Created: 2019-07-17
 * Author: hzchenwei7
 * Copyright (c)￼ 2018 netease
 */

#ifndef SRC_TOOLS_STATUS_TOOL_H_
#define SRC_TOOLS_STATUS_TOOL_H_

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <brpc/channel.h>
#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <map>
#include "src/mds/dao/mdsRepo.h"
#include "src/repo/repo.h"
#include "proto/nameserver2.pb.h"
#include "proto/topology.pb.h"
#include "src/common/timeutility.h"
#include "src/mds/common/mds_define.h"
#include "src/common/configuration.h"

using curve::common::Configuration;
using curve::mds::topology::ChunkServerStatus;
using curve::mds::topology::DiskState;
using curve::mds::topology::OnlineState;
using curve::mds::ChunkServerRepoItem;

namespace curve {
namespace tool {
class StatusTool {
 public:
    StatusTool() {}
    ~StatusTool() = default;

    /**
     *  @brief 打印help信息
     *  @param 无
     *  @return 无
     */
    void PrintHelp();

    /**
     *  @brief 初始化mds repo
     *  @param conf：配置文件，从中读取mdsrepo所需的db的信息
     *         MdsRepo：需要初始化的mdsrepo
     *  @return 成功返回0，失败返回-1
     */
    int InitMdsRepo(Configuration *conf, std::shared_ptr<curve::mds::MdsRepo>);

    /**
     *  @brief 执行命令
     *  @param cmd：执行的命令
     *  @return 成功返回0，失败返回-1
     */
    int RunCommand(const std::string &cmd);

 private:
    int SpaceCmd();
    int StatusCmd();
    int ChunkServerCmd();
    std::shared_ptr<curve::mds::MdsRepo> mdsRepo_;
};
}  // namespace tool
}  // namespace curve

#endif  // SRC_TOOLS_STATUS_TOOL_H_
