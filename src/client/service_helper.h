/*
 * Project: curve
 * File Created: Wednesday, 26th December 2018 12:28:16 pm
 * Author: tongguangxun
 * Copyright (c)￼ 2018 netease
 */

#ifndef CURVE_CLIENT_SERVICE_ADAPTOR_H
#define CURVE_CLIENT_SERVICE_ADAPTOR_H

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <stdint.h>
#include <vector>
#include <string>
#include "proto/cli.pb.h"
#include "proto/nameserver2.pb.h"
#include "src/client/client_common.h"

namespace curve {
namespace client {
// ServiceHelper是client端RPC服务的一些工具
class ServiceHelper {
 public:
    /**
     * proto格式的FInfo转换为本地格式的FInfo
     * @param: finfo为proto格式的文件信息
     * @param: fi为本地格式的文件信息
     */
    static void ProtoFileInfo2Local(curve::mds::FileInfo* finfo, FInfo_t* fi);
    /**
     * 从chunkserver端获取最新的leader信息
     * @param: logicPoolId为逻辑池ID
     * @param: copysetId为复制组的ID
     * @param: conf为当前复制组的raft配置信息
     * @param: leaderId是出参，返回当前copyset的leader信息
     * @return: 成功返回0，否则返回-1
     */
    static int GetLeader(const LogicPoolID &logicPoolId,
                        const CopysetID &copysetId,
                        const Configuration &conf,
                        PeerId *leaderId);
};
}   // namespace client
}   // namespace curve
#endif  // ! CURVE_CLIENT_SERVICE_ADAPTOR_H
