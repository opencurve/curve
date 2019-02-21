/*
 * Project: curve
 * Created Date: 18-8-23
 * Author: wudemiao
 * Copyright (c) 2018 netease
 */

#ifndef CURVE_CHUNKSERVER_COPYSET_SERVICE_H
#define CURVE_CHUNKSERVER_COPYSET_SERVICE_H

#include "proto/copyset.pb.h"

namespace curve {
namespace chunkserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

class CopysetNodeManager;

/**
 * 复制组管理的Rpc服务，目前仅有创建复制组
 */
class CopysetServiceImpl : public CopysetService {
 public:
    explicit CopysetServiceImpl(CopysetNodeManager* copysetNodeManager) :
        copysetNodeManager_(copysetNodeManager) {}
    ~CopysetServiceImpl() {}

    /**
     * 创建复制组
     */
    void CreateCopysetNode(RpcController *controller,
                           const CopysetRequest *request,
                           CopysetResponse *response,
                           Closure *done);

 private:
    // 复制组管理者
    CopysetNodeManager* copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_COPYSET_SERVICE_H
