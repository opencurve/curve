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

class CopysetNodeManager;

class CopysetServiceImpl : public CopysetService {
 public:
    explicit CopysetServiceImpl(CopysetNodeManager* copysetNodeManager) :
        copysetNodeManager_(copysetNodeManager) {}
    ~CopysetServiceImpl() {}
    void CreateCopysetNode(::google::protobuf::RpcController *controller,
                           const ::curve::chunkserver::CopysetRequest *request,
                           ::curve::chunkserver::CopysetResponse *response,
                           google::protobuf::Closure *done);

 private:
    CopysetNodeManager* copysetNodeManager_;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // CURVE_CHUNKSERVER_COPYSET_SERVICE_H
