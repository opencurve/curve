/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 * Copyright (c) 2018 netease
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVICE_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVICE_H_

#include <brpc/server.h>
#include <memory>
#include <string>

#include "proto/snapshotcloneserver.pb.h"
#include "src/snapshotcloneserver/snapshot/snapshot_service_manager.h"
#include "src/snapshotcloneserver/clone/clone_service_manager.h"

namespace curve {
namespace snapshotcloneserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

/**
 * @brief 快照转储rpc服务实现
 */
class SnapshotCloneServiceImpl : public SnapshotCloneService {
 public:
     /**
      * @brief 构造函数
      *
      * @param manager 快照转储服务管理对象
      */
    SnapshotCloneServiceImpl(
        std::shared_ptr<SnapshotServiceManager> snapshotManager,
        std::shared_ptr<CloneServiceManager> cloneManager)
        : snapshotManager_(snapshotManager),
          cloneManager_(cloneManager) {}
    virtual ~SnapshotCloneServiceImpl() {}

    /**
     * @brief http服务默认方法
     *
     * @param cntl rpc controller
     * @param req  http请求报文
     * @param resp http回复报文
     * @param done http异步回调闭包
     */
    void default_method(RpcController* cntl,
                        const HttpRequest* req,
                        HttpResponse* resp,
                        Closure* done);

 private:
    void HandleCreateSnapshotAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleDeleteSnapshotAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleCancelSnapshotAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleGetFileSnapshotInfoAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleCloneAction(brpc::Controller* bcntl,
        const std::string &requestId,
        Closure* done);
    void HandleRecoverAction(brpc::Controller* bcntl,
        const std::string &requestId,
        Closure* done);
    void HandleGetCloneTasksAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleCleanCloneTaskAction(brpc::Controller* bcntl,
        const std::string &requestId);

    bool CheckBoolParamter(
        const std::string *param, bool *valueOut);

 private:
    // 快照转储服务管理对象
    std::shared_ptr<SnapshotServiceManager> snapshotManager_;
    std::shared_ptr<CloneServiceManager> cloneManager_;
};
}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVICE_H_
