/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: Fri Dec 14 2018
 * Author: xuchaojie
 */

#ifndef SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVICE_H_
#define SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVICE_H_

#include <brpc/server.h>
#include <memory>
#include <string>

#include "proto/snapshotcloneserver.pb.h"
#include "src/common/authenticator.h"
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
    void HandleFlattenAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleGetCloneTasksAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleCleanCloneTaskAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleGetFileSnapshotListAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleGetCloneTaskListAction(brpc::Controller* bcntl,
        const std::string &requestId);
    void HandleGetCloneRefStatusAction(brpc::Controller* bcntl,
        const std::string &requestId);
    bool CheckBoolParamter(
        const std::string *param, bool *valueOut);
    void SetErrorMessage(brpc::Controller* bcntl, int errCode,
                        const std::string &requestId,
                        const std::string &uuid = "");
    void HandleBadRequestError(brpc::Controller* bcntl,
                        const std::string &requestId,
                        const std::string &uuid = "");

 private:
    // 快照转储服务管理对象
    std::shared_ptr<SnapshotServiceManager> snapshotManager_;
    std::shared_ptr<CloneServiceManager> cloneManager_;
};
}  // namespace snapshotcloneserver
}  // namespace curve

#endif  // SRC_SNAPSHOTCLONESERVER_SNAPSHOTCLONE_SERVICE_H_
