/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Created Date: Thur Sept 2 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_RPCCLIENT_TASK_EXCUTOR_H_
#define CURVEFS_SRC_CLIENT_RPCCLIENT_TASK_EXCUTOR_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <unordered_map>
#include <memory>
#include <list>
#include <string>

#include "src/common/concurrent/rw_lock.h"
#include "curvefs/proto/common.pb.h"
#include "curvefs/src/client/common/common.h"
#include "curvefs/src/client/rpcclient/metacache.h"
#include "curvefs/src/client/rpcclient/channel_manager.h"
#include "src/common/math_util.h"

using ::curve::client::CopysetID;
using ::curve::client::LogicPoolID;
using ::curvefs::client::common::ExcutorOpt;
using ::curvefs::client::common::MetaserverID;
using ::curvefs::client::common::MetaServerOpType;
using ::curvefs::common::PartitionInfo;
using ::curvefs::metaserver::MetaStatusCode;

namespace curvefs {
namespace client {
namespace rpcclient {

class TaskExecutorDone;

MetaStatusCode ConvertToMetaStatusCode(int retcode);

class TaskContext {
 public:
    using RpcFunc = std::function<int(
        LogicPoolID poolID, CopysetID copysetID, PartitionID partitionID,
        uint64_t txId, uint64_t applyIndex, brpc::Channel *channel,
        brpc::Controller *cntl, TaskExecutorDone *done)>;

    TaskContext() = default;
    TaskContext(MetaServerOpType type, RpcFunc func, uint32_t fsid = 0,
                uint32_t inodeid = 0, bool refreshTxId = false)
        : optype(type), rpctask(func), fsID(fsid), inodeID(inodeid),
          refreshTxId(refreshTxId) {}

    std::string TaskContextStr() {
        std::ostringstream oss;
        oss << "{" << optype << ",fsid=" << fsID << ",inodeid=" << inodeID
            << ",refreshTxId=" << refreshTxId << "}";
        return oss.str();
    }

 public:
    uint64_t rpcTimeoutMs;
    MetaServerOpType optype;
    RpcFunc rpctask = nullptr;
    uint32_t fsID = 0;
    // inode used to locate replacement of dentry or inode. for CreateDentry
    // inodeid,`task_->inodeID` is parentinodeID
    uint64_t inodeID = 0;

    CopysetTarget target;
    uint64_t applyIndex = 0;

    uint64_t retryTimes = 0;
    bool suspend = false;
    bool retryDirectly = false;

    bool refreshTxId = false;

    brpc::Controller cntl_;
};


class TaskExecutor {
 public:
    TaskExecutor() {}
    TaskExecutor(const ExcutorOpt &opt,
        const std::shared_ptr<MetaCache> &metaCache,
        const std::shared_ptr<ChannelManager<MetaserverID>> &channelManager,
        const std::shared_ptr<TaskContext> &task)
        : metaCache_(metaCache), channelManager_(channelManager),
        task_(task), opt_(opt) {
        SetRetryParam();
    }

    int DoRPCTask();
    void DoAsyncRPCTask(TaskExecutorDone *done);
    int DoRPCTaskInner(TaskExecutorDone *done);

    bool OnReturn(int retCode);
    void PreProcessBeforeRetry(int retCode);

    std::shared_ptr<TaskContext> GetTaskCxt() const {
        return task_;
    }

    std::shared_ptr<MetaCache> GetMetaCache() const {
        return metaCache_;
    }

 protected:
    // prepare resource and excute task
    bool NeedRetry();
    int ExcuteTask(brpc::Channel* channel, TaskExecutorDone *done);
    virtual bool GetTarget();
    void UpdateApplyIndex(const LogicPoolID &poolID, const CopysetID &copysetId,
                          uint64_t applyIndex);

    // handle a returned rpc
    void OnSuccess();
    void OnReDirected();
    void OnCopysetNotExist();
    void OnPartitionAllocIDFail();

    // retry policy
    void RefreshLeader();
    uint64_t OverLoadBackOff();
    uint64_t TimeoutBackOff();
    void SetRetryParam();

 private:
    bool HasValidTarget() const;

    void ResetChannelIfNotHealth();

 protected:
    std::shared_ptr<MetaCache> metaCache_;
    std::shared_ptr<ChannelManager<MetaserverID>> channelManager_;
    std::shared_ptr<TaskContext> task_;

    ExcutorOpt opt_;
    uint64_t maxOverloadPow_;
    uint64_t maxTimeoutPow_;
};

class MetaServerClientDone : public google::protobuf::Closure {
 public:
    MetaServerClientDone() {}
    ~MetaServerClientDone() {}

    void SetMetaStatusCode(MetaStatusCode code) {
        code_ = code;
    }

    MetaStatusCode GetStatusCode() {
        return code_;
    }

 private:
    MetaStatusCode code_;
};

class TaskExecutorDone : public google::protobuf::Closure {
 public:
    TaskExecutorDone(const std::shared_ptr<TaskExecutor> excutor,
        MetaServerClientDone *done) :
        excutor_(excutor),
        done_(done) {}
    virtual ~TaskExecutorDone() {}

    void Run() override;

    void SetRetCode(int code) {
        code_ = code;
    }

    int GetRetCode() const {
        return code_;
    }

    std::shared_ptr<TaskExecutor> GetTaskExcutor() const {
        return excutor_;
    }

 private:
    friend class TaskExecutor;

 private:
    std::shared_ptr<TaskExecutor> excutor_;
    MetaServerClientDone *done_;
    int code_;
};

class CreateInodeExcutor : public TaskExecutor {
 public:
    explicit CreateInodeExcutor(
        const ExcutorOpt &opt,
        const std::shared_ptr<MetaCache> &metaCache,
        const std::shared_ptr<ChannelManager<MetaserverID>> &channelManager,
        const std::shared_ptr<TaskContext> &task)
        : TaskExecutor(opt, metaCache, channelManager, task) {}

 protected:
    bool GetTarget() override;
};

}  // namespace rpcclient
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_RPCCLIENT_TASK_EXCUTOR_H_
