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
 * Date: Sat Aug  7 22:46:58 CST 2021
 * Author: wuhanqing
 */

#ifndef CURVEFS_SRC_METASERVER_COPYSET_META_OPERATOR_H_
#define CURVEFS_SRC_METASERVER_COPYSET_META_OPERATOR_H_

#include <brpc/controller.h>
#include <google/protobuf/message.h>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/common/rpc_stream.h"
#include "curvefs/src/metaserver/copyset/operator_type.h"
#include "curvefs/src/metaserver/copyset/copyset_node.h"

namespace curvefs {
namespace metaserver {
namespace copyset {

class MetaOperator {
 public:
    MetaOperator(CopysetNode* node, google::protobuf::RpcController* cntl,
                 const google::protobuf::Message* request,
                 google::protobuf::Message* response,
                 google::protobuf::Closure* done)
        : node_(node),
          cntl_(cntl),
          request_(request),
          response_(response),
          done_(done),
          ownRequest_(false) {}

    MetaOperator(CopysetNode* node, const google::protobuf::Message* request,
                 bool ownRequest = true)
        : node_(node),
          cntl_(nullptr),
          request_(request),
          response_(nullptr),
          done_(nullptr),
          ownRequest_(ownRequest) {}

    virtual ~MetaOperator();

    MetaOperator(const MetaOperator&) = delete;

    MetaOperator& operator=(const MetaOperator&) = delete;

    /**
     * @brief Propose current operator to Raft
     */
    void Propose();

    /**
     * @brief Return internal closure
     */
    google::protobuf::Closure* Closure() const { return done_; }

    void RedirectRequest();

    virtual void OnApply(int64_t index, google::protobuf::Closure* done,
                         uint64_t startTimeUs) = 0;

    virtual void OnApplyFromLog(int64_t index, uint64_t startTimeUs) = 0;

    // Get hash code of current operator which is used to push current operator
    // task to apply queue, and apply queue will guarantee that operators with
    // the same hash code are executed serially.
    // Current, all operators' hash code is request's PARTITION-ID
    // TODO(wuhanqing): Finer grained hash code
    // e.g.,
    // For dentry-related operator hash code is `parent-inode-id`
    // For inode-related operator hash code is `inode-id`
    // For partition-related operator hash code is `partition-id`
    // but above hash policy has BUG
    // scenario:
    // two operators: create partition and create inode in this partition
    // if leader crashed after second operator is executed, the new elected
    // leader must apply those two operators, BUT those two operators are hashed
    // by different ID, so they can be executed in parallel, and this can lead
    // to create inode operator is executed prior to create partition operator
    // Possible solution:
    // 1. create a partition if crate inode found partition not exist
    // 2. create inode wait partition create when found partition not exist
    virtual uint64_t HashCode() const = 0;

    virtual OperatorType GetOperatorType() const = 0;

 private:
    /**
     * @brief Check whether current copyset node is leader
     */
    bool IsLeaderTerm() const { return node_->IsLeaderTerm(); }

    /**
     * @brief Propose current operator to braft::Task
     */
    bool ProposeTask();

    /**
     * @brief Directly push operator to concurrently module
     */
    void FastApplyTask();

 private:
    /**
     * @brief Redirect request if current node is not leader
     */
    virtual void Redirect() = 0;

    virtual void OnFailed(MetaStatusCode code) = 0;

    /**
     * @brief Whether an operator can bypass propose to raft,
     *        return true if operator is readonly and request carry with
     *        an valid appliedindex
     */
    virtual bool CanBypassPropose() const { return false; }

 protected:
    CopysetNode* node_;

    // rpc controller
    google::protobuf::RpcController* cntl_;

    // rpc request
    const google::protobuf::Message* request_;

    // rpc response
    google::protobuf::Message* response_;

    // rpc closure or meta service closure
    google::protobuf::Closure* done_;

    // whether own request, if true, delete request when destory
    const bool ownRequest_;

 public:
    butil::Timer timerPropose;
};

class GetDentryOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;

    bool CanBypassPropose() const override;
};

class ListDentryOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;

    bool CanBypassPropose() const override;
};

class CreateDentryOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class DeleteDentryOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class GetInodeOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;

    bool CanBypassPropose() const override;
};

class BatchGetInodeAttrOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;

    bool CanBypassPropose() const override;
};

class BatchGetXAttrOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;

    bool CanBypassPropose() const override;
};

class CreateInodeOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class UpdateInodeOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class GetOrModifyS3ChunkInfoOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class DeleteInodeOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class CreateRootInodeOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class CreateManageInodeOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class UpdateInodeS3VersionOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class CreatePartitionOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class DeletePartitionOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class GetVolumeExtentOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
        uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;

    bool CanBypassPropose() const override;
};

class UpdateVolumeExtentOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
        uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class UpdateDeallocatableBlockGroupOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
        uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class PrepareRenameTxOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class PrewriteRenameTxOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
        uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class CheckTxStatusOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;

    bool CanBypassPropose() const override;
};

class ResolveTxLockOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

class CommitTxOperator : public MetaOperator {
 public:
    using MetaOperator::MetaOperator;

    void OnApply(int64_t index, google::protobuf::Closure* done,
                 uint64_t startTimeUs) override;

    void OnApplyFromLog(int64_t index, uint64_t startTimeUs) override;

    uint64_t HashCode() const override;

    OperatorType GetOperatorType() const override;

 private:
    void Redirect() override;

    void OnFailed(MetaStatusCode code) override;
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_COPYSET_META_OPERATOR_H_
