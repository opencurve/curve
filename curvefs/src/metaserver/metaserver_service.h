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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_METASERVER_METASERVER_SERVICE_H_
#define CURVEFS_SRC_METASERVER_METASERVER_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <memory>
#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/metaserver/copyset/copyset_node_manager.h"
#include "curvefs/src/metaserver/inflight_throttle.h"

namespace curvefs {
namespace metaserver {

using ::curvefs::metaserver::copyset::CopysetNodeManager;

class MetaServerServiceImpl : public MetaServerService {
 public:
    MetaServerServiceImpl(CopysetNodeManager* copysetNodeManager,
                          InflightThrottle* inflightThrottle)
        : copysetNodeManager_(copysetNodeManager),
          inflightThrottle_(inflightThrottle) {}

    void GetDentry(::google::protobuf::RpcController* controller,
                   const ::curvefs::metaserver::GetDentryRequest* request,
                   ::curvefs::metaserver::GetDentryResponse* response,
                   ::google::protobuf::Closure* done) override;
    void ListDentry(::google::protobuf::RpcController* controller,
                    const ::curvefs::metaserver::ListDentryRequest* request,
                    ::curvefs::metaserver::ListDentryResponse* response,
                    ::google::protobuf::Closure* done) override;
    void CreateDentry(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::CreateDentryRequest* request,
                      ::curvefs::metaserver::CreateDentryResponse* response,
                      ::google::protobuf::Closure* done) override;
    void DeleteDentry(::google::protobuf::RpcController* controller,
                      const ::curvefs::metaserver::DeleteDentryRequest* request,
                      ::curvefs::metaserver::DeleteDentryResponse* response,
                      ::google::protobuf::Closure* done) override;
    void GetInode(::google::protobuf::RpcController* controller,
                  const ::curvefs::metaserver::GetInodeRequest* request,
                  ::curvefs::metaserver::GetInodeResponse* response,
                  ::google::protobuf::Closure* done) override;
    void BatchGetInodeAttr(::google::protobuf::RpcController* controller,
                const ::curvefs::metaserver::BatchGetInodeAttrRequest* request,
                ::curvefs::metaserver::BatchGetInodeAttrResponse* response,
                ::google::protobuf::Closure* done) override;
    void BatchGetXAttr(::google::protobuf::RpcController* controller,
                  const ::curvefs::metaserver::BatchGetXAttrRequest* request,
                  ::curvefs::metaserver::BatchGetXAttrResponse* response,
                  ::google::protobuf::Closure* done) override;
    void CreateInode(::google::protobuf::RpcController* controller,
                     const ::curvefs::metaserver::CreateInodeRequest* request,
                     ::curvefs::metaserver::CreateInodeResponse* response,
                     ::google::protobuf::Closure* done) override;
    void CreateRootInode(
            ::google::protobuf::RpcController* controller,
            const ::curvefs::metaserver::CreateRootInodeRequest* request,
            ::curvefs::metaserver::CreateRootInodeResponse* response,
            ::google::protobuf::Closure* done) override;
    void CreateManageInode(
            ::google::protobuf::RpcController* controller,
            const ::curvefs::metaserver::CreateManageInodeRequest* request,
            ::curvefs::metaserver::CreateManageInodeResponse* response,
            ::google::protobuf::Closure* done) override;
    void UpdateInode(::google::protobuf::RpcController* controller,
                     const ::curvefs::metaserver::UpdateInodeRequest* request,
                     ::curvefs::metaserver::UpdateInodeResponse* response,
                     ::google::protobuf::Closure* done) override;
    void GetOrModifyS3ChunkInfo(
        ::google::protobuf::RpcController* controller,
        const ::curvefs::metaserver::GetOrModifyS3ChunkInfoRequest* request,
        ::curvefs::metaserver::GetOrModifyS3ChunkInfoResponse* response,
        ::google::protobuf::Closure* done) override;
    void DeleteInode(::google::protobuf::RpcController* controller,
                     const ::curvefs::metaserver::DeleteInodeRequest* request,
                     ::curvefs::metaserver::DeleteInodeResponse* response,
                     ::google::protobuf::Closure* done) override;

    void CreatePartition(google::protobuf::RpcController* controller,
                         const CreatePartitionRequest* request,
                         CreatePartitionResponse* response,
                         google::protobuf::Closure* done) override;

    void DeletePartition(google::protobuf::RpcController* controller,
                         const DeletePartitionRequest* request,
                         DeletePartitionResponse* response,
                         google::protobuf::Closure* done) override;

    void GetVolumeExtent(::google::protobuf::RpcController* controller,
                         const GetVolumeExtentRequest* request,
                         GetVolumeExtentResponse* response,
                         ::google::protobuf::Closure* done) override;

    void UpdateVolumeExtent(::google::protobuf::RpcController* controller,
                            const UpdateVolumeExtentRequest* request,
                            UpdateVolumeExtentResponse* response,
                            ::google::protobuf::Closure* done) override;

    void UpdateDeallocatableBlockGroup(
        ::google::protobuf::RpcController *controller,
        const UpdateDeallocatableBlockGroupRequest *request,
        UpdateDeallocatableBlockGroupResponse *response,
        ::google::protobuf::Closure *done) override;

    // reserved for compatibility
    void PrepareRenameTx(google::protobuf::RpcController* controller,
        const PrepareRenameTxRequest* request,
        PrepareRenameTxResponse* response,
        google::protobuf::Closure* done) override;

    void PrewriteRenameTx(google::protobuf::RpcController* controller,
        const PrewriteRenameTxRequest* request,
        PrewriteRenameTxResponse* response,
        google::protobuf::Closure* done) override;

    void CheckTxStatus(google::protobuf::RpcController* controller,
        const CheckTxStatusRequest* request, CheckTxStatusResponse* response,
        google::protobuf::Closure* done) override;

    void ResolveTxLock(google::protobuf::RpcController* controller,
        const ResolveTxLockRequest* request, ResolveTxLockResponse* response,
        google::protobuf::Closure* done) override;

    void CommitTx(google::protobuf::RpcController* controller,
        const CommitTxRequest* request, CommitTxResponse* response,
        google::protobuf::Closure* done) override;

 private:
    CopysetNodeManager* copysetNodeManager_;
    InflightThrottle* inflightThrottle_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_METASERVER_SERVICE_H_
