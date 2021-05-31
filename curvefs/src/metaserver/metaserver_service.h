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
#include "curvefs/src/metaserver/dentry_storage.h"
#include "curvefs/src/metaserver/dentry_manager.h"
#include "curvefs/src/metaserver/inode_storage.h"
#include "curvefs/src/metaserver/inode_manager.h"

namespace curvefs {
namespace metaserver {
class MetaServerServiceImpl: public MetaServerService {
 public:
    MetaServerServiceImpl(std::shared_ptr<InodeManager> inodeManager,
                          std::shared_ptr<DentryManager> dentryManager) {
        inodeManager_ = inodeManager;
        dentryManager_ = dentryManager;
    }

    void GetDentry(::google::protobuf::RpcController* controller,
                    const ::curvefs::metaserver::GetDentryRequest* request,
                       ::curvefs::metaserver::GetDentryResponse* response,
                       ::google::protobuf::Closure* done);
    void ListDentry(::google::protobuf::RpcController* controller,
                    const ::curvefs::metaserver::ListDentryRequest* request,
                       ::curvefs::metaserver::ListDentryResponse* response,
                       ::google::protobuf::Closure* done);
    void CreateDentry(::google::protobuf::RpcController* controller,
                    const ::curvefs::metaserver::CreateDentryRequest* request,
                       ::curvefs::metaserver::CreateDentryResponse* response,
                       ::google::protobuf::Closure* done);
    void DeleteDentry(::google::protobuf::RpcController* controller,
                    const ::curvefs::metaserver::DeleteDentryRequest* request,
                       ::curvefs::metaserver::DeleteDentryResponse* response,
                       ::google::protobuf::Closure* done);
    void GetInode(::google::protobuf::RpcController* controller,
                    const ::curvefs::metaserver::GetInodeRequest* request,
                       ::curvefs::metaserver::GetInodeResponse* response,
                       ::google::protobuf::Closure* done);
    void CreateInode(::google::protobuf::RpcController* controller,
                       const ::curvefs::metaserver::CreateInodeRequest* request,
                       ::curvefs::metaserver::CreateInodeResponse* response,
                       ::google::protobuf::Closure* done);
    void UpdateInode(::google::protobuf::RpcController* controller,
                       const ::curvefs::metaserver::UpdateInodeRequest* request,
                       ::curvefs::metaserver::UpdateInodeResponse* response,
                       ::google::protobuf::Closure* done);
    void DeleteInode(::google::protobuf::RpcController* controller,
                       const ::curvefs::metaserver::DeleteInodeRequest* request,
                       ::curvefs::metaserver::DeleteInodeResponse* response,
                       ::google::protobuf::Closure* done);

 private:
    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<DentryManager> dentryManager_;
};
}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_METASERVER_SERVICE_H_
