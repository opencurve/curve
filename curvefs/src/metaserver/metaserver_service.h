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
#include "curvefs/src/metaserver/dentry_manager.h"
#include "curvefs/src/metaserver/dentry_storage.h"
#include "curvefs/src/metaserver/inode_manager.h"
#include "curvefs/src/metaserver/inode_storage.h"

namespace curvefs {
namespace metaserver {

#define DEFINE_RPC(function_name) function_name( \
    ::google::protobuf::RpcController* controller, \
    const ::curvefs::metaserver::function_name##Request* request, \
    ::curvefs::metaserver::function_name##Response* response, \
    ::google::protobuf::Closure* done)

class MetaServerServiceImpl : public MetaServerService {
 public:
    MetaServerServiceImpl(std::shared_ptr<InodeManager> inodeManager,
                          std::shared_ptr<DentryManager> dentryManager) {
        inodeManager_ = inodeManager;
        dentryManager_ = dentryManager;
    }

    // dentry interface
    void DEFINE_RPC(CreateDentry);
    void DEFINE_RPC(DeleteDentry);
    void DEFINE_RPC(GetDentry);
    void DEFINE_RPC(ListDentry);
    void DEFINE_RPC(PrepareRenameTx);

    // inode interface
    void DEFINE_RPC(CreateInode);
    void DEFINE_RPC(CreateRootInode);
    void DEFINE_RPC(DeleteInode);
    void DEFINE_RPC(GetInode);
    void DEFINE_RPC(UpdateInode);
    void DEFINE_RPC(UpdateInodeS3Version);

 private:
    std::shared_ptr<InodeManager> inodeManager_;
    std::shared_ptr<DentryManager> dentryManager_;
};

}  // namespace metaserver
}  // namespace curvefs

#endif  // CURVEFS_SRC_METASERVER_METASERVER_SERVICE_H_
