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

#ifndef CURVEFS_SRC_MDS_MDS_SERVICE_H_
#define CURVEFS_SRC_MDS_MDS_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <memory>
#include <string>
#include "curvefs/proto/mds.pb.h"
#include "curvefs/src/mds/fs_manager.h"

namespace curvefs {
namespace mds {

#define DEFINE_RPC(function_name) function_name( \
    ::google::protobuf::RpcController* controller, \
    const ::curvefs::mds::function_name##Request* request, \
    ::curvefs::mds::function_name##Response* response, \
    ::google::protobuf::Closure* done)

class MdsServiceImpl : public MdsService {
 public:
    explicit MdsServiceImpl(std::shared_ptr<FsManager> fsManager) {
        fsManager_ = fsManager;
    }

    void CreateFs(::google::protobuf::RpcController* controller,
                  const ::curvefs::mds::CreateFsRequest* request,
                  ::curvefs::mds::CreateFsResponse* response,
                  ::google::protobuf::Closure* done);

    void MountFs(::google::protobuf::RpcController* controller,
                 const ::curvefs::mds::MountFsRequest* request,
                 ::curvefs::mds::MountFsResponse* response,
                 ::google::protobuf::Closure* done);

    void UmountFs(::google::protobuf::RpcController* controller,
                  const ::curvefs::mds::UmountFsRequest* request,
                  ::curvefs::mds::UmountFsResponse* response,
                  ::google::protobuf::Closure* done);

    void GetFsInfo(::google::protobuf::RpcController* controller,
                   const ::curvefs::mds::GetFsInfoRequest* request,
                   ::curvefs::mds::GetFsInfoResponse* response,
                   ::google::protobuf::Closure* done);

    void DeleteFs(::google::protobuf::RpcController* controller,
                  const ::curvefs::mds::DeleteFsRequest* request,
                  ::curvefs::mds::DeleteFsResponse* response,
                  ::google::protobuf::Closure* done);

    void DEFINE_RPC(CommitTx);

 private:
    std::shared_ptr<FsManager> fsManager_;
};
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_MDS_SERVICE_H_
